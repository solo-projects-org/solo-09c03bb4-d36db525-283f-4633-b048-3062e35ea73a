"""本地 HTTP 对象源（岸站对象库模拟）。

仅用 Python 标准库实现：
- ``GET  /objects``           日档清单（key / 强 ETag / 全长 / 当日 UTC 秒范围）
- ``HEAD /objects/{key}``     强 ETag（SHA-256，无 ``W/`` 前缀）+ Content-Length
- ``GET  /objects/{key}``     流式取档，支持演示用延迟/中途失败/旧竞态注入
- ``POST /admin/correct/{key}``          用请求体新 Parquet 原子替换对象（源端更正 → 新 ETag）
- ``POST /admin/correct_from_file/{key}``用数据目录里的 ``*.corrected.parquet`` 原子替换对象
- ``POST /admin/simulate``    注入慢下载/中途失败/旧版竞态，``DELETE`` 取消注入

版本一致性承诺（修复点）
------------------------
岸站更正走 ``os.replace`` 原子换名，因此**已打开的 fd 永远指向一个完整版本**。
GET 先打开文件、对同一 fd 做 ``fstat`` 并（按 inode 缓存）计算 ETag，响应头的
ETag、Content-Length 与随后发送的字节必然属于同一完整版本；更正即使在传输
中途发生，在途响应也只是“完整地发旧版”，绝不会把新版字节挂在旧版 ETag 下。

元数据按 ``(st_dev, st_ino)`` 索引并以 ``mtime_ns + size`` 复核：原子替换产生
新 inode 必然未命中并重算哈希；未命中时才对真实内容做 SHA-256，命中时零开销。

故障注入 ``stale_get`` 用于演示/测试复现修复前的竞态：先按当时元数据发响应头，
休眠 ``open_delay`` 后才打开路径取内容 —— 期间发生的更正会让字节与 ETag 脱节
（等长更正时只有内容哈希能识破），供 Web 端校验逻辑演示“收错即失败并清理”。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [object-source] %(message)s"
)
log = logging.getLogger("object-source")

DATA_DIR = os.environ.get("DATA_DIR", "/data/objects")
PORT = int(os.environ.get("PORT", "8080"))
CHUNK_BYTES = 256 * 1024

# 故障注入参数的默认值
SIM_DEFAULTS = {
    "delay": 0.0,          # 每个块发送后的休眠秒数
    "fail_after": 0,       # 发送 N 字节后中途断开；0 表示不断
    "chunk_bytes": CHUNK_BYTES,  # 流式块大小（调小可拉长在途窗口，演示交错）
    "stale_get": False,    # True：复现旧竞态（先发头、后开路径）
    "open_delay": 0.0,     # stale_get 模式下，发头后到打开路径前的休眠秒数
}


def _hash_fileobj(fh) -> str:
    """对已打开 fd 的当前内容算 SHA-256，返回带引号的强 ETag。"""
    h = hashlib.sha256()
    fh.seek(0)
    for block in iter(lambda: fh.read(1024 * 1024), b""):
        h.update(block)
    fh.seek(0)
    return '"' + h.hexdigest() + '"'


class ObjectStore:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir
        self._lock = threading.RLock()
        # key -> meta（清单/HEAD 视图）；meta 含 etag/size/mtime_ns/ino
        self._meta_by_key: dict[str, dict] = {}
        # (st_dev, st_ino) -> meta：同一物理版本的权威索引
        self._meta_by_ino: dict[tuple[int, int], dict] = {}
        # key -> 故障注入参数（见 SIM_DEFAULTS）
        self._sim: dict[str, dict] = {}
        self._manifest: dict[str, dict] = {}
        self._dir_signature: tuple | None = None
        self.refresh()

    # -------------------------------------------------------------元数据
    def _meta_for_stat_locked(self, key: str, st: os.stat_result, fh) -> dict:
        """按 inode 索引取元数据；未命中时对 ``fh``（与 stat 同一 fd）重算哈希。

        调用方须持锁。``fh`` 必须是已 ``fstat`` 为 ``st`` 的打开文件，
        这样算出的 ETag 与该 fd 之后被读取的字节同属一个完整版本。
        """
        ino_key = (st.st_dev, st.st_ino)
        meta = self._meta_by_ino.get(ino_key)
        if (
            meta is not None
            and meta["size"] == st.st_size
            and meta["mtime_ns"] == st.st_mtime_ns
        ):
            return meta
        meta = {
            "etag": _hash_fileobj(fh),
            "size": st.st_size,
            "mtime_ns": st.st_mtime_ns,
            "ino": st.st_ino,
        }
        self._meta_by_ino[ino_key] = meta
        self._meta_by_key[key] = meta
        log.info("登记/刷新对象 %s etag=%s size=%d", key, meta["etag"], meta["size"])
        return meta

    def maybe_refresh(self) -> None:
        """目录签名（文件清单+mtime）变化时才重扫，平时只做一次 listdir+stat。"""
        try:
            entries = tuple(
                sorted(
                    (name, os.stat(os.path.join(self.data_dir, name)).st_mtime_ns)
                    for name in os.listdir(self.data_dir)
                    if name.endswith(".parquet")
                )
            )
        except FileNotFoundError:
            entries = ()
        if entries != self._dir_signature:
            self.refresh()

    def refresh(self) -> None:
        with self._lock:
            manifest_path = os.path.join(self.data_dir, "manifest.json")
            if os.path.exists(manifest_path):
                with open(manifest_path, encoding="utf-8") as fh:
                    self._manifest = json.load(fh)
            if not os.path.isdir(self.data_dir):
                self._dir_signature = ()
                return
            present: dict[str, dict] = {}
            live_inos: set[tuple[int, int]] = set()
            for name in sorted(os.listdir(self.data_dir)):
                if not name.endswith(".parquet"):
                    continue
                full = os.path.join(self.data_dir, name)
                try:
                    # 先打开再 fstat：fd 是稳定版本句柄，不受并发更正影响。
                    with open(full, "rb") as fh:
                        st = os.fstat(fh.fileno())
                        meta = self._meta_for_stat_locked(name, st, fh)
                except FileNotFoundError:
                    continue  # 扫描期间被更正/删除：下一轮再说
                present[name] = meta
                live_inos.add((st.st_dev, st.st_ino))
            self._meta_by_key = present
            # 只保留仍存活的 inode；在途 GET 已持有自己的 meta 引用，不受影响。
            for ino_key in list(self._meta_by_ino):
                if ino_key not in live_inos:
                    del self._meta_by_ino[ino_key]
            try:
                self._dir_signature = tuple(
                    sorted(
                        (name, os.stat(os.path.join(self.data_dir, name)).st_mtime_ns)
                        for name in present
                    )
                )
            except FileNotFoundError:
                self._dir_signature = ()

    def catalog(self) -> list[dict]:
        self.maybe_refresh()
        with self._lock:
            out = []
            for key, meta in sorted(self._meta_by_key.items()):
                item = {
                    "key": key,
                    "etag": meta["etag"],
                    "size": meta["size"],
                }
                item.update(self._manifest.get(key, {}))
                out.append(item)
            return out

    def meta_for(self, key: str) -> dict | None:
        """HEAD 用：当前路径指向版本的元数据（目录变化时已重扫）。"""
        self.maybe_refresh()
        with self._lock:
            return self._meta_by_key.get(key)

    # -------------------------------------------------------------故障注入
    def set_simulation(self, key: str, **params) -> None:
        sim = dict(SIM_DEFAULTS)
        for name, value in params.items():
            if name not in sim:
                raise KeyError(f"未知注入参数：{name}")
            sim[name] = value
        sim["delay"] = max(0.0, float(sim["delay"]))
        sim["fail_after"] = max(0, int(sim["fail_after"]))
        sim["chunk_bytes"] = max(1, int(sim["chunk_bytes"]))
        sim["stale_get"] = bool(sim["stale_get"])
        sim["open_delay"] = max(0.0, float(sim["open_delay"]))
        with self._lock:
            self._sim[key] = sim

    def clear_simulation(self, key: str) -> None:
        with self._lock:
            self._sim.pop(key, None)

    def get_simulation(self, key: str) -> dict:
        with self._lock:
            return dict(self._sim.get(key, SIM_DEFAULTS))

    # -------------------------------------------------------------源端更正
    def correct(self, key: str, payload: bytes) -> dict:
        """原子替换对象内容，返回新版本元数据。"""
        if not key.endswith(".parquet") or "/" in key or ".." in key:
            raise ValueError("非法对象名")
        with self._lock:
            tmp = os.path.join(self.data_dir, f".{key}.new-{os.getpid()}")
            with open(tmp, "wb") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            final = os.path.join(self.data_dir, key)
            os.replace(tmp, final)
            self.refresh()
            log.info("源端更正 %s -> %s", key, self._meta_by_key[key]["etag"])
            return dict(self._meta_by_key[key])

    def correct_from_file(self, key: str) -> dict:
        """用数据目录里的 ``{stem}.corrected.parquet`` 原子替换对象。"""
        if not key.endswith(".parquet") or "/" in key or ".." in key:
            raise ValueError("非法对象名")
        stem = key[: -len(".parquet")]
        src = os.path.join(self.data_dir, f"{stem}.corrected.parquet")
        if not os.path.exists(src):
            raise FileNotFoundError(f"找不到更正素材：{src}")
        with open(src, "rb") as fh:
            payload = fh.read()
        return self.correct(key, payload)

    # -------------------------------------------------------------取档
    def open_version(self, key: str):
        """打开对象，返回 ``(文件对象, 与该 fd 内容一致的元数据)``。

        响应头与后续字节都从这个 fd 派生，三者同属一个完整版本；
        传输中途发生的原子更正不会影响已打开的 fd。
        """
        self.maybe_refresh()
        path = os.path.join(self.data_dir, key)
        fh = open(path, "rb")  # FileNotFoundError 由调用方映射 404
        try:
            st = os.fstat(fh.fileno())
            with self._lock:
                meta = dict(self._meta_for_stat_locked(key, st, fh))
            fh.seek(0)
            return fh, meta
        except Exception:
            fh.close()
            raise

    def open_current_path(self, key: str):
        """【仅供 stale_get 故障注入】发头之后才打开路径，复现旧竞态。"""
        return open(os.path.join(self.data_dir, key), "rb")

    def stream_file(self, fh, key: str, write, flush, sim: dict) -> None:
        """从已打开的 fd 流式发送；模拟失败时抛 :class:`ConnectionAbortedError`。"""
        chunk = max(1, sim["chunk_bytes"])
        sent = 0
        while True:
            block = fh.read(chunk)
            if not block:
                break
            if sim["fail_after"] and sent + len(block) > sim["fail_after"]:
                cut = max(0, sim["fail_after"] - sent)
                if cut:
                    write(block[:cut])
                    flush()
                    sent += cut
                log.warning("模拟中断：%s 已发送 %d 字节后断开", key, sent)
                raise ConnectionAbortedError("simulated mid-transfer failure")
            write(block)
            flush()
            sent += len(block)
            if sim["delay"]:
                time.sleep(sim["delay"])


STORE = ObjectStore(DATA_DIR)


class Handler(BaseHTTPRequestHandler):
    server_version = "BuoyObjectSource/1.0"

    def log_message(self, fmt: str, *args) -> None:
        log.info("%s - %s", self.address_string(), fmt % args)

    # -------------------------------------------------------------路由
    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/healthz":
            self._json(200, {"ok": True})
        elif path == "/objects":
            self._json(200, STORE.catalog())
        elif path.startswith("/objects/"):
            self._get_object(path[len("/objects/"):])
        else:
            self._json(404, {"detail": "not found"})

    def do_HEAD(self) -> None:
        path = urlparse(self.path).path
        if not path.startswith("/objects/"):
            self._json(404, {"detail": "not found"})
            return
        meta = STORE.meta_for(path[len("/objects/"):])
        if meta is None:
            self._json(404, {"detail": "对象不存在"})
            return
        self._send_object_headers(meta)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path == "/admin/simulate":
            body = self._read_json()
            if body is None:
                return
            try:
                STORE.set_simulation(
                    body["key"],
                    delay=body.get("delay_per_chunk", 0.0),
                    fail_after=body.get("fail_after_bytes", body.get("fail_after", 0)),
                    chunk_bytes=body.get("chunk_bytes", CHUNK_BYTES),
                    stale_get=body.get("stale_get", False),
                    open_delay=body.get("open_delay_seconds", body.get("open_delay", 0.0)),
                )
            except (KeyError, ValueError, TypeError) as exc:
                self._json(400, {"detail": f"参数错误：{exc}"})
                return
            self._json(200, {"ok": True, "simulation": STORE.get_simulation(body["key"])})
        elif path.startswith("/admin/correct_from_file/"):
            key = path[len("/admin/correct_from_file/"):]
            try:
                meta = STORE.correct_from_file(key)
            except FileNotFoundError as exc:
                self._json(404, {"detail": str(exc)})
                return
            except ValueError as exc:
                self._json(400, {"detail": str(exc)})
                return
            self._json(200, {"ok": True, "meta": meta})
        elif path.startswith("/admin/correct/"):
            key = path[len("/admin/correct/"):]
            length = int(self.headers.get("Content-Length", "0"))
            payload = self.rfile.read(length) if length else b""
            try:
                meta = STORE.correct(key, payload)
            except ValueError as exc:
                self._json(400, {"detail": str(exc)})
                return
            self._json(200, {"ok": True, "meta": meta})
        else:
            self._json(404, {"detail": "not found"})

    def do_DELETE(self) -> None:
        path = urlparse(self.path).path
        if path.startswith("/admin/simulate/"):
            key = path[len("/admin/simulate/"):]
            STORE.clear_simulation(key)
            self._json(200, {"ok": True})
        else:
            self._json(404, {"detail": "not found"})

    # -------------------------------------------------------------实现
    def _send_object_headers(self, meta: dict) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("ETag", meta["etag"])  # 强 ETag：无 W/ 前缀
        self.send_header("Content-Length", str(meta["size"]))
        self.send_header("Content-Type", "application/vnd.apache.parquet")
        self.end_headers()

    def _get_object(self, key: str) -> None:
        sim = STORE.get_simulation(key)
        if sim["stale_get"]:
            # 故障注入：复现修复前的竞态 —— 先按当前元数据发头，
            # 休眠后才打开路径取内容，期间更正会让字节与 ETag 脱节。
            self._get_object_stale(key, sim)
            return
        try:
            fh, meta = STORE.open_version(key)
        except FileNotFoundError:
            self._json(404, {"detail": "对象不存在"})
            return
        with fh:
            self._send_object_headers(meta)
            try:
                STORE.stream_file(fh, key, self.wfile.write, self.wfile.flush, sim)
            except (ConnectionAbortedError, BrokenPipeError, ConnectionResetError) as exc:
                log.warning("取档中断 %s：%s", key, exc)
                # 直接关闭连接，让客户端把它当成传输失败处理。
                self.close_connection = True

    def _get_object_stale(self, key: str, sim: dict) -> None:
        meta = STORE.meta_for(key)
        if meta is None:
            self._json(404, {"detail": "对象不存在"})
            return
        log.warning("故障注入 stale_get：%s 先发头（%s），%.1fs 后才打开路径",
                    key, meta["etag"], sim["open_delay"])
        self._send_object_headers(meta)
        if sim["open_delay"]:
            time.sleep(sim["open_delay"])
        try:
            fh = STORE.open_current_path(key)
        except FileNotFoundError:
            self.close_connection = True
            return
        with fh:
            try:
                STORE.stream_file(fh, key, self.wfile.write, self.wfile.flush, sim)
            except (ConnectionAbortedError, BrokenPipeError, ConnectionResetError) as exc:
                log.warning("取档中断 %s：%s", key, exc)
                self.close_connection = True

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError) as exc:
            self._json(400, {"detail": f"JSON 解析失败：{exc}"})
            return None

    def _json(self, status: int, payload) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    log.info("对象源监听 0.0.0.0:%d，数据目录 %s", PORT, DATA_DIR)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
