"""演示「岸站更正 × 离线下载」交错时平台的版本一致性承诺。

前置：栈已启动（``docker compose up --build`` 或本地三进程），样例已生成。

剧本
====
第一幕（key A）：源端修复后的真实交错 —— 下载进行中发生更正
  · 先把日档 A 换成本次运行全新的有效版本（保证未缓存、必走下载）；
  · 慢速 GET 在途时更正对象 → 在途响应仍完整送达旧版（ETag/长度/字节同源）；
  · 本次查询拿到旧版 CSV；下一次查询固定新版；缓存内两版共存。

第二幕（key B）：故障注入复现旧竞态 —— 等长新版字节冒充旧版
  · 把日档 B 换成全新随机版本（保证未缓存；该版本只被固定、永不被解析）；
  · 注入 stale-get：GET 先发旧版响应头、3 秒后才打开路径；
  · 休眠期间更正为**等长**有效新版 → 客户端内容哈希识破 → HTTP 409；
  · 残件与预留被清理；其他日档读者不受影响；下一次查询固定新版（200）。

脚本结束时把两个日档恢复为运行前的原始字节，可重复运行。
退出码 0 = 全部断言通过。纯标准库 + 项目内 pyarrow（生成有效 Parquet 负载）。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request

CHUNK = 2048  # 慢下载块大小：拉长第一幕的在途窗口


def _req(method: str, url: str, body: bytes | None = None,
         headers: dict | None = None, timeout: float = 60.0):
    """返回 (status, headers, body_bytes)；响应头统一小写键；非 2xx 不抛异常。"""
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, {k.lower(): v for k, v in exc.headers.items()}, exc.read()


def _json(method: str, url: str, payload=None, timeout: float = 60.0):
    body = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"} if payload is not None else None
    status, hdrs, data = _req(method, url, body, headers, timeout)
    return status, hdrs, json.loads(data.decode() or "{}")


class QueryResult:
    def __init__(self) -> None:
        self.status = 0
        self.headers: dict = {}
        self.body = b""
        self.error: Exception | None = None


def _query_async(web: str, key: str, start: int, end: int):
    result = QueryResult()

    def run() -> None:
        try:
            url = f"{web}/api/query?key={urllib.parse.quote(key)}&start={start}&end={end}"
            result.status, result.headers, result.body = _req("GET", url, timeout=120)
        except Exception as exc:  # noqa: BLE001 - 演示脚本：如实记录
            result.error = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return result, thread


def _head(source: str, key: str) -> tuple[str, int]:
    status, headers, _ = _req("HEAD", f"{source}/objects/{key}")
    assert status == 200, f"HEAD {key} 失败：{status}"
    return headers["etag"], int(headers["content-length"])


def _simulate(source: str, key: str, **params) -> None:
    payload = {"key": key}
    payload.update(params)
    status, _, body = _json("POST", f"{source}/admin/simulate", payload)
    assert status == 200, f"注入失败：{status} {body}"


def _clear_simulation(source: str, key: str) -> None:
    status, _, _ = _req("DELETE", f"{source}/admin/simulate/{key}")
    assert status == 200, f"清除注入失败：{status}"


def _correct_payload(source: str, key: str, payload: bytes) -> dict:
    """用请求体负载原子替换对象，返回新版本元数据。"""
    status, _, data = _req(
        "POST", f"{source}/admin/correct/{key}", payload,
        {"Content-Type": "application/vnd.apache.parquet"},
    )
    body = json.loads(data.decode() or "{}")
    assert status == 200, f"源端更正失败：{status} {body}"
    return body["meta"]


def _correct_from_file(source: str, key: str) -> dict:
    """让源端换入数据目录里的 {stem}.corrected.parquet（有效 Parquet，等长素材）。"""
    status, _, body = _json("POST", f"{source}/admin/correct_from_file/{key}")
    assert status == 200, f"源端更正失败（缺 corrected 素材？）：{status} {body}"
    return body["meta"]


def _cache_entries(web: str, key: str) -> list[dict]:
    status, _, snap = _json("GET", f"{web}/api/cache")
    assert status == 200, f"读取缓存面板失败：{status}"
    return [e for e in snap["entries"] if e["key"] == key]


def _expect(cond: bool, message: str) -> None:
    print(("  ✔ " if cond else "  ✘ ") + message)
    if not cond:
        raise AssertionError(message)


def _fresh_parquet_payload(day_start_utc: int, seed: int) -> bytes:
    """造一份本次运行全新的有效日 Parquet（版本必然未被任何缓存见过）。"""
    # 以脚本位置推导项目根目录，不依赖调用时的 CWD。
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    try:
        from scripts.generate_samples import build_day, write_parquet
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "需要项目依赖 pyarrow 来生成有效 Parquet 负载；"
            "请先 pip install -r requirements.txt，或在 web 容器内运行本脚本：\n"
            "  docker compose exec web1 python scripts/demo_correction_interleave.py "
            "--web http://web1:8000 --source http://object-source:8080"
        ) from exc
    table = build_day(day_start_utc, step_seconds=60, seed=seed)
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "fresh.parquet")
        write_parquet(table, path)
        with open(path, "rb") as fh:
            return fh.read()


def main() -> int:
    parser = argparse.ArgumentParser(description="更正 × 下载交错演示")
    parser.add_argument("--web", default="http://localhost:8090", help="Web 入口（nginx 轮询）")
    parser.add_argument("--source", default="http://localhost:8080", help="对象源")
    args = parser.parse_args()
    web, source = args.web.rstrip("/"), args.source.rstrip("/")

    status, _, catalog = _json("GET", f"{source}/objects")
    assert status == 200, "对象源不可达"
    days = [it for it in catalog
            if it["key"].endswith(".parquet") and ".corrected." not in it["key"]
            and "day_start_utc" in it]
    assert len(days) >= 2, "至少需要 2 份日档（先运行 scripts/generate_samples.py）"
    key_a, key_b = days[0]["key"], days[1]["key"]
    start_a = int(days[0]["day_start_utc"])
    start_b = int(days[1]["day_start_utc"])
    print(f"日档 A：{key_a}    日档 B：{key_b}")

    #  corrected 素材（等长有效 Parquet）的全长，供第二幕造等长随机版本。
    corrected_key_b = key_b.replace(".parquet", ".corrected.parquet")
    _, corrected_size_b = _head(source, corrected_key_b)

    # 先存下当前版本字节，脚本结束时恢复原版本（可重复运行）。
    saved: dict[str, bytes] = {}
    for key in (key_a, key_b):
        status, _, data = _req("GET", f"{source}/objects/{key}")
        assert status == 200
        saved[key] = data

    try:
        # ---------------------------------------------------------- 第一幕
        print("\n第一幕：下载进行中发生源端更正（源端已修复的真实交错）")
        # 换成全新有效版本：保证未缓存，查询必走真实下载。
        fresh_a = _fresh_parquet_payload(start_a, seed=int(time.time() * 1000) % 2**31)
        meta_a1 = _correct_payload(source, key_a, fresh_a)
        etag_a1, size_a1 = meta_a1["etag"], meta_a1["size"]
        print(f"  固定旧版：ETag {etag_a1[:20]}… 全长 {size_a1} B（本次运行全新版本）")
        chunks = max(1, -(-size_a1 // CHUNK))
        _simulate(source, key_a, delay_per_chunk=round(4.0 / chunks, 3),
                  chunk_bytes=CHUNK)
        res_a, t_a = _query_async(web, key_a, start_a, start_a + 3600)
        time.sleep(1.5)  # 确认 GET 在途后再更正
        meta_a2 = _correct_from_file(source, key_a)
        print(f"  GET 在途时更正：新版 ETag {meta_a2['etag'][:20]}…")
        t_a.join(timeout=120)
        _clear_simulation(source, key_a)
        assert res_a.error is None, res_a.error
        _expect(res_a.status == 200, f"在途下载完整完成：HTTP {res_a.status}")
        got_a1 = res_a.headers.get("x-cache-etag", "")
        _expect(got_a1 == etag_a1,
                f"在途响应忠于固定旧版（X-Cache-ETag {got_a1[:20]}…）")
        csv_a_old = res_a.body

        res_a2, t_a2 = _query_async(web, key_a, start_a, start_a + 3600)
        t_a2.join(timeout=120)
        assert res_a2.error is None, res_a2.error
        _expect(res_a2.status == 200
                and res_a2.headers.get("x-cache-etag") == meta_a2["etag"],
                "下一次查询固定新版")
        _expect(res_a2.body != csv_a_old, "新旧两版 CSV 内容确实不同（更正生效）")
        entries_a = _cache_entries(web, key_a)
        etags_cached = {e["etag"] for e in entries_a if e["state"] == "complete"}
        _expect({etag_a1, meta_a2["etag"]} <= etags_cached,
                f"缓存内新旧两版共存（{len(entries_a)} 条），互不影响")

        # ---------------------------------------------------------- 第二幕
        print("\n第二幕：故障注入复现旧竞态 —— 等长新版字节冒充旧版")
        # 换成全新随机版本：等长于 corrected 素材；该版本只被固定、永不被解析
        # （stale-get 下 GET 发的是它的响应头 + 更正后的字节）。
        meta_b1 = _correct_payload(source, key_b, os.urandom(corrected_size_b))
        etag_b1, size_b1 = meta_b1["etag"], meta_b1["size"]
        _simulate(source, key_b, stale_get=True, open_delay_seconds=3.0)
        res_b, t_b = _query_async(web, key_b, start_b, start_b + 3600)
        time.sleep(1.0)  # 响应头已发、路径未开：更正落进休眠窗口
        meta_b2 = _correct_from_file(source, key_b)
        _expect(meta_b2["size"] == size_b1 and meta_b2["etag"] != etag_b1,
                f"等长更正：{size_b1} B → {meta_b2['size']} B，ETag 已变")

        # 交错期间，其他日档的读者照常取数。
        res_a3, t_a3 = _query_async(web, key_a, start_a, start_a + 600)
        t_a3.join(timeout=120)
        _expect(res_a3.status == 200, "交错期间其他日档读者照常取数（HTTP 200）")

        t_b.join(timeout=120)
        _clear_simulation(source, key_b)
        assert res_b.error is None, res_b.error
        _expect(res_b.status == 409,
                f"内容哈希识破等长新版冒充旧版：HTTP {res_b.status}")
        detail = json.loads(res_b.body.decode() or "{}").get("detail", "")
        print(f"      409 detail：{detail}")
        entries_b = _cache_entries(web, key_b)
        _expect(all(e["etag"] != etag_b1 for e in entries_b)
                and all(e["state"] == "complete" for e in entries_b),
                "残件与预留已清理：失败版本在账本与缓存中无任何痕迹")
        _expect(len(_cache_entries(web, key_a)) >= 2,
                "key A 的完整新旧版本与读者不受本次失败影响")

        res_b2, t_b2 = _query_async(web, key_b, start_b, start_b + 3600)
        t_b2.join(timeout=120)
        assert res_b2.error is None, res_b2.error
        _expect(res_b2.status == 200
                and res_b2.headers.get("x-cache-etag") == meta_b2["etag"],
                "清除注入后下一次查询固定新版（HTTP 200）")
    finally:
        print("\n收尾：清除注入并把对象恢复为原版本（脚本可重复运行）")
        for key in (key_a, key_b):
            _clear_simulation(source, key)
            _correct_payload(source, key, saved[key])

    print("\nPASS：更正与下载交错的全部一致性承诺均成立。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
