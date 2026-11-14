"""对象源：强 ETag、清单自动刷新、故障注入与源端更正。"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from urllib.request import Request, urlopen

import pytest

from scripts.generate_samples import build_day, write_parquet

from conftest import DAY_START, DAY_SECONDS


@pytest.fixture()
def http_source(object_source):
    base_url, store = object_source
    return base_url, store


def _get_json(url: str):
    with urlopen(url, timeout=5) as resp:
        return resp.status, json.loads(resp.read().decode())


def _sha256_etag(data: bytes) -> str:
    return '"' + hashlib.sha256(data).hexdigest() + '"'


def test_catalog_includes_manifest_fields(tmp_path):
    from object_source.server import ObjectStore

    d = tmp_path / "objs"
    d.mkdir()
    write_parquet(build_day(DAY_START, 60, 1), d / "d.parquet")
    (d / "manifest.json").write_text(json.dumps({
        "d.parquet": {"day_start_utc": DAY_START, "day_end_utc": DAY_START + DAY_SECONDS}
    }))
    store = ObjectStore(str(d))
    item = store.catalog()[0]
    assert item["key"] == "d.parquet"
    assert item["day_start_utc"] == DAY_START
    assert item["etag"].startswith('"') and not item["etag"].startswith(("W/", 'w/'))


def test_head_exposes_strong_etag_and_length(http_source):
    base_url, _ = http_source
    req = Request(f"{base_url}/objects/day1.parquet", method="HEAD")
    with urlopen(req, timeout=5) as resp:
        assert resp.status == 200
        etag = resp.headers["ETag"]
        assert etag.startswith('"') and not etag.startswith(("W/", "w/"))
        assert int(resp.headers["Content-Length"]) > 0


def test_auto_refresh_picks_up_new_file(http_source, object_dir):
    base_url, store = http_source
    before = {x["key"] for x in store.catalog()}
    assert "newday.parquet" not in before
    write_parquet(build_day(DAY_START + 2 * DAY_SECONDS, 60, 7), object_dir / "newday.parquet")
    # 不重启进程，下一次 catalog 调用应自动发现新文件。
    _, catalog = _get_json(f"{base_url}/objects")
    assert any(x["key"] == "newday.parquet" for x in catalog)
    # 新对象可直接 HEAD/GET。
    req = Request(f"{base_url}/objects/newday.parquet", method="HEAD")
    with urlopen(req, timeout=5) as resp:
        assert resp.status == 200


def test_correction_changes_etag(http_source, object_dir):
    base_url, store = http_source
    old_etag = store.meta_for("day1.parquet")["etag"]
    payload = (object_dir / "day1.corrected.parquet").read_bytes()
    new_meta = store.correct("day1.parquet", payload)
    assert new_meta["etag"] != old_etag
    # 通过 HTTP 读到的 ETag 已更新。
    req = Request(f"{base_url}/objects/day1.parquet", method="HEAD")
    with urlopen(req, timeout=5) as resp:
        assert resp.headers["ETag"] == new_meta["etag"]


def test_corrected_sample_is_equal_length(http_source):
    """样例更正素材与原版等长：只有内容哈希能区分两版（等长更正场景）。"""
    _, store = http_source
    orig = store.meta_for("day1.parquet")
    corrected = store.meta_for("day1.corrected.parquet")
    assert orig["size"] == corrected["size"]
    assert orig["etag"] != corrected["etag"]


def test_correct_from_file_endpoint(http_source):
    base_url, store = http_source
    old = store.meta_for("day1.parquet")
    req = Request(f"{base_url}/admin/correct_from_file/day1.parquet",
                  data=b"", method="POST")
    with urlopen(req, timeout=5) as resp:
        body = json.loads(resp.read().decode())
    assert body["meta"]["etag"] != old["etag"]
    assert body["meta"]["size"] == old["size"]  # 等长更正素材
    assert store.meta_for("day1.parquet")["etag"] == body["meta"]["etag"]


def test_get_consistent_when_corrected_midstream(http_source):
    """修复核心：GET 在途时发生更正，响应头 ETag 与实际字节仍属同一完整版本。"""
    base_url, store = http_source
    old_meta = store.meta_for("day1.parquet")
    # 慢速发送，拉长在途窗口，让更正落在传输中途。
    store.set_simulation("day1.parquet", delay=0.05, chunk_bytes=1024)
    try:
        received: dict = {}

        def fetch():
            with urlopen(f"{base_url}/objects/day1.parquet", timeout=30) as resp:
                received["etag"] = resp.headers["ETag"]
                received["length"] = int(resp.headers["Content-Length"])
                received["body"] = resp.read()

        thread = threading.Thread(target=fetch)
        thread.start()
        time.sleep(0.3)  # 传输进行中更正对象（原子替换 → 新 inode）
        store.correct_from_file("day1.parquet")
        thread.join(timeout=30)
        assert not thread.is_alive()

        # 在途响应完整地发了旧版：ETag、长度、字节三者同源。
        assert received["etag"] == old_meta["etag"]
        assert received["length"] == old_meta["size"]
        assert len(received["body"]) == old_meta["size"]
        assert _sha256_etag(received["body"]) == received["etag"]
        # 之后的 HEAD 看到新版本。
        assert store.meta_for("day1.parquet")["etag"] != old_meta["etag"]
    finally:
        store.clear_simulation("day1.parquet")


def test_stale_get_injection_reproduces_version_mixup(http_source):
    """故障注入复现修复前竞态：旧版响应头 + 等长新版字节（供客户端校验演示）。"""
    base_url, store = http_source
    old_meta = store.meta_for("day1.parquet")
    store.set_simulation("day1.parquet", stale_get=True, open_delay=1.0)
    try:
        received: dict = {}

        def fetch():
            with urlopen(f"{base_url}/objects/day1.parquet", timeout=30) as resp:
                received["etag"] = resp.headers["ETag"]
                received["length"] = int(resp.headers["Content-Length"])
                received["body"] = resp.read()

        thread = threading.Thread(target=fetch)
        thread.start()
        time.sleep(0.3)  # 响应头已发、路径未开：更正落进休眠窗口
        new_meta = store.correct_from_file("day1.parquet")
        thread.join(timeout=30)
        assert not thread.is_alive()

        # 复现成功：响应头是旧版，字节是等长新版 —— 只有内容哈希能识破。
        assert received["etag"] == old_meta["etag"]
        assert received["length"] == old_meta["size"] == len(received["body"])
        assert _sha256_etag(received["body"]) == new_meta["etag"] != received["etag"]
    finally:
        store.clear_simulation("day1.parquet")


def test_simulation_fail_after_aborts_transfer(http_source):
    base_url, store = http_source
    store.set_simulation("day1.parquet", delay=0.0, fail_after=256)
    try:
        with pytest.raises(Exception):  # 连接被源端中途切断
            urlopen(f"{base_url}/objects/day1.parquet", timeout=5).read()
    finally:
        store.clear_simulation("day1.parquet")
    # 清除注入后恢复完整可读。
    with urlopen(f"{base_url}/objects/day1.parquet", timeout=5) as resp:
        data = resp.read()
    assert len(data) == store.meta_for("day1.parquet")["size"]


def test_404_for_missing_object(http_source):
    base_url, _ = http_source
    from urllib.error import HTTPError
    with pytest.raises(HTTPError) as exc:
        urlopen(f"{base_url}/objects/missing.parquet", timeout=5)
    assert exc.value.code == 404
