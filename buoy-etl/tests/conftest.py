"""pytest 公共夹具：临时对象源 + 两个 Web 进程共用的 SQLite/缓存目录。"""

from __future__ import annotations

import socket
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from object_source.server import Handler  # noqa: E402
from scripts.generate_samples import (  # noqa: E402
    build_day,
    write_parquet,
    write_parquet_pair_equal_size,
)

DAY_START = 1735689600  # 2025-01-01T00:00:00Z
DAY_SECONDS = 86_400


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _write_equal_length_pair(table, corrected_table, path: Path, corrected_path: Path) -> None:
    """写原版 + 等长更正版：两版内容不同、字节数一致（等长更正场景）。"""
    write_parquet_pair_equal_size(table, str(path), corrected_table, str(corrected_path))


@pytest.fixture()
def object_dir(tmp_path: Path) -> Path:
    d = tmp_path / "objects"
    d.mkdir()
    # day1 与更正素材等长：只有内容哈希（强 ETag）能区分两版。
    _write_equal_length_pair(
        build_day(DAY_START, step_seconds=60, seed=1),
        build_day(DAY_START, step_seconds=60, seed=999),
        d / "day1.parquet",
        d / "day1.corrected.parquet",
    )
    table2 = build_day(DAY_START + DAY_SECONDS, step_seconds=60, seed=2)
    write_parquet(table2, d / "day2.parquet")
    return d


@pytest.fixture()
def object_source(object_dir: Path):
    import object_source.server as srv

    store = srv.ObjectStore(str(object_dir))
    srv.STORE = store
    port = _free_port()
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"
    yield base_url, store
    httpd.shutdown()
    httpd.server_close()


@pytest.fixture()
def cache_paths(tmp_path: Path):
    return {
        "sqlite": str(tmp_path / "coord" / "cache.db"),
        "files": str(tmp_path / "files"),
    }


@pytest.fixture()
def slow_object_source(object_dir: Path):
    """带块延迟的对象源：用于验证慢下载不阻塞其他日档。"""
    import object_source.server as srv

    store = srv.ObjectStore(str(object_dir))
    srv.STORE = store
    port = _free_port()
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}", store
    httpd.shutdown()
    httpd.server_close()
