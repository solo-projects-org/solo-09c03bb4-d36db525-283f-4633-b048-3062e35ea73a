"""向对象源注入慢下载/中途失败/旧版竞态，或清除注入（演示用，纯标准库）。

示例：
  # 慢下载：每 4 KiB 块后休眠 0.2s（拉长在途窗口，演示更正与下载交错）
  python scripts/simulate_source.py --key K --delay 0.2 --chunk-bytes 4096

  # 发送 1024 字节后中途断开
  python scripts/simulate_source.py --key K --fail-after 1024

  # 复现修复前竞态：GET 先发当前版本响应头，3 秒后才打开路径取内容
  # （休眠期间更正对象 → 等长新版字节挂在旧版 ETag 下发出）
  python scripts/simulate_source.py --key K --stale-get --open-delay 3

  python scripts/simulate_source.py --key K --clear   # 清除注入
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.request


def main() -> int:
    parser = argparse.ArgumentParser(description="对象源故障/延迟/旧竞态注入")
    parser.add_argument("--source", default=os.environ.get("OBJECT_SOURCE_URL", "http://localhost:8080"))
    parser.add_argument("--key", required=True)
    parser.add_argument("--delay", type=float, default=0.0, help="每个块后的休眠秒数")
    parser.add_argument("--chunk-bytes", type=int, default=256 * 1024, help="流式块大小（调小可拉长在途窗口）")
    parser.add_argument("--fail-after", type=int, default=0, help="发送多少字节后中途断开；0 表示不断")
    parser.add_argument("--stale-get", action="store_true",
                        help="复现修复前竞态：先发响应头、延迟后才打开路径")
    parser.add_argument("--open-delay", type=float, default=0.0,
                        help="stale-get 模式下，发头后到打开路径前的休眠秒数")
    parser.add_argument("--clear", action="store_true", help="清除该 key 的注入")
    args = parser.parse_args()

    base = args.source.rstrip("/")
    if args.clear:
        req = urllib.request.Request(f"{base}/admin/simulate/{args.key}", method="DELETE")
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(resp.read().decode())
        return 0

    body = json.dumps(
        {
            "key": args.key,
            "delay_per_chunk": args.delay,
            "fail_after_bytes": args.fail_after,
            "chunk_bytes": args.chunk_bytes,
            "stale_get": args.stale_get,
            "open_delay_seconds": args.open_delay,
        }
    ).encode()
    req = urllib.request.Request(
        f"{base}/admin/simulate",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        print(resp.read().decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
