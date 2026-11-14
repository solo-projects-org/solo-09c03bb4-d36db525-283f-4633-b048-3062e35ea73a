"""触发对象源的源端更正（原子替换 → 新强 ETag），纯标准库。

两种模式：
  # 1. 源端自己换入数据目录里的 {stem}.corrected.parquet（compose 下最省事）
  python scripts/correct_object.py --key buoy-st-ts-1735689600.parquet

  # 2. 本机读取 corrected 素材并随请求体推送（本机能看到数据目录时用）
  python scripts/correct_object.py --key buoy-st-ts-1735689600.parquet \
      --data-dir ./sample_data/objects
"""

from __future__ import annotations

import argparse
import os
import sys
import urllib.request


def main() -> int:
    parser = argparse.ArgumentParser(description="触发对象源源端更正")
    parser.add_argument("--source", default=os.environ.get("OBJECT_SOURCE_URL", "http://localhost:8080"))
    parser.add_argument("--data-dir", default=None,
                        help="更正素材所在目录；缺省时让源端自己换入数据目录里的素材")
    parser.add_argument("--key", required=True, help="目标对象 key，如 buoy-st-ts-1735689600.parquet")
    args = parser.parse_args()
    base = args.source.rstrip("/")

    if args.data_dir is None:
        req = urllib.request.Request(
            f"{base}/admin/correct_from_file/{args.key}", data=b"", method="POST",
        )
    else:
        corrected_path = os.path.join(
            args.data_dir, args.key.replace(".parquet", ".corrected.parquet")
        )
        if not os.path.exists(corrected_path):
            print(f"找不到更正素材：{corrected_path}", file=sys.stderr)
            return 2
        with open(corrected_path, "rb") as fh:
            payload = fh.read()
        req = urllib.request.Request(
            f"{base}/admin/correct/{args.key}",
            data=payload,
            method="POST",
            headers={"Content-Type": "application/vnd.apache.parquet"},
        )
    with urllib.request.urlopen(req, timeout=30) as resp:
        print(resp.read().decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
