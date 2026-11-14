"""生成浮标温盐日 Parquet 样例。

每份日档固定三列：
- ``utc_second``    ：UTC 秒（int64，单调递增、不重复）
- ``temperature_c`` ：海表温度（摄氏度，含少量缺失）
- ``salinity_psu``  ：实用盐度（PSU，含少量缺失）

同时写 ``manifest.json`` 记录每份日档的当日 UTC 秒范围，供网页预填演示窗口。
另生成一份 ``*.corrected.parquet`` 作为源端更正演示素材：**更正版与原版
字节数完全相同**（用 schema 自定义元数据精确填充），只有内容哈希
（强 ETag）能区分两版 —— 复现“等长更正”这一最棘手的交错场景。
"""

from __future__ import annotations

import argparse
import json
import math
import os

import pyarrow as pa
import pyarrow.parquet as pq

DAY_SECONDS = 86_400
COLUMNS = ["utc_second", "temperature_c", "salinity_psu"]
PAD_META_KEY = b"buoy_qc_pad"
# 等长目标 = 两版基础大小的较大者 + 填充余量（须大于填充键的固定编码开销）
PAD_SLACK_BYTES = 256


def build_day(day_start_utc: int, step_seconds: int, seed: int) -> pa.Table:
    count = DAY_SECONDS // step_seconds
    utc = [day_start_utc + i * step_seconds for i in range(count)]

    temps, sals = [], []
    for i in range(count):
        phase = 2 * math.pi * (i * step_seconds) / DAY_SECONDS
        # 日变化 + 仪器噪声；每 211 个点留一个缺失，模拟真实浮标。
        t = 18.0 + 2.6 * math.sin(phase - 1.1) + 0.15 * math.sin(seed + i * 0.37)
        s = 34.7 + 0.25 * math.cos(phase) + 0.05 * math.cos(seed + i * 0.21)
        temps.append(None if i % 211 == 37 else round(t, 3))
        sals.append(None if i % 173 == 91 else round(s, 3))

    return pa.table(
        {
            "utc_second": pa.array(utc, type=pa.int64()),
            "temperature_c": pa.array(temps, type=pa.float64()),
            "salinity_psu": pa.array(sals, type=pa.float64()),
        }
    )


def write_parquet(table: pa.Table, path: str) -> None:
    pq.write_table(
        table,
        path,
        compression="snappy",
        # 显式行组大小，方便演示 PyArrow 行组过滤下推。
        row_group_size=2048,
        use_dictionary=False,
    )


def _write_with_pad(table: pa.Table, path: str, pad_len: int) -> None:
    meta = dict(table.schema.metadata or {})
    if pad_len > 0:
        meta[PAD_META_KEY] = b"x" * pad_len
    else:
        meta.pop(PAD_META_KEY, None)
    pq.write_table(
        table.replace_schema_metadata(meta or None),
        path,
        compression="snappy",
        row_group_size=2048,
        use_dictionary=False,
    )


def _size_with_pad(table: pa.Table, path: str, pad_len: int) -> int:
    _write_with_pad(table, path, pad_len)
    return os.path.getsize(path)


def _pad_exact(table: pa.Table, path: str, target_size: int) -> bool:
    """尝试把文件精确填充到 ``target_size``；不可达返回 False。

    文件大小随填充长度单调不减（页脚元数据只会变长），但 base64/varint
    编码让个别长度不可达 —— 二分找到首个不小于目标的填充量，命中即精确。
    """
    if _size_with_pad(table, path, 0) > target_size:
        return False
    lo, hi = 0, 8
    while _size_with_pad(table, path, hi) < target_size:
        hi *= 2
    while lo < hi:
        mid = (lo + hi) // 2
        if _size_with_pad(table, path, mid) >= target_size:
            hi = mid
        else:
            lo = mid + 1
    return _size_with_pad(table, path, lo) == target_size


def write_parquet_padded_to(table: pa.Table, path: str, target_size: int) -> None:
    """写 Parquet 并用 schema 自定义元数据把文件精确填充到 ``target_size`` 字节。

    填充字节放在页脚键值元数据里，读取端（PyArrow Dataset/ParquetFile）
    会忽略未知键，查询结果不受影响；文件内容变了，强 ETag 随之改变。
    """
    if not _pad_exact(table, path, target_size):
        raise ValueError(f"目标长度 {target_size} 不可精确填充")


def write_parquet_pair_equal_size(
    table_a: pa.Table,
    path_a: str,
    table_b: pa.Table,
    path_b: str,
    slack: int = PAD_SLACK_BYTES,
) -> int:
    """写两份 Parquet 并填充到相同字节数，返回最终长度。

    个别长度不可精确填充时上调共同目标（编码间隙最多十几字节），
    保证两份文件最终等长 —— 等长更正场景的关键素材。
    """
    base_a = _size_with_pad(table_a, path_a, 0)
    base_b = _size_with_pad(table_b, path_b, 0)
    target = max(base_a, base_b) + slack
    for _ in range(4 * slack):
        if _pad_exact(table_a, path_a, target) and _pad_exact(table_b, path_b, target):
            return target
        target += 1
    raise RuntimeError("无法找到两份文件共同的等长目标")


def main() -> None:
    parser = argparse.ArgumentParser(description="生成温盐日 Parquet 样例")
    parser.add_argument("--out", default="/data/objects", help="输出目录")
    parser.add_argument("--days", type=int, default=4, help="生成天数（默认 4）")
    parser.add_argument(
        "--day-start",
        type=int,
        default=1735689600,  # 2025-01-01T00:00:00Z
        help="首日 00:00 UTC 秒",
    )
    parser.add_argument("--step", type=int, default=10, help="采样间隔秒（默认 10s）")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    manifest: dict[str, dict] = {}

    for d in range(args.days):
        day_start = args.day_start + d * DAY_SECONDS
        key = f"buoy-st-ts-{day_start}.parquet"
        table = build_day(day_start, args.step, seed=d + 1)
        path = os.path.join(args.out, key)
        # 更正素材：同一日档、带漂移偏差的新版本，文件名带 .corrected。
        ctable = build_day(day_start, args.step, seed=100 + d)
        cpath = os.path.join(args.out, f"buoy-st-ts-{day_start}.corrected.parquet")

        # 等长更正：两版内容不同但字节数一致，只有强 ETag（内容哈希）能区分。
        target = write_parquet_pair_equal_size(table, path, ctable, cpath)

        manifest[key] = {
            "day_start_utc": day_start,
            "day_end_utc": day_start + DAY_SECONDS,
            "step_seconds": args.step,
            "rows": table.num_rows,
        }
        print(f"wrote {key} ({table.num_rows} rows, {target} B) + 等长 corrected 素材")

    with open(os.path.join(args.out, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    print(f"manifest -> {os.path.join(args.out, 'manifest.json')}")


if __name__ == "__main__":
    main()
