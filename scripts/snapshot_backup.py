"""
Phase R-0: 着手前スナップショットバックアップ。

対象:
  - data/permit_tracker.db (sqlite3 で integrity_check)
  - output/FDE_MANAGED/00_一覧表_v2_*.xlsx
  - output/FDE_MANAGED/00_突合せレポート_v2_*.xlsx
  - output/delivery_zips/FDE_MANAGED_*.zip (zipfile -t)

出力:
  - data/snapshots/permit_tracker.db.<TS>.bak
  - output/snapshots/<original_name>.<TS>.bak.<ext>
  - data/snapshots/manifest_<TS>.json (sha256, size, integrity)

Usage:
  python scripts/snapshot_backup.py [--ts <TS>]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from shutil import copy2

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "permit_tracker.db"
FDE_DIR = PROJECT / "output" / "FDE_MANAGED"
ZIP_DIR = PROJECT / "output" / "delivery_zips"
DATA_SNAP = PROJECT / "data" / "snapshots"
OUT_SNAP = PROJECT / "output" / "snapshots"


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def backup_one(src: Path, dst_dir: Path, ts: str) -> dict:
    dst_dir.mkdir(parents=True, exist_ok=True)
    suffix = src.suffix
    stem = src.name[: -len(suffix)] if suffix else src.name
    dst = dst_dir / f"{stem}.{ts}.bak{suffix}"
    copy2(src, dst)
    return {
        "src": str(src.relative_to(PROJECT)).replace("\\", "/"),
        "dst": str(dst.relative_to(PROJECT)).replace("\\", "/"),
        "size": dst.stat().st_size,
        "sha256": sha256_of(dst),
    }


def verify_db(db_path: Path) -> str:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("PRAGMA integrity_check")
        return cur.fetchone()[0]
    finally:
        conn.close()


def verify_zip(zip_path: Path) -> str:
    with zipfile.ZipFile(zip_path) as zf:
        bad = zf.testzip()
    return "ok" if bad is None else f"corrupted: {bad}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ts", help="タイムスタンプ。未指定なら現在時刻")
    args = ap.parse_args()
    ts = args.ts or datetime.now().strftime("%Y%m%d_%H%M%S")

    manifest = {
        "ts": ts,
        "created_at": datetime.now().isoformat(),
        "items": [],
    }

    # DB
    if DB_PATH.exists():
        info = backup_one(DB_PATH, DATA_SNAP, ts)
        info["integrity"] = verify_db(Path(PROJECT / info["dst"]))
        manifest["items"].append(info)
        print(f"[DB] {info['src']} -> {info['dst']}  integrity={info['integrity']}")

    # FDE Excel
    for pattern in ["00_一覧表_v2_*.xlsx", "00_突合せレポート_v2_*.xlsx"]:
        for src in sorted(FDE_DIR.glob(pattern)):
            info = backup_one(src, OUT_SNAP, ts)
            manifest["items"].append(info)
            print(f"[XLSX] {info['src']} -> {info['dst']}  size={info['size']}")

    # ZIP
    for src in sorted(ZIP_DIR.glob("FDE_MANAGED_*.zip")):
        # スナップショット ZIP (.bak.) は再バックアップしない
        if ".bak." in src.name:
            continue
        info = backup_one(src, OUT_SNAP, ts)
        info["integrity"] = verify_zip(Path(PROJECT / info["dst"]))
        manifest["items"].append(info)
        print(f"[ZIP] {info['src']} -> {info['dst']}  integrity={info['integrity']}")

    # manifest 保存
    DATA_SNAP.mkdir(parents=True, exist_ok=True)
    manifest_path = DATA_SNAP / f"manifest_{ts}.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print()
    print(f"=== Snapshot complete ===")
    print(f"  TS: {ts}")
    print(f"  Items: {len(manifest['items'])}")
    print(f"  Manifest: {manifest_path.relative_to(PROJECT)}")


if __name__ == "__main__":
    main()
