"""
全 pages の file_hash 列を実ファイル sha256 で再計算して埋める。

GPT-5.5 推奨: legacy file_hash は信用せず、recomputed sha256 を dedup key に使う。

Usage:
  python scripts/recompute_file_hashes.py --dry-run
  python scripts/recompute_file_hashes.py --execute
"""
from __future__ import annotations

import argparse
import hashlib
import sqlite3
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
ORIGINALS = PROJECT / "data" / "originals"
sys.stdout.reconfigure(encoding="utf-8")


def file_sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def find_actual_path(file_name: str, cid: str) -> Path | None:
    for c in ORIGINALS.glob(f"{cid}_*"):
        for p in c.rglob(file_name):
            if p.is_file():
                return p
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()
    if not args.dry_run and not args.execute:
        print("ERROR: --dry-run か --execute", file=sys.stderr); sys.exit(1)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # ユニーク (cid, file_name)
    files = list(conn.execute(
        "SELECT DISTINCT company_id, file_name FROM pages WHERE is_active=1"
    ).fetchall())
    print(f"対象 (cid, file): {len(files)}", file=sys.stderr)

    n_updated = 0
    n_already = 0
    n_no_path = 0
    n_changed = 0  # 既存 hash と異なる場合

    if args.execute:
        conn.execute("BEGIN IMMEDIATE")

    for r in files:
        cid = r["company_id"]
        fn = r["file_name"]
        p = find_actual_path(fn, cid)
        if p is None:
            n_no_path += 1
            continue
        try:
            digest = file_sha256(p)
        except Exception:
            continue

        # 現在の file_hash 状態
        rows = list(conn.execute(
            "SELECT page_id, file_hash FROM pages WHERE company_id=? AND file_name=? AND is_active=1",
            (cid, fn)
        ).fetchall())
        for row in rows:
            cur = row["file_hash"]
            if cur == digest:
                n_already += 1
                continue
            if cur and cur != digest:
                n_changed += 1
            n_updated += 1
            if args.execute:
                conn.execute("UPDATE pages SET file_hash=? WHERE page_id=?", (digest, row["page_id"]))

    if args.execute:
        conn.commit()

    print(f"\n=== 結果 ===")
    print(f"  対象 ファイル:        {len(files)}")
    print(f"  既に正しい:          {n_already}")
    print(f"  更新:               {n_updated}")
    print(f"  既存値と差異:        {n_changed}")
    print(f"  実ファイル未検出:    {n_no_path}")
    if args.dry_run:
        print(f"\n[dry-run] --execute で実行")


if __name__ == "__main__":
    main()
