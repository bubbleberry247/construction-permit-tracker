"""
MLIT 145 社 CSV (C:/tmp/mlit_145_all.csv) → ローカル DB permits + permit_trades 直接取り込み。

fetch_all_145.py の出力 CSV を読んで、GAS round-trip なしで DB を更新。

CSV 列 (CSV_HEADERS in fetch_all_145.py):
  company_id, company_name, permit_number, authority,
  category, expiry_date, days_remaining, trades, trades_count, source

Usage:
  python scripts/import_mlit_csv_to_db.py            # dry-run
  python scripts/import_mlit_csv_to_db.py --execute  # 本実行
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
CSV_PATH = Path("C:/tmp/mlit_145_all.csv")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(CSV_PATH))
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"NOT FOUND: {csv_path}")
        sys.exit(1)

    with csv_path.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    print(f"=== MLIT CSV → DB permits 取込 (mode={'EXECUTE' if args.execute else 'DRY-RUN'}) ===")
    print(f"  CSV: {csv_path}")
    print(f"  rows: {len(rows)}\n")

    valid = [r for r in rows if r.get("company_id") and r.get("permit_number") and r.get("permit_number").strip()]
    print(f"  有効行 (cid + permit_number あり): {len(valid)}")

    conn = sqlite3.connect(str(DB))
    inserts = []
    updates = []
    for r in valid:
        cid = r["company_id"]
        pnum = r["permit_number"].strip()
        existing = conn.execute(
            "SELECT permit_id FROM permits WHERE company_id=? AND permit_number=?",
            (cid, pnum),
        ).fetchone()
        if existing:
            updates.append((r, existing[0]))
        else:
            inserts.append(r)
    print(f"  INSERT 予定: {len(inserts)} 行")
    print(f"  UPDATE 予定: {len(updates)} 行\n")

    if not args.execute:
        print("=== INSERT 予定 上位 10 ===")
        for r in inserts[:10]:
            print(f"  {r['company_id']} {r['company_name'][:25]:25} | {r['permit_number']:25} | {r.get('expiry_date','')}")
        print("\n[DRY-RUN] DB は変更されません。--execute で実行。")
        return

    inserted = updated = trades_count = 0
    conn.execute("BEGIN")
    try:
        for r in inserts:
            cur = conn.execute(
                "INSERT INTO permits (company_id, permit_number, permit_authority, "
                "permit_category, expiry_date, current_flag, source) "
                "VALUES (?, ?, ?, ?, ?, 1, 'mlit_csv_import')",
                (r["company_id"], r["permit_number"], r.get("authority", ""),
                 r.get("category", ""), r.get("expiry_date") or None),
            )
            permit_id = cur.lastrowid
            inserted += 1
            trades_str = r.get("trades", "") or ""
            for t in trades_str.replace("、", ",").replace("／", ",").replace("/", ",").split(","):
                t = t.strip()
                if t:
                    conn.execute(
                        "INSERT INTO permit_trades (permit_id, trade_name) VALUES (?, ?)",
                        (permit_id, t),
                    )
                    trades_count += 1
        for r, permit_id in updates:
            conn.execute(
                "UPDATE permits SET permit_authority=?, permit_category=?, "
                "expiry_date=?, current_flag=1, source='mlit_csv_import', "
                "updated_at=datetime('now','localtime') WHERE permit_id=?",
                (r.get("authority", ""), r.get("category", ""),
                 r.get("expiry_date") or None, permit_id),
            )
            updated += 1
            conn.execute("DELETE FROM permit_trades WHERE permit_id=?", (permit_id,))
            trades_str = r.get("trades", "") or ""
            for t in trades_str.replace("、", ",").replace("／", ",").replace("/", ",").split(","):
                t = t.strip()
                if t:
                    conn.execute(
                        "INSERT INTO permit_trades (permit_id, trade_name) VALUES (?, ?)",
                        (permit_id, t),
                    )
                    trades_count += 1
        conn.commit()
        print(f"\n✓ commit: INSERT {inserted} / UPDATE {updated} / trades {trades_count}")
    except Exception as e:
        conn.rollback()
        print(f"\n✗ rollback: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
