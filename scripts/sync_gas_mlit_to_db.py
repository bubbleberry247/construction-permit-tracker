"""
sync_gas_mlit_to_db.py — GAS MLITPermits シート → ローカル DB permits テーブル に同期。

逆方向 (DB→GAS) は sync_db_master_to_gas.py / sync_to_sheets.py が既存。
本スクリプトは GAS に毎日 rolling で蓄積される MLIT データをローカル DB に取り込む。

MLITPermits シート列 (sync_to_sheets.py 由来):
  company_id, company_name, permit_number, authority, category,
  expiry_date, expiry_wareki, days_remaining,
  trades_ippan, trades_tokutei, trades_count, fetch_status, last_synced

ローカル permits テーブル列:
  permit_id (auto), company_id, permit_number, permit_authority, permit_category,
  permit_year (省略), issue_date (空), expiry_date, current_flag (1), source

ローカル permit_trades テーブル:
  permit_id, trade_name (一般・特定問わず)

Usage:
  python scripts/sync_gas_mlit_to_db.py            # dry-run
  python scripts/sync_gas_mlit_to_db.py --execute  # 本実行
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
CONFIG = PROJECT / "config.json"
DB = PROJECT / "data" / "permit_tracker.db"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def open_sheet():
    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    creds = Credentials.from_service_account_file(
        cfg["GOOGLE_SERVICE_ACCOUNT_FILE"], scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(cfg["GOOGLE_SHEETS_ID"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    print("=== GAS MLITPermits → DB permits 同期 ===")
    print(f"  mode: {'EXECUTE' if args.execute else 'DRY-RUN'}\n")

    # Read GAS sheet
    sh = open_sheet()
    ws = sh.worksheet("MLITPermits")
    rows = ws.get_all_records()
    print(f"  MLITPermits シート: {len(rows)} 行\n")

    # フィルタ: permit_number あり, fetch_status=OK 系
    valid = [r for r in rows if r.get("permit_number") and r.get("company_id")]
    print(f"  有効行 (permit_number + cid あり): {len(valid)}\n")

    conn = sqlite3.connect(str(DB))
    plan_insert = []
    plan_update = []
    plan_skip = []

    for r in valid:
        cid = r["company_id"]
        pnum = str(r["permit_number"]).strip()
        if not pnum or pnum == "":
            continue
        # 既存レコードチェック
        existing = conn.execute(
            "SELECT permit_id FROM permits WHERE company_id=? AND permit_number=?",
            (cid, pnum),
        ).fetchone()
        if existing:
            plan_update.append((r, existing[0]))
        else:
            plan_insert.append(r)

    print(f"  INSERT 予定: {len(plan_insert)} 行")
    print(f"  UPDATE 予定: {len(plan_update)} 行")
    print()

    if not args.execute:
        print("[DRY-RUN] DB は変更されません。--execute で実行。")
        # 上位 10 表示
        print("\n=== INSERT 予定 上位 10 ===")
        for r in plan_insert[:10]:
            print(f"  {r['company_id']} {r['company_name'][:25]:25} | {r['permit_number']:25} | {r['expiry_date']}")
        return

    inserted = updated = trades_inserted = 0
    conn.execute("BEGIN")
    try:
        # INSERT
        for r in plan_insert:
            cur = conn.execute(
                "INSERT INTO permits (company_id, permit_number, permit_authority, "
                "permit_category, expiry_date, current_flag, source) "
                "VALUES (?, ?, ?, ?, ?, 1, 'gas_mlit_sync')",
                (r["company_id"], r["permit_number"], r.get("authority", ""),
                 r.get("category", ""), r.get("expiry_date") or None),
            )
            permit_id = cur.lastrowid
            inserted += 1
            # trades 一般+特定 を分割して INSERT
            for trade_field in ("trades_ippan", "trades_tokutei"):
                trades_str = r.get(trade_field, "") or ""
                for t in trades_str.replace("、", ",").replace("／", ",").replace("/", ",").split(","):
                    t = t.strip()
                    if t:
                        conn.execute(
                            "INSERT INTO permit_trades (permit_id, trade_name) VALUES (?, ?)",
                            (permit_id, t),
                        )
                        trades_inserted += 1

        # UPDATE (既存 permit の expiry_date / authority / category を更新)
        for r, permit_id in plan_update:
            conn.execute(
                "UPDATE permits SET permit_authority=?, permit_category=?, "
                "expiry_date=?, current_flag=1, source='gas_mlit_sync', "
                "updated_at=datetime('now','localtime') WHERE permit_id=?",
                (r.get("authority", ""), r.get("category", ""),
                 r.get("expiry_date") or None, permit_id),
            )
            updated += 1
            # trades は full replace
            conn.execute("DELETE FROM permit_trades WHERE permit_id=?", (permit_id,))
            for trade_field in ("trades_ippan", "trades_tokutei"):
                trades_str = r.get(trade_field, "") or ""
                for t in trades_str.replace("、", ",").replace("／", ",").replace("/", ",").split(","):
                    t = t.strip()
                    if t:
                        conn.execute(
                            "INSERT INTO permit_trades (permit_id, trade_name) VALUES (?, ?)",
                            (permit_id, t),
                        )
                        trades_inserted += 1

        conn.commit()
        print(f"\n✓ commit: INSERT {inserted} / UPDATE {updated} / trades {trades_inserted}")
    except Exception as e:
        conn.rollback()
        print(f"\n✗ rollback: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
