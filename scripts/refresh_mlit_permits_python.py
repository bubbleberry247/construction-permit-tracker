"""
GAS rolling 代替: Python から MLIT 検索を直接実行して MLITPermits シートを更新。

GAS の runDailyMlitRolling() が「最終同期から 30 日未満は対象外」のため動かない場合の代替。
verify_permits_full.py の API 関数を再利用。

Usage:
  python scripts/refresh_mlit_permits_python.py --dry-run     # 対象一覧表示のみ
  python scripts/refresh_mlit_permits_python.py --execute     # 全対象更新
  python scripts/refresh_mlit_permits_python.py --execute --company-id C0117  # 特定社のみ
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, date
from pathlib import Path

import requests
import gspread
from google.oauth2.service_account import Credentials

PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT / "src"))
sys.stdout.reconfigure(encoding="utf-8")

from verify_permits_full import (
    get_license_no_kbn, get_pref_code, search_permit, fetch_detail,
    REQUEST_INTERVAL_SEC,
)

CONFIG = PROJECT / "config.json"
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


def parse_days_remaining(expiry_iso: str) -> int | None:
    if not expiry_iso:
        return None
    try:
        d = date.fromisoformat(expiry_iso)
        return (d - date.today()).days
    except ValueError:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--company-id", help="特定 company_id のみ")
    ap.add_argument("--limit", type=int, default=99, help="最大処理件数")
    args = ap.parse_args()

    sh = open_sheet()
    ws = sh.worksheet("MLITPermits")
    rows = ws.get_all_values()
    hdr = rows[0]

    cid_i = hdr.index("company_id")
    cnam_i = hdr.index("company_name")
    pnum_i = hdr.index("permit_number")
    auth_i = hdr.index("authority")
    cat_i = hdr.index("category")
    exp_i = hdr.index("expiry_date")
    expw_i = hdr.index("expiry_wareki")
    days_i = hdr.index("days_remaining")
    ti_i = hdr.index("trades_ippan")
    tt_i = hdr.index("trades_tokutei")
    tc_i = hdr.index("trades_count")
    fs_i = hdr.index("fetch_status")
    ls_i = hdr.index("last_synced")

    # 対象選定: fetch_status=OK のみ、期限近い順
    targets = []
    for row_idx, r in enumerate(rows[1:], start=2):
        if len(r) <= max(cid_i, fs_i): continue
        if r[fs_i] != "OK": continue
        if args.company_id and r[cid_i] != args.company_id: continue
        try:
            days = int(r[days_i]) if r[days_i].lstrip("-").isdigit() else 99999
        except (ValueError, IndexError):
            days = 99999
        targets.append({
            "row": row_idx,
            "cid": r[cid_i],
            "name": r[cnam_i],
            "auth": r[auth_i],
            "pnum": r[pnum_i],
            "days": days,
            "last_synced": r[ls_i] if len(r) > ls_i else "",
        })
    targets.sort(key=lambda x: x["days"])
    targets = targets[:args.limit]

    print(f"対象: {len(targets)} 社\n")
    for t in targets:
        print(f"  {t['days']:>5}  {t['cid']:<8} {t['name'][:30]:<30} {t['auth']} {t['pnum']}")

    if not args.execute:
        print("\n[DRY-RUN] --execute で実行")
        return

    # MLIT 検索 + 詳細取得 + シート更新
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; FDE-rolling/1.0)"})

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats = {"updated": 0, "not_found": 0, "error": 0}
    cell_updates = []

    for i, t in enumerate(targets, 1):
        print(f"\n[{i}/{len(targets)}] {t['cid']} {t['name']} {t['auth']} {t['pnum']}")
        kbn = get_license_no_kbn(t["auth"])
        pref = get_pref_code(t["auth"])

        try:
            sv = search_permit(session, kbn, t["pnum"], pref)
        except Exception as e:
            print(f"  [ERROR search] {e}")
            stats["error"] += 1
            cell_updates.append({"range": gspread.utils.rowcol_to_a1(t["row"], ls_i + 1), "values": [[now_str]]})
            time.sleep(REQUEST_INTERVAL_SEC)
            continue

        if not sv:
            print(f"  [NOT_FOUND]")
            cell_updates.append({"range": gspread.utils.rowcol_to_a1(t["row"], fs_i + 1), "values": [["NOT_FOUND_AT_REFRESH"]]})
            cell_updates.append({"range": gspread.utils.rowcol_to_a1(t["row"], ls_i + 1), "values": [[now_str]]})
            stats["not_found"] += 1
            time.sleep(REQUEST_INTERVAL_SEC)
            continue

        time.sleep(REQUEST_INTERVAL_SEC)
        try:
            detail = fetch_detail(session, sv)
        except Exception as e:
            print(f"  [ERROR detail] {e}")
            stats["error"] += 1
            cell_updates.append({"range": gspread.utils.rowcol_to_a1(t["row"], ls_i + 1), "values": [[now_str]]})
            time.sleep(REQUEST_INTERVAL_SEC)
            continue

        if not detail.found:
            print(f"  [PARSE_FAIL] {detail.error}")
            cell_updates.append({"range": gspread.utils.rowcol_to_a1(t["row"], ls_i + 1), "values": [[now_str]]})
            stats["error"] += 1
            time.sleep(REQUEST_INTERVAL_SEC)
            continue

        # 成功 → 全フィールド更新
        days_remain = parse_days_remaining(detail.api_expiry_to)
        has_ippan = bool(detail.api_trades_ippan)
        has_tokutei = bool(detail.api_trades_tokutei)
        category = "般特" if has_ippan and has_tokutei else ("般" if has_ippan else ("特" if has_tokutei else ""))
        all_trades = set(detail.api_trades_ippan) | set(detail.api_trades_tokutei)

        updates_for_row = [
            (exp_i, detail.api_expiry_to or ""),
            (expw_i, detail.api_expiry_wareki or ""),
            (days_i, days_remain if days_remain is not None else ""),
            (ti_i, "|".join(detail.api_trades_ippan)),
            (tt_i, "|".join(detail.api_trades_tokutei)),
            (tc_i, len(all_trades)),
            (cat_i, category),
            (fs_i, "OK"),
            (ls_i, now_str),
        ]
        for col_i, val in updates_for_row:
            cell_updates.append({
                "range": gspread.utils.rowcol_to_a1(t["row"], col_i + 1),
                "values": [[str(val)]],
            })
        print(f"  [OK] expiry={detail.api_expiry_to} days={days_remain} trades={len(all_trades)}")
        stats["updated"] += 1
        time.sleep(REQUEST_INTERVAL_SEC)

    # batch update
    if cell_updates:
        print(f"\n=== シート更新中: {len(cell_updates)} cells ===")
        ws.batch_update(cell_updates, value_input_option="RAW")
        print(f"完了")

    print(f"\n=== 結果 ===")
    print(f"  更新成功:  {stats['updated']}")
    print(f"  NOT_FOUND: {stats['not_found']}")
    print(f"  エラー:    {stats['error']}")


if __name__ == "__main__":
    main()
