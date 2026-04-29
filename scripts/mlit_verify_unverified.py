"""
GAS Permits シートで mlit_confirmed_date が空（=未検証）の許可を MLIT 検索 → 結果を mlit_* 列に書き込む。

抽出条件:
  - parse_status='OK' の行のみ（INCOMPLETE は除外）
  - mlit_confirmed_date が空
  - permit_number_full と permit_authority_name が両方ある

書き込み列:
  - mlit_confirmed_date: 実行日 ISO
  - mlit_confirm_result: OK / NOT_FOUND / EXPIRED / ERROR:<reason>
  - mlit_screenshot_url: 詳細URL（kensetsu_kensaku 結果ページ）

Usage:
    python scripts/mlit_verify_unverified.py             # dry-run（対象社一覧のみ）
    python scripts/mlit_verify_unverified.py --execute   # 実行 + 書込
    python scripts/mlit_verify_unverified.py --execute --limit 5
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import gspread
from google.oauth2.service_account import Credentials
import requests

from verify_permits_full import (
    fetch_detail,
    get_license_no_kbn,
    get_pref_code,
    search_permit,
)

PROJECT = Path(__file__).resolve().parent.parent
CONFIG = PROJECT / "config.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SAFE_INTERVAL_BASE = 2.0
JITTER_MIN = 0.3
JITTER_MAX = 0.8

PERMITS_HEADERS = [
    "permit_id", "company_id", "company_name_raw",
    "permit_authority_name", "permit_authority_name_normalized", "permit_authority_type",
    "permit_category", "permit_year", "contractor_number", "permit_number_full",
    "trade_categories", "issue_date", "expiry_date", "renewal_deadline_date",
    "current_status", "evidence_renewal_application", "renewal_application_date",
    "mlit_confirmed_date", "mlit_confirm_result", "mlit_screenshot_url",
    "permit_file_path", "permit_file_share_url", "permit_file_version", "evidence_file_path",
    "last_received_date", "source_file", "source_file_hash",
    "parse_status", "error_category", "error_reason",
    "note", "created_at", "updated_at",
]


def safe_sleep() -> None:
    time.sleep(SAFE_INTERVAL_BASE + random.uniform(JITTER_MIN, JITTER_MAX))


PERMIT_NUM_RE = re.compile(r"第?\s*(\d{1,8})\s*号")


def extract_permit_digits(s: str) -> str:
    """'愛知県知事 許可（般-3）第33471号' → '33471'。失敗したら最長連続数字。"""
    if not s:
        return ""
    m = PERMIT_NUM_RE.search(s)
    if m:
        return m.group(1).lstrip("0") or "0"
    runs = re.findall(r"\d+", s)
    if not runs:
        return ""
    return max(runs, key=len).lstrip("0") or "0"


def normalize_authority(s: str) -> str:
    """'千葉県' → '千葉県知事'、'国土交通大臣 ...' → '国土交通大臣' に正規化"""
    if not s:
        return ""
    s = s.strip()
    if "国土交通大臣" in s:
        return "国土交通大臣"
    # 「○○県知事」「○○都知事」「○○府知事」「○○道知事」を抜き出す
    m = re.search(r"([一-鿿]+[都道府県])(?:知事)?", s)
    if m:
        return m.group(1) + "知事"
    return s


def col_letter(idx_1based: int) -> str:
    """1-based 列番号 → A, B, ..., AA, AB"""
    s = ""
    n = idx_1based
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="実書込（既定 dry-run）")
    ap.add_argument("--limit", type=int, default=0, help="先頭 N 件のみ処理（0 で全件）")
    args = ap.parse_args()

    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    sheet_id = cfg["GOOGLE_SHEETS_ID"]
    sa_path = cfg["GOOGLE_SERVICE_ACCOUNT_FILE"]
    if not Path(sa_path).exists():
        print(f"ERROR: service account not found: {sa_path}", file=sys.stderr)
        sys.exit(1)

    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet("Permits")

    # ヘッダー検証
    actual = ws.row_values(1)
    if actual[:len(PERMITS_HEADERS)] != PERMITS_HEADERS:
        print("ERROR: Permits ヘッダー不一致", file=sys.stderr)
        for i, (e, a) in enumerate(zip(PERMITS_HEADERS, actual)):
            if e != a:
                print(f"  col {i}: expected={e!r} actual={a!r}", file=sys.stderr)
        sys.exit(2)
    print(f"  [OK] Permits ヘッダー検証: {len(PERMITS_HEADERS)} 列")

    all_rows = ws.get_all_values()
    headers = all_rows[0]
    data_rows = all_rows[1:]
    H = {h: i for i, h in enumerate(headers)}

    # 対象抽出
    targets = []
    for row_idx, row in enumerate(data_rows, start=2):  # シート上の行番号は 1-origin、ヘッダー除き 2 開始
        def cell(name: str) -> str:
            i = H.get(name, -1)
            return row[i] if 0 <= i < len(row) else ""

        if cell("parse_status") != "OK":
            continue
        if cell("mlit_confirmed_date").strip():
            continue
        permit_num = cell("permit_number_full").strip()
        authority = cell("permit_authority_name").strip()
        if not permit_num or not authority:
            continue
        targets.append({
            "row": row_idx,
            "company_id": cell("company_id"),
            "company_name": cell("company_name_raw"),
            "permit_number": permit_num,
            "authority": authority,
        })

    print(f"対象（mlit_confirmed_date 空 + parse_status=OK + permit/authority あり）: {len(targets)} 行")
    if args.limit and len(targets) > args.limit:
        targets = targets[:args.limit]
        print(f"  --limit {args.limit} 適用")

    est_sec = len(targets) * (SAFE_INTERVAL_BASE + (JITTER_MIN + JITTER_MAX) / 2 + 1.5)
    print(f"  推定所要時間: 約 {est_sec/60:.1f} 分")

    if not args.execute:
        print("\n=== サンプル（先頭 20 件、正規化結果も併記） ===")
        for t in targets[:20]:
            num = extract_permit_digits(t["permit_number"])
            auth = normalize_authority(t["authority"])
            pref = get_pref_code(auth) if auth else None
            warn = "" if (num and pref) else "  ⚠ パース失敗の可能性"
            print(f"  row={t['row']} {t['company_id']} {t['company_name'][:20]} | {auth} (pref={pref}) | num={num}{warn}")
        print("\n(dry-run) 実行しません。--execute で実行してください。")
        return

    print("\n--- MLIT 検索 + 書込 開始 ---")
    today = date.today().isoformat()
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept-Language": "ja,en;q=0.9",
    })

    confirm_col = col_letter(H["mlit_confirmed_date"] + 1)
    result_col = col_letter(H["mlit_confirm_result"] + 1)
    screenshot_col = col_letter(H["mlit_screenshot_url"] + 1)
    print(f"  書込列: {confirm_col}=mlit_confirmed_date, {result_col}=mlit_confirm_result, {screenshot_col}=mlit_screenshot_url")

    updates = []  # batch_update 用
    counts = {"OK": 0, "NOT_FOUND": 0, "EXPIRED": 0, "ERROR": 0}

    for i, t in enumerate(targets, 1):
        cid = t["company_id"]
        cname = t["company_name"]
        permit_raw = t["permit_number"]
        authority_raw = t["authority"]

        permit = extract_permit_digits(permit_raw)
        authority = normalize_authority(authority_raw)

        if not permit or not authority:
            result_str = f"ERROR:parse:permit={permit_raw!r},auth={authority_raw!r}"
            counts["ERROR"] += 1
            updates.append({"range": f"{confirm_col}{t['row']}", "values": [[today]]})
            updates.append({"range": f"{result_col}{t['row']}", "values": [[result_str]]})
            print(f"  [{i}/{len(targets)}] row={t['row']} {cid} {cname[:20]} | {result_str}")
            continue

        license_kbn = get_license_no_kbn(authority)
        pref = get_pref_code(authority)

        result_str = ""
        screenshot_url = ""
        try:
            sv = search_permit(session, license_kbn, permit, pref)
            safe_sleep()
            if not sv:
                result_str = "NOT_FOUND"
                counts["NOT_FOUND"] += 1
            else:
                detail = fetch_detail(session, sv)
                if not detail.found:
                    result_str = f"NOT_FOUND:{detail.error or 'detail empty'}"
                    counts["NOT_FOUND"] += 1
                else:
                    expiry_iso = detail.api_expiry_to or ""
                    is_expired = False
                    if expiry_iso:
                        try:
                            is_expired = date.fromisoformat(expiry_iso) < date.today()
                        except ValueError:
                            pass
                    if is_expired:
                        result_str = f"EXPIRED:{expiry_iso}"
                        counts["EXPIRED"] += 1
                    else:
                        n_trades = len(detail.api_trades_ippan) + len(detail.api_trades_tokutei)
                        result_str = f"OK:expiry={expiry_iso},trades={n_trades}"
                        counts["OK"] += 1
                    screenshot_url = sv  # 検索結果sv ID。GAS UI で詳細画面 URL に組み立て可能
        except Exception as exc:
            result_str = f"ERROR:{type(exc).__name__}:{str(exc)[:60]}"
            counts["ERROR"] += 1

        updates.append({"range": f"{confirm_col}{t['row']}", "values": [[today]]})
        updates.append({"range": f"{result_col}{t['row']}", "values": [[result_str]]})
        if screenshot_url:
            updates.append({"range": f"{screenshot_col}{t['row']}", "values": [[screenshot_url]]})

        print(f"  [{i}/{len(targets)}] row={t['row']} {cid} {cname[:20]} | {result_str}")

        # 100 件ごとに途中保存
        if i % 50 == 0:
            ws.batch_update(updates, value_input_option="RAW")
            print(f"    → {len(updates)} セル中間保存")
            updates = []

        safe_sleep()

    if updates:
        ws.batch_update(updates, value_input_option="RAW")
        print(f"    → {len(updates)} セル最終保存")

    print(f"\n=== 集計 ===")
    print(f"  OK:        {counts['OK']}")
    print(f"  NOT_FOUND: {counts['NOT_FOUND']}")
    print(f"  EXPIRED:   {counts['EXPIRED']}")
    print(f"  ERROR:     {counts['ERROR']}")
    print(f"  合計:      {sum(counts.values())} / {len(targets)}")


if __name__ == "__main__":
    main()
