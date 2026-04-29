"""
DB current_flag=1 にあるが GAS に未表示の会社・許可証を Google Sheets に同期。

入力: data/gas_db_diff_20260424.json の db_only_sync_to_gas 13 社
出力:
  - GAS Companies シート: company_id 未登録のみ append
  - GAS Permits シート: (company_id, NFKC正規化された permit_number_full) 未登録のみ append
  - data/sheet_snapshots/sheet_snapshot_<ts>.json に実行直前の全行スナップショット保存
  - data/skipped_incomplete_permits_<ts>.json に除外した両日 null permit を退避

GPT-5.5 レビューで指摘された本番前ブロッカー対応済:
  (1) permit_id は GAS 既存 (Utilities.getUuid) と同形式の UUID v4
  (2) permit_number_full の重複判定は NFKC 正規化 + 空白/ハイフン統一後に比較
  (3a) issue_date/expiry_date 両方 null は GAS append 対象から除外し
       data/skipped_incomplete_permits_<ts>.json に退避（task 3 の手動レビューへ）
  (3b) 片方のみ欠落は INCOMPLETE マークで append
  (4) renewal_deadline_date は expiry_date - 30 日（ocr_permit.py と同じロジック）
  (5) 実シートヘッダーを実行前に検証、不一致で abort
  (6) 実書込直前に Companies/Permits 全行スナップショットを JSON 保存
  (7) value_input_option="RAW"（USER_ENTERED だと permit_number が数値化される恐れ）

Usage:
    python scripts/sync_db_only_to_gas.py             # dry-run（既定）
    python scripts/sync_db_only_to_gas.py --execute   # 実書込（事前スナップショット自動取得）
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import unicodedata
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

import gspread
from google.oauth2.service_account import Credentials

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
DIFF_JSON = PROJECT / "data" / "gas_db_diff_20260424.json"
CONFIG = PROJECT / "config.json"
SNAPSHOT_DIR = PROJECT / "data" / "sheet_snapshots"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

COMPANIES_HEADERS = [
    "company_id", "company_name_raw", "company_name_normalized",
    "representative_name", "contact_person",
    "contact_email", "contact_email_cc", "phone",
    "status", "created_at", "updated_at",
]

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

RENEWAL_DAYS_BEFORE_EXPIRY = 30  # ocr_permit.py と同じ


def normalize_name(name: str) -> str:
    if not name:
        return ""
    return (
        name.replace("㈱", "株式会社").replace("㈲", "有限会社")
        .replace("（株）", "株式会社").replace("（有）", "有限会社")
        .replace(" ", "").replace("　", "").strip().lower()
    )


def normalize_permit_number(s: str) -> str:
    """許可番号の重複判定用正規化: NFKC + 空白/ハイフン類統一"""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[‐−–—ー]", "-", s)
    s = re.sub(r"\s+", "", s)
    return s.strip().lower()


def authority_type(authority: str) -> str:
    if not authority:
        return ""
    return "国土交通大臣" if "国土交通大臣" in authority else "都道府県知事"


def calc_renewal_deadline(expiry_iso: str | None) -> str:
    if not expiry_iso:
        return ""
    try:
        d = date.fromisoformat(expiry_iso)
    except (ValueError, TypeError):
        return ""
    return (d - timedelta(days=RENEWAL_DAYS_BEFORE_EXPIRY)).isoformat()


def load_db_only_ids() -> list[str]:
    diff = json.loads(DIFF_JSON.read_text(encoding="utf-8"))
    return [r["company_id"] for r in diff["db_only_sync_to_gas"]]


def fetch_db_records(company_ids: list[str]) -> tuple[list[dict], list[dict]]:
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    placeholders = ",".join("?" * len(company_ids))
    comps = [dict(r) for r in conn.execute(
        f"SELECT * FROM companies WHERE company_id IN ({placeholders})", company_ids
    )]
    permits = [dict(r) for r in conn.execute(
        f"SELECT * FROM permits WHERE company_id IN ({placeholders}) AND current_flag=1", company_ids
    )]
    trades_map: dict[int, list[str]] = {}
    for r in conn.execute(
        f"SELECT pt.permit_id, pt.trade_name FROM permit_trades pt "
        f"JOIN permits p ON p.permit_id=pt.permit_id "
        f"WHERE p.company_id IN ({placeholders}) AND p.current_flag=1", company_ids
    ):
        trades_map.setdefault(r["permit_id"], []).append(r["trade_name"])
    for p in permits:
        p["_trades"] = ",".join(trades_map.get(p["permit_id"], []))
    conn.close()
    return comps, permits


def verify_headers(ws, expected: list[str], sheet_name: str) -> None:
    """実シート 1 行目と期待ヘッダーを照合。不一致で abort。"""
    actual = ws.row_values(1)
    if actual[:len(expected)] != expected:
        print(f"ERROR: {sheet_name} ヘッダー不一致", file=sys.stderr)
        print(f"  期待: {expected}", file=sys.stderr)
        print(f"  実際: {actual}", file=sys.stderr)
        print(f"  差分列: {[(i, e, a) for i, (e, a) in enumerate(zip(expected, actual)) if e != a]}",
              file=sys.stderr)
        sys.exit(2)
    if len(actual) < len(expected):
        print(f"ERROR: {sheet_name} 列数不足: actual={len(actual)} expected={len(expected)}",
              file=sys.stderr)
        sys.exit(2)
    print(f"  [OK] {sheet_name} ヘッダー検証: {len(expected)} 列一致")


def build_company_row(c: dict, now_str: str) -> list:
    d = {h: "" for h in COMPANIES_HEADERS}
    d["company_id"] = c["company_id"]
    d["company_name_raw"] = c["official_name"]
    d["company_name_normalized"] = normalize_name(c["official_name"])
    d["status"] = c.get("status") or "ACTIVE"
    d["created_at"] = c.get("created_at") or now_str
    d["updated_at"] = now_str
    return [d[h] for h in COMPANIES_HEADERS]


def build_permit_row(p: dict, company_name: str, now_str: str) -> list:
    expiry = p.get("expiry_date") or ""
    issue = p.get("issue_date") or ""
    is_partial = bool(expiry) != bool(issue)  # 片方のみ欠落

    d = {h: "" for h in PERMITS_HEADERS}
    d["permit_id"] = str(uuid.uuid4())  # GAS Utilities.getUuid と同形式
    d["company_id"] = p["company_id"]
    d["company_name_raw"] = company_name
    d["permit_authority_name"] = p.get("permit_authority") or ""
    d["permit_authority_name_normalized"] = (p.get("permit_authority") or "").replace(" ", "")
    d["permit_authority_type"] = authority_type(p.get("permit_authority") or "")
    d["permit_category"] = p.get("permit_category") or ""
    d["permit_year"] = p.get("permit_year") or ""
    d["permit_number_full"] = p.get("permit_number") or ""
    d["trade_categories"] = p.get("_trades") or ""
    d["issue_date"] = issue
    d["expiry_date"] = expiry
    d["renewal_deadline_date"] = calc_renewal_deadline(expiry)
    d["current_status"] = "要確認" if is_partial else "有効"
    d["source_file"] = p.get("source") or ""
    d["parse_status"] = "INCOMPLETE" if is_partial else "OK"
    if is_partial:
        missing = "issue_date" if not issue else "expiry_date"
        d["error_reason"] = f"DB同期時点で欠損: {missing}"
    d["note"] = f"db_permit_id={p['permit_id']}"
    d["created_at"] = p.get("created_at") or now_str
    d["updated_at"] = now_str
    return [d[h] for h in PERMITS_HEADERS]


def save_snapshot(ws_comp, ws_perm) -> Path:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    snap_path = SNAPSHOT_DIR / f"sheet_snapshot_{ts}.json"
    snapshot = {
        "taken_at": datetime.now().isoformat(),
        "Companies": ws_comp.get_all_values(),
        "Permits": ws_perm.get_all_values(),
    }
    snap_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    return snap_path


def save_skipped(skipped: list[dict]) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = PROJECT / "data" / f"skipped_incomplete_permits_{ts}.json"
    path.write_text(json.dumps(skipped, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="実書込（既定 dry-run）")
    ap.add_argument("--sheet-id")
    args = ap.parse_args()

    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    sheet_id = args.sheet_id or cfg["GOOGLE_SHEETS_ID"]
    sa_path = cfg["GOOGLE_SERVICE_ACCOUNT_FILE"]
    if not Path(sa_path).exists():
        print(f"ERROR: service account not found: {sa_path}", file=sys.stderr)
        sys.exit(1)

    ids = load_db_only_ids()
    print(f"DB-only 同期対象: {len(ids)} 社")
    comps, permits = fetch_db_records(ids)
    print(f"  companies: {len(comps)}, permits(current=1): {len(permits)}")

    # 両日 null の permit を除外して退避
    both_null = [p for p in permits if not p.get("issue_date") and not p.get("expiry_date")]
    permits = [p for p in permits if p.get("issue_date") or p.get("expiry_date")]
    if both_null:
        skip_path = save_skipped(both_null)
        print(f"  [EXCLUDE] issue/expiry 両日 null: {len(both_null)} 件 → {skip_path.name}")
        for p in both_null:
            print(f"    {p['company_id']} / {p.get('permit_number')}")

    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)

    ws_comp = sh.worksheet("Companies")
    ws_perm = sh.worksheet("Permits")

    print("\n--- シートヘッダー検証 ---")
    verify_headers(ws_comp, COMPANIES_HEADERS, "Companies")
    verify_headers(ws_perm, PERMITS_HEADERS, "Permits")

    comp_values = ws_comp.get_all_values()
    perm_values = ws_perm.get_all_values()
    existing_comp_ids = {row[0] for row in comp_values[1:] if row and row[0]}
    existing_perm_keys = set()
    for row in perm_values[1:]:
        if not row or len(row) <= 9:
            continue
        existing_perm_keys.add((row[1], normalize_permit_number(row[9])))

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    comp_rows_to_add = []
    for c in comps:
        if c["company_id"] in existing_comp_ids:
            print(f"  [SKIP Company] {c['company_id']} 既存")
            continue
        comp_rows_to_add.append(build_company_row(c, now_str))
        print(f"  [ADD Company] {c['company_id']} {c['official_name']}")

    name_map = {c["company_id"]: c["official_name"] for c in comps}
    perm_rows_to_add = []
    for p in permits:
        num_norm = normalize_permit_number(p.get("permit_number") or "")
        key = (p["company_id"], num_norm)
        if key in existing_perm_keys:
            print(f"  [SKIP Permit] {p['company_id']} / {p.get('permit_number')} (正規化一致で既存)")
            continue
        row = build_permit_row(p, name_map.get(p["company_id"], ""), now_str)
        perm_rows_to_add.append(row)
        status = row[PERMITS_HEADERS.index("parse_status")]
        print(f"  [ADD Permit] {p['company_id']} {p.get('permit_number')} "
              f"expiry={p.get('expiry_date')} [{status}]")

    print(f"\n追加予定: Companies {len(comp_rows_to_add)} / Permits {len(perm_rows_to_add)}")
    incomplete = [r for r in perm_rows_to_add
                  if r[PERMITS_HEADERS.index("parse_status")] == "INCOMPLETE"]
    if incomplete:
        print(f"  うち INCOMPLETE（片方のみ欠落）: {len(incomplete)} 件")
    if both_null:
        print(f"  除外（両日 null）: {len(both_null)} 件")

    if not args.execute:
        print("\n(dry-run) 実書込しません。--execute で実行してください。")
        return

    print("\n--- 実行直前スナップショット取得 ---")
    snap = save_snapshot(ws_comp, ws_perm)
    print(f"  保存: {snap}")

    if comp_rows_to_add:
        ws_comp.append_rows(comp_rows_to_add, value_input_option="RAW")
        print(f"\n✓ Companies に {len(comp_rows_to_add)} 行 append (RAW)")
    if perm_rows_to_add:
        ws_perm.append_rows(perm_rows_to_add, value_input_option="RAW")
        print(f"✓ Permits に {len(perm_rows_to_add)} 行 append (RAW)")

    print(f"\n同期完了。ロールバック時は snapshot {snap.name} を参照してください。")


if __name__ == "__main__":
    main()
