"""
sync_ocr_overrides.py — permit_ocr_overrides.csv の手動修正データを
Google Sheets MLITPermits シートに反映する。

Usage:
    python src/sync_ocr_overrides.py              # 本番実行
    python src/sync_ocr_overrides.py --dry-run    # 確認のみ

Logic:
    1. permit_ocr_overrides.csv を (file_id, page_no) でグループ化してレコード再構築
    2. MLITPermits から全行読み込み
    3. company_name で既存行を検索（完全一致優先、次いで部分一致）
    4. 見つかれば UPDATE、なければ APPEND
    5. fetch_status = 'MANUAL_ENTRY' を設定
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print("ERROR: Install dependencies: pip install gspread google-auth")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config.json"
OVERRIDES_CSV = PROJECT_ROOT / "output" / "permit_ocr_overrides.csv"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEET_HEADERS = [
    "company_id", "company_name", "permit_number", "authority",
    "category", "expiry_date", "expiry_wareki", "days_remaining",
    "trades_ippan", "trades_tokutei", "trades_count",
    "fetch_status", "last_synced",
]


# ---------------------------------------------------------------------------
# Parse overrides CSV → list of company records
# ---------------------------------------------------------------------------
def load_overrides(csv_path: Path) -> list[dict]:
    """
    (file_id, page_no) でグループ化し、field_name→value を再構築する。
    Returns list of dicts with keys matching the business fields.
    """
    grouped: dict[tuple, dict] = defaultdict(dict)

    with csv_path.open("r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            key = (row["file_id"], row["page_no"])
            grouped[key][row["field_name"]] = row["value"]

    records = []
    for (file_id, page_no), fields in grouped.items():
        records.append({
            "file_id": file_id,
            "page_no": page_no,
            **fields,
        })
    return records


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------
def parse_permit_number_full(raw: str) -> tuple[str, str, str]:
    """
    Examples:
      '愛知県知事　許可　（特-7）第36304号' → ('36304', '愛知県知事', '特定')
      '愛知県知事　許可（般-3）第50825号'  → ('50825', '愛知県知事', '一般')
    Returns (permit_number, authority, category)
    """
    # Authority: 最初の「〜知事」または「〜大臣」部分
    auth_match = re.search(r'(\S+?知事|\S+?大臣)', raw)
    authority = auth_match.group(1) if auth_match else ""

    # Category: （特-N）or （般-N）
    cat_match = re.search(r'[（(](特|般)-\d+[）)]', raw)
    if cat_match:
        cat_char = cat_match.group(1)
        category = "特定" if cat_char == "特" else "一般"
    else:
        category = ""

    # Permit number: 第NNNNN号
    num_match = re.search(r'第(\d+)号', raw)
    permit_number = num_match.group(1) if num_match else ""

    return permit_number, authority, category


def normalize_date(date_str: str, permit_year: str = "") -> str:
    """
    Various date formats → 'YYYY-MM-DD'.
    - '2030/11/24'  → '2030-11-24'
    - '2026/6/12'   → '2026-06-12'
    - '11月25日' + permit_year='2025年' → '2025-11-25'
    - '12月15日' + permit_year='2021年' → issue_date only (not used for expiry)
    - '6/13'        → use permit_year
    """
    s = date_str.strip()

    # YYYY/M/D or YYYY/MM/DD
    m = re.match(r'^(\d{4})/(\d{1,2})/(\d{1,2})$', s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # YYYY-MM-DD already
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', s)
    if m:
        return s

    # N月M日 (with permit_year like '2025年')
    m = re.match(r'^(\d{1,2})月(\d{1,2})日$', s)
    if m:
        year_m = re.search(r'(\d{4})', permit_year)
        year = year_m.group(1) if year_m else str(date.today().year)
        return f"{year}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"

    # N/M (like '6/13' — month/day only)
    m = re.match(r'^(\d{1,2})/(\d{1,2})$', s)
    if m:
        year_m = re.search(r'(\d{4})', permit_year)
        year = year_m.group(1) if year_m else str(date.today().year)
        return f"{year}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"

    return s  # fallback: return as-is


def calc_days_remaining(expiry_date_str: str) -> str:
    """Calculate days from today to expiry date."""
    try:
        expiry = datetime.strptime(expiry_date_str, "%Y-%m-%d").date()
        return str((expiry - date.today()).days)
    except ValueError:
        return ""


def normalize_category_short(raw: str) -> str:
    """'特' → '特定', '般' → '一般'. Pass-through if already normalized."""
    if raw in ("特", "特定"):
        return "特定"
    if raw in ("般", "一般"):
        return "一般"
    return raw


def build_record(fields: dict) -> dict:
    """
    overrides CSVのフィールド群 → MLITPermitsシートの列構造 に変換する。
    """
    permit_number_full = fields.get("permit_number_full", "")
    permit_year = fields.get("permit_year", "")

    permit_number, authority_from_full, category_from_full = parse_permit_number_full(
        permit_number_full
    )

    # authority: CSVの permit_authority_name が「愛知県」等略称の場合、
    # permit_number_full からの「愛知県知事」を優先
    authority = authority_from_full or fields.get("permit_authority_name", "")

    # category: permit_number_full パース結果優先
    category = category_from_full or normalize_category_short(
        fields.get("permit_category", "")
    )

    expiry_raw = fields.get("expiry_date", "")
    expiry_date = normalize_date(expiry_raw, permit_year)
    days_remaining = calc_days_remaining(expiry_date)

    # trades
    trade_categories_raw = fields.get("trade_categories", "")
    trades = [t.strip() for t in trade_categories_raw.split("|") if t.strip()]
    trades_count = str(len(trades))
    trades_str = "|".join(trades)

    # category に応じて trades_ippan / trades_tokutei を振り分け
    if category == "特定":
        trades_ippan = ""
        trades_tokutei = trades_str
    else:
        trades_ippan = trades_str
        trades_tokutei = ""

    return {
        "company_name": fields.get("company_name_raw", ""),
        "permit_number": permit_number,
        "authority": authority,
        "category": category,
        "expiry_date": expiry_date,
        "expiry_wareki": "",   # OCR手動データにはない（空で上書き）
        "days_remaining": days_remaining,
        "trades_ippan": trades_ippan,
        "trades_tokutei": trades_tokutei,
        "trades_count": trades_count,
        "fetch_status": "MANUAL_ENTRY",
        "last_synced": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# Fuzzy name match
# ---------------------------------------------------------------------------
def find_row_index(sheet_rows: list[list], company_name: str) -> int | None:
    """
    sheet_rows: get_all_values() の全行（0行目=ヘッダー）
    Returns 1-based row index of matching row, or None.
    Exact match first, then substring match.
    """
    # Strip whitespace variants (全角スペース対応)
    normalized = company_name.replace("\u3000", " ").strip()

    exact_idx = None
    partial_idx = None

    for i, row in enumerate(sheet_rows[1:], start=2):  # start=2 (1-based, skip header)
        cell_name = row[1].replace("\u3000", " ").strip() if len(row) > 1 else ""
        if cell_name == normalized:
            exact_idx = i
            break
        if normalized in cell_name or cell_name in normalized:
            if partial_idx is None:
                partial_idx = i

    return exact_idx or partial_idx


# ---------------------------------------------------------------------------
# Main sync
# ---------------------------------------------------------------------------
def sync_overrides(
    client: gspread.Client,
    sheet_id: str,
    override_records: list[dict],
    dry_run: bool = False,
) -> None:
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet("MLITPermits")
    all_rows = ws.get_all_values()

    header_row = all_rows[0] if all_rows else SHEET_HEADERS
    col_idx = {h: i for i, h in enumerate(header_row)}

    updated = 0
    appended = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for raw_fields in override_records:
        rec = build_record(raw_fields)
        company_name = rec["company_name"]

        row_idx = find_row_index(all_rows, company_name)

        if row_idx is not None:
            # UPDATE existing row
            existing_row = all_rows[row_idx - 1]  # convert to 0-based
            company_id = existing_row[0] if existing_row else ""

            new_row = list(existing_row)  # copy
            # Pad if shorter than headers
            while len(new_row) < len(header_row):
                new_row.append("")

            for field, value in rec.items():
                if field in col_idx:
                    new_row[col_idx[field]] = value

            # Ensure company_id preserved
            new_row[col_idx["company_id"]] = company_id

            print(f"  UPDATE row {row_idx}: {company_name} (ID={company_id})")
            print(f"    permit_number={rec['permit_number']}, category={rec['category']}, "
                  f"expiry={rec['expiry_date']}, days={rec['days_remaining']}")
            print(f"    trades_ippan='{rec['trades_ippan']}', trades_tokutei='{rec['trades_tokutei']}', "
                  f"count={rec['trades_count']}")

            if not dry_run:
                # Update the full row in one call
                col_end = chr(ord('A') + len(header_row) - 1)
                range_name = f"A{row_idx}:{col_end}{row_idx}"
                ws.update(range_name=range_name, values=[new_row])
                # Refresh in-memory copy to keep subsequent fuzzy matches consistent
                all_rows[row_idx - 1] = new_row

            updated += 1

        else:
            # APPEND new row
            # Determine new company_id
            existing_ids = [r[0] for r in all_rows[1:] if r and r[0].startswith("C")]
            max_id = max((int(i[1:]) for i in existing_ids if i[1:].isdigit()), default=0)
            new_id = f"C{max_id + 1:04d}"

            new_row = [""] * len(header_row)
            new_row[col_idx["company_id"]] = new_id
            for field, value in rec.items():
                if field in col_idx:
                    new_row[col_idx[field]] = value

            print(f"  APPEND: {company_name} (new ID={new_id})")
            print(f"    permit_number={rec['permit_number']}, category={rec['category']}, "
                  f"expiry={rec['expiry_date']}, days={rec['days_remaining']}")

            if not dry_run:
                ws.append_row(new_row)
                all_rows.append(new_row)

            appended += 1

    print(f"\n  Result: {updated} updated, {appended} appended"
          + (" [DRY-RUN — no writes]" if dry_run else ""))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="OCR overrides → MLITPermits 同期")
    parser.add_argument("--dry-run", action="store_true", help="読み取りのみ（書き込みなし）")
    parser.add_argument("--sheet-id", help="Google Sheets ID (configを上書き)")
    args = parser.parse_args()

    with CONFIG_PATH.open(encoding="utf-8") as f:
        cfg = json.load(f)

    sheet_id = args.sheet_id or cfg.get("GOOGLE_SHEETS_ID", "")
    sa_path = cfg.get("GOOGLE_SERVICE_ACCOUNT_FILE", "")

    if not sheet_id:
        print("ERROR: GOOGLE_SHEETS_ID が未設定")
        sys.exit(1)
    if not Path(sa_path).exists():
        print(f"ERROR: Service account file not found: {sa_path}")
        sys.exit(1)
    if not OVERRIDES_CSV.exists():
        print(f"ERROR: overrides CSV not found: {OVERRIDES_CSV}")
        sys.exit(1)

    print("=" * 60)
    print("  OCR Overrides → Google Sheets Sync")
    print(f"  CSV : {OVERRIDES_CSV}")
    print(f"  Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)

    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    print("  Authenticated OK")

    records = load_overrides(OVERRIDES_CSV)
    print(f"  Loaded {len(records)} company records from CSV")

    sync_overrides(client, sheet_id, records, dry_run=args.dry_run)
    print("=" * 60)


if __name__ == "__main__":
    main()
