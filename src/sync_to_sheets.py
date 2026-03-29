"""
sync_to_sheets.py — MLIT取得データをGoogle Sheetsに同期する。

mlit_all_permits.csv を読み込み、GASスプレッドシートの
Permits / Companies シートを更新する。

Usage:
    python src/sync_to_sheets.py                    # 本番同期
    python src/sync_to_sheets.py --dry-run          # 読み取りのみ
    python src/sync_to_sheets.py --sheet-id XXXXX   # シートID指定

Prerequisites:
    pip install gspread google-auth
    Service account JSON at config.json GOOGLE_SERVICE_ACCOUNT_FILE
    Share the spreadsheet with the service account email
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
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
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config.json"
CSV_ALL = Path("C:/tmp/mlit_all_permits.csv")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

PERMITS_HEADERS = [
    "company_id", "company_name", "permit_number", "authority",
    "category", "expiry_date", "expiry_wareki", "days_remaining",
    "trades_ippan", "trades_tokutei", "trades_count",
    "fetch_status", "last_synced",
]

SYNC_LOG_HEADERS = [
    "sync_at", "records_synced", "records_ok", "records_failed", "source_csv",
]


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def get_gspread_client(sa_path: str) -> gspread.Client:
    """Authenticate with service account and return gspread client."""
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    return gspread.authorize(creds)


# ---------------------------------------------------------------------------
# CSV Load
# ---------------------------------------------------------------------------
def load_csv(csv_path: Path) -> list[dict]:
    """Load MLIT CSV results."""
    if not csv_path.exists():
        print(f"ERROR: CSV not found: {csv_path}")
        sys.exit(1)

    records = []
    with csv_path.open("r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            records.append(row)
    return records


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
def sync_permits(
    client: gspread.Client,
    sheet_id: str,
    records: list[dict],
    dry_run: bool = False,
) -> dict:
    """Sync MLIT data to Permits sheet (full replace)."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Prepare rows
    data_rows = []
    ok_count = 0
    fail_count = 0

    for r in records:
        status = r.get("fetch_status", "")
        if status == "OK":
            ok_count += 1
        else:
            fail_count += 1

        data_rows.append([
            r.get("company_id", ""),
            r.get("company_name", ""),
            r.get("permit_number", ""),
            r.get("authority", ""),
            r.get("category", ""),
            r.get("expiry_date", ""),
            r.get("expiry_wareki", ""),
            r.get("days_remaining", ""),
            r.get("trades_ippan", ""),
            r.get("trades_tokutei", ""),
            r.get("trades_count", ""),
            r.get("fetch_status", ""),
            now_str,
        ])

    print(f"\n  Records: {len(data_rows)} (OK={ok_count}, FAIL={fail_count})")

    if dry_run:
        print("  [DRY-RUN] Would write to Permits sheet")
        return {"synced": len(data_rows), "ok": ok_count, "failed": fail_count}

    # Open spreadsheet
    sh = client.open_by_key(sheet_id)

    # Get or create Permits sheet
    try:
        ws = sh.worksheet("MLITPermits")
        print(f"  Found existing MLITPermits sheet")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="MLITPermits", rows=200, cols=len(PERMITS_HEADERS))
        print(f"  Created new MLITPermits sheet")

    # Full replace: write first, then clear old rows (safer than clear→write)
    all_values = [PERMITS_HEADERS] + data_rows
    try:
        ws.update(range_name="A1", values=all_values)
        # Clear any leftover rows beyond new data
        total_rows = ws.row_count
        new_rows = len(all_values)
        if total_rows > new_rows:
            ws.batch_clear([f"A{new_rows + 1}:Z{total_rows}"])
        print(f"  Written {len(data_rows)} rows to MLITPermits")
    except Exception as e:
        print(f"  [ERROR] Write failed: {e}")
        print(f"  [SAFE] Existing data preserved (overwrite failed, not cleared)")
        raise

    # Read back for verification
    actual = ws.get_all_values()
    print(f"  Verified: {len(actual) - 1} data rows in sheet")

    # Append to SyncRuns log
    try:
        log_ws = sh.worksheet("SyncRuns")
    except gspread.WorksheetNotFound:
        log_ws = sh.add_worksheet(title="SyncRuns", rows=100, cols=len(SYNC_LOG_HEADERS))
        log_ws.update(range_name="A1", values=[SYNC_LOG_HEADERS])

    log_ws.append_row([now_str, len(data_rows), ok_count, fail_count, str(CSV_ALL)])
    print(f"  SyncRuns log appended")

    return {"synced": len(data_rows), "ok": ok_count, "failed": fail_count}


def update_config_value(
    client: gspread.Client,
    sheet_id: str,
    key: str,
    value: str,
    dry_run: bool = False,
) -> bool:
    """Update a value in the Config sheet by key."""
    sh = client.open_by_key(sheet_id)

    try:
        ws = sh.worksheet("Config")
    except gspread.WorksheetNotFound:
        print(f"  [ERROR] Config sheet not found")
        return False

    # Find key in column A
    all_values = ws.get_all_values()
    for row_idx, row in enumerate(all_values):
        if row and row[0] == key:
            if dry_run:
                old_val = row[1] if len(row) > 1 else ""
                print(f"  [DRY-RUN] Would update Config[{key}]: '{old_val}' -> '{value}'")
                return True

            ws.update_cell(row_idx + 1, 2, value)
            print(f"  Config[{key}] updated to '{value}'")
            return True

    print(f"  [WARN] Key '{key}' not found in Config sheet")
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="MLIT → Sheets 同期")
    parser.add_argument("--dry-run", action="store_true", help="読み取りのみ")
    parser.add_argument("--sheet-id", help="Google Sheets ID (overrides config.json)")
    parser.add_argument("--csv", default=str(CSV_ALL), help="CSV path")
    parser.add_argument("--update-config", action="store_true",
                        help="Config sheet の NOTIFY_STAGES_DAYS を 150 に更新")
    args = parser.parse_args()

    # Load config
    with CONFIG_PATH.open(encoding="utf-8") as f:
        cfg = json.load(f)

    sheet_id = args.sheet_id or cfg.get("GOOGLE_SHEETS_ID", "")
    sa_path = cfg.get("GOOGLE_SERVICE_ACCOUNT_FILE", "")

    if not sheet_id:
        print("ERROR: GOOGLE_SHEETS_ID is not set. Use --sheet-id or set in config.json")
        sys.exit(1)

    if not Path(sa_path).exists():
        print(f"ERROR: Service account file not found: {sa_path}")
        print("  Create a service account and share the spreadsheet with its email.")
        sys.exit(1)

    print("=" * 60)
    print("  MLIT → Google Sheets Sync")
    print(f"  Sheet ID: {sheet_id[:20]}...")
    print(f"  Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")
    print("=" * 60)

    # Auth
    client = get_gspread_client(sa_path)
    print("  Authenticated with service account")

    # Load CSV
    csv_path = Path(args.csv)
    records = load_csv(csv_path)
    print(f"  Loaded {len(records)} records from {csv_path.name}")

    # Sync permits
    result = sync_permits(client, sheet_id, records, dry_run=args.dry_run)

    # Optionally update Config
    if args.update_config:
        print(f"\n  Updating Config sheet...")
        update_config_value(client, sheet_id, "NOTIFY_STAGES_DAYS", "150", dry_run=args.dry_run)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  Sync complete: {result['synced']} records ({result['ok']} OK, {result['failed']} failed)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
