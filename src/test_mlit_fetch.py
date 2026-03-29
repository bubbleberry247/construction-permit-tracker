"""
test_mlit_fetch.py — MLITから許可業種+有効期限を取得するテスト

Usage:
    # 愛知県知事許可の1社テスト
    python src/test_mlit_fetch.py --auth 愛知県知事 --num 69390

    # 大臣許可
    python src/test_mlit_fetch.py --auth 国土交通大臣 --num 14569

    # DB上の先頭N社を一括テスト（デフォルト3社）
    python src/test_mlit_fetch.py --from-db --limit 3

    # dry-run（APIアクセスなし、パラメータ確認のみ）
    python src/test_mlit_fetch.py --auth 愛知県知事 --num 69390 --dry-run
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# Reuse existing functions from verify_permits_mlit
from verify_permits_mlit import (
    DB_PATH,
    MLIT_TRADE_ABBREV,
    REQUEST_INTERVAL_SEC,
    ApiResult,
    build_sv_license_no,
    fetch_detail,
    get_license_no_kbn,
    get_pref_code,
    search_permit,
)

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip install requests beautifulsoup4")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

# etsuran2 has no published rate limit, but aggressive scraping risks IP block.
# Conservative: 2 seconds between requests (search + detail = 2 requests per company).
SAFE_INTERVAL_SEC = 2.0
MAX_BATCH_SIZE = 20  # Safety cap for --from-db


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def display_result(
    label: str,
    api: ApiResult,
    elapsed: float,
) -> None:
    """Fetch result to console."""
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")

    if not api.found:
        print(f"  Status: NOT FOUND")
        if api.error:
            print(f"  Error:  {api.error}")
        return

    print(f"  Status:   FOUND")
    print(f"  Company:  {api.api_name}")
    print(f"  Category: {api.api_category} (般=一般, 特=特定)")
    print(f"  Expiry:   {api.api_expiry_iso} ({api.api_expiry_wareki})")
    print(f"  Trades ({len(api.api_trades)}):")
    for i, trade in enumerate(api.api_trades, 1):
        print(f"    {i:2d}. {trade}")
    print(f"  Elapsed:  {elapsed:.1f}s")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Single fetch
# ---------------------------------------------------------------------------

def fetch_one(
    session: requests.Session,
    authority: str,
    permit_number: str,
    dry_run: bool = False,
) -> ApiResult:
    """Fetch permit data for one company from MLIT."""

    license_no_kbn = get_license_no_kbn(authority)
    pref_code = get_pref_code(authority)

    print(f"\n[Params] authority={authority} kbn={license_no_kbn} "
          f"pref={pref_code} num={permit_number}")

    if dry_run:
        print("[DRY-RUN] Skipping API access")
        return ApiResult()

    # Step 1: Search
    print("[1/2] Searching ...", end=" ", flush=True)
    sv_license_no = search_permit(session, license_no_kbn, permit_number, pref_code)

    if not sv_license_no:
        print("NOT FOUND")
        return ApiResult(error="Search returned no results")

    print(f"found sv_licenseNo={sv_license_no}")

    time.sleep(SAFE_INTERVAL_SEC)

    # Step 2: Detail
    print("[2/2] Fetching detail ...", end=" ", flush=True)
    detail = fetch_detail(session, sv_license_no)
    print("OK")

    return detail


# ---------------------------------------------------------------------------
# Batch from DB
# ---------------------------------------------------------------------------

def fetch_from_db(
    session: requests.Session,
    limit: int,
    dry_run: bool = False,
) -> None:
    """Fetch permit data for companies from DB."""

    if limit > MAX_BATCH_SIZE:
        print(f"[WARN] Limiting to {MAX_BATCH_SIZE} (requested {limit})")
        limit = MAX_BATCH_SIZE

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT p.permit_number, p.permit_authority, c.official_name
        FROM permits p
        JOIN companies c ON p.company_id = c.company_id
        WHERE p.permit_number IS NOT NULL
          AND p.permit_number != ''
          AND p.permit_authority IS NOT NULL
          AND p.permit_authority != ''
        ORDER BY p.permit_id
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()

    print(f"\n[DB] {len(rows)} companies to test (limit={limit})")

    success = 0
    fail = 0

    for i, row in enumerate(rows):
        name = row["official_name"]
        num = row["permit_number"]
        auth = row["permit_authority"]

        label = f"[{i+1}/{len(rows)}] {name} ({auth} {num})"
        print(f"\n{'-'*60}")
        print(f"  {label}")

        t0 = time.time()
        api = fetch_one(session, auth, num, dry_run=dry_run)
        elapsed = time.time() - t0

        if api.found:
            display_result(name, api, elapsed)
            success += 1
        else:
            print(f"  -> NOT FOUND / ERROR: {api.error}")
            fail += 1

        # Rate limit between companies
        if i < len(rows) - 1 and not dry_run:
            time.sleep(SAFE_INTERVAL_SEC)

    # Summary
    print(f"\n{'='*60}")
    print(f"  Summary: {success} found, {fail} not found, {len(rows)} total")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="MLIT etsuran2 から許可業種+有効期限を取得するテスト"
    )
    parser.add_argument("--auth", help="許可行政庁 (例: 愛知県知事, 国土交通大臣)")
    parser.add_argument("--num", help="許可番号 (例: 69390)")
    parser.add_argument("--from-db", action="store_true", help="DBの会社を一括テスト")
    parser.add_argument("--limit", type=int, default=3, help="--from-db時の件数上限 (default: 3, max: 20)")
    parser.add_argument("--dry-run", action="store_true", help="APIアクセスなし")

    args = parser.parse_args()

    if not args.from_db and (not args.auth or not args.num):
        parser.error("--auth と --num を指定するか、--from-db を使ってください")

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
    })

    if args.from_db:
        fetch_from_db(session, args.limit, dry_run=args.dry_run)
    else:
        t0 = time.time()
        api = fetch_one(session, args.auth, args.num, dry_run=args.dry_run)
        elapsed = time.time() - t0
        display_result(f"{args.auth} 第{args.num}号", api, elapsed)


if __name__ == "__main__":
    main()
