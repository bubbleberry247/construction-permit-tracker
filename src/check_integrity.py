"""
check_integrity.py — DB整合性チェッカー

fetch_gmail.py 実行後に毎回実行し、問題があれば即停止+報告。

Usage:
    python src/check_integrity.py           # チェック実行
    python src/check_integrity.py --fix     # 自動修正可能な問題を修正

Exit codes:
    0 = OK (no issues)
    1 = WARN (non-critical issues found)
    2 = ERROR (critical issues found)
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from datetime import date
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

DB_PATH = Path(__file__).parent.parent / "data" / "permit_tracker.db"
INBOUND_LOG = Path(__file__).parent.parent / "logs" / "inbound_log.csv"

# Permit number range by authority (observed maximums)
PERMIT_NUMBER_RANGES: dict[str, int] = {
    "愛知県知事": 120000,
    "岐阜県知事": 50000,
    "山形県知事": 110000,
    "国土交通大臣": 30000,
}


def check_null_company_files(conn: sqlite3.Connection) -> list[dict]:
    """Check for files with NULL company_id (excluding non-business files)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT f.file_id, f.file_name, f.new_filename
        FROM files f
        WHERE f.company_id IS NULL
          AND f.file_name NOT LIKE '%Invoice%'
          AND f.file_name NOT LIKE '%Receipt%'
          AND f.file_name NOT LIKE '%bitlocker%'
          AND f.file_name NOT LIKE '%申請用%'
          AND f.new_filename IS NOT NULL
    """)
    return [{"file_id": r[0], "file_name": r[1], "new_filename": r[2]} for r in cur.fetchall()]


def check_orphan_messages(conn: sqlite3.Connection) -> list[dict]:
    """Check for inbound_messages with no files attached."""
    cur = conn.cursor()
    cur.execute("""
        SELECT im.message_id, im.company_id, im.received_at, im.sender_email
        FROM inbound_messages im
        WHERE im.company_id IS NOT NULL
          AND im.message_id NOT LIKE 'manual%'
          AND NOT EXISTS (SELECT 1 FROM files f WHERE f.message_id = im.message_id)
    """)
    return [{"message_id": r[0], "company_id": r[1], "received_at": r[2], "sender": r[3]} for r in cur.fetchall()]


def check_company_mismatch(conn: sqlite3.Connection) -> list[dict]:
    """Check for files where filename prefix doesn't match assigned company."""
    cur = conn.cursor()
    cur.execute("""
        SELECT f.file_id, f.new_filename, f.company_id, c.official_name
        FROM files f
        JOIN companies c ON f.company_id = c.company_id
        WHERE f.new_filename IS NOT NULL AND f.new_filename LIKE '%_%'
    """)
    issues = []
    for r in cur.fetchall():
        fname = r[0]
        new_filename = r[1] or ""
        company_id = r[2]
        official_name = r[3]

        if "_" not in new_filename:
            continue

        prefix = new_filename.split("_")[0]
        # Normalize for comparison
        norm_prefix = prefix.replace("株式会社", "").replace("有限会社", "").replace("㈱", "").strip()
        norm_official = official_name.replace("株式会社", "").replace("有限会社", "").replace("㈱", "").strip()

        if norm_prefix and norm_official and norm_prefix not in norm_official and norm_official not in norm_prefix:
            issues.append({
                "file_id": fname,
                "filename_company": prefix,
                "db_company": f"{company_id} {official_name}",
                "filename": new_filename,
            })
    return issues


def check_permit_number_range(conn: sqlite3.Connection) -> list[dict]:
    """Check for permit numbers outside expected range."""
    cur = conn.cursor()
    cur.execute("""
        SELECT p.permit_id, p.company_id, c.official_name, p.permit_number, p.permit_authority
        FROM permits p
        JOIN companies c ON p.company_id = c.company_id
        WHERE p.permit_number IS NOT NULL AND p.permit_number != ''
    """)
    issues = []
    for r in cur.fetchall():
        try:
            num = int(r[3])
        except (ValueError, TypeError):
            issues.append({
                "permit_id": r[0], "company": f"{r[1]} {r[2]}",
                "permit_number": r[3], "authority": r[4],
                "issue": "Non-numeric permit number",
            })
            continue

        max_range = PERMIT_NUMBER_RANGES.get(r[4], 200000)
        if num > max_range:
            issues.append({
                "permit_id": r[0], "company": f"{r[1]} {r[2]}",
                "permit_number": r[3], "authority": r[4],
                "issue": f"Permit number {num} > expected max {max_range}",
            })
    return issues


def check_expiry_dates(conn: sqlite3.Connection) -> list[dict]:
    """Check for permits with missing or invalid expiry dates."""
    cur = conn.cursor()
    cur.execute("""
        SELECT p.permit_id, p.company_id, c.official_name, p.permit_number,
               p.issue_date, p.expiry_date
        FROM permits p
        JOIN companies c ON p.company_id = c.company_id
        WHERE p.current_flag = 1
    """)
    issues = []
    for r in cur.fetchall():
        expiry = r[5]
        issue_date = r[4]

        if not expiry or expiry == "UNCERTAIN":
            issues.append({
                "permit_id": r[0], "company": f"{r[1]} {r[2]}",
                "issue": f"Missing expiry_date (current={expiry})",
            })
            continue

        try:
            exp_d = date.fromisoformat(expiry[:10])
            if issue_date and issue_date != "UNCERTAIN":
                iss_d = date.fromisoformat(issue_date[:10])
                if iss_d > exp_d:
                    issues.append({
                        "permit_id": r[0], "company": f"{r[1]} {r[2]}",
                        "issue": f"issue_date ({issue_date}) > expiry_date ({expiry})",
                    })
        except (ValueError, TypeError):
            issues.append({
                "permit_id": r[0], "company": f"{r[1]} {r[2]}",
                "issue": f"Invalid date format: expiry={expiry}",
            })
    return issues


def check_duplicate_permits(conn: sqlite3.Connection) -> list[dict]:
    """Check for duplicate active permits (same company + permit_number)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT p.company_id, c.official_name, p.permit_number, p.permit_authority,
               COUNT(*) as cnt
        FROM permits p
        JOIN companies c ON p.company_id = c.company_id
        WHERE p.current_flag = 1
          AND p.permit_number IS NOT NULL AND p.permit_number != ''
        GROUP BY p.company_id, p.permit_number, p.permit_authority, p.permit_category
        HAVING COUNT(*) > 1
    """)
    return [{
        "company": f"{r[0]} {r[1]}",
        "permit_number": r[2],
        "authority": r[3],
        "count": r[4],
    } for r in cur.fetchall()]


def check_inbound_log_vs_db(conn: sqlite3.Connection) -> list[dict]:
    """Check for inbound_log records not in files DB (business files only)."""
    if not INBOUND_LOG.exists():
        return []

    cur = conn.cursor()
    cur.execute("SELECT file_hash FROM files")
    db_hashes = {r[0] for r in cur.fetchall()}

    skip_senders = {"anthropic", "stripe", "google.com", "hotmail", "kalimistk", "nano banana"}

    issues = []
    with INBOUND_LOG.open("r", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            fhash = r.get("file_hash", "")
            sender = (r.get("sender_email", "") + r.get("original_sender_email", "")).lower()
            if any(s in sender for s in skip_senders):
                continue
            if fhash and fhash not in db_hashes:
                issues.append({
                    "file_name": r.get("file_name", ""),
                    "sender": r.get("original_sender_email", ""),
                    "received_at": r.get("received_at", ""),
                })
    return issues


def main() -> None:
    parser = argparse.ArgumentParser(description="DB整合性チェック")
    parser.add_argument("--fix", action="store_true", help="自動修正可能な問題を修正")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    print("=" * 60)
    print("  DB Integrity Check")
    print("=" * 60)

    all_issues: dict[str, list] = {}
    exit_code = 0

    # Run checks
    checks = [
        ("NULL company_id files", check_null_company_files, "WARN"),
        ("Orphan messages (no files)", check_orphan_messages, "WARN"),
        ("Company name mismatch", check_company_mismatch, "ERROR"),
        ("Permit number out of range", check_permit_number_range, "ERROR"),
        ("Invalid expiry dates", check_expiry_dates, "WARN"),
        ("Duplicate active permits", check_duplicate_permits, "ERROR"),
        ("Inbound log vs DB gaps", check_inbound_log_vs_db, "WARN"),
    ]

    for name, check_fn, severity in checks:
        issues = check_fn(conn)
        all_issues[name] = issues

        if issues:
            level = 2 if severity == "ERROR" else 1
            exit_code = max(exit_code, level)
            icon = "❌" if severity == "ERROR" else "⚠"
            print(f"\n  {icon} {name}: {len(issues)} issues")
            for issue in issues[:5]:
                print(f"    {issue}")
            if len(issues) > 5:
                print(f"    ... +{len(issues) - 5} more")
        else:
            print(f"\n  ✓ {name}: OK")

    # Summary
    total_issues = sum(len(v) for v in all_issues.values())
    print(f"\n{'=' * 60}")
    print(f"  Result: {'OK' if exit_code == 0 else 'WARN' if exit_code == 1 else 'ERROR'}")
    print(f"  Total issues: {total_issues}")
    print(f"{'=' * 60}")

    conn.close()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
