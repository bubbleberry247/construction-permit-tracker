"""
個別会社の MLIT 許可情報を最新化する CLI。

Usage:
    python scripts/mlit_refresh_one.py --company-id C0008
    python scripts/mlit_refresh_one.py --company-id C0008 --json  # JSON 出力

API用: FastAPI からサブプロセスとして呼び出し、結果を JSON で受け取る想定。
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT / "src"))

from mlit_confirm import (  # noqa: E402
    confirm_permit_with_playwright,
    get_screenshot_path,
    append_confirmation_log,
    load_config,
    PROJECT_ROOT as MLIT_PROJECT_ROOT,
)


def fetch_permit_for_company(db_path: Path, company_id: str) -> dict | None:
    """DB から指定会社の current_flag=1 permit を取得"""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT p.permit_id, p.permit_number, p.permit_authority, "
            "p.permit_category, p.issue_date, p.expiry_date, "
            "c.official_name, c.permit_number AS company_permit_num, "
            "c.permit_authority AS company_permit_auth "
            "FROM permits p JOIN companies c ON c.company_id = p.company_id "
            "WHERE p.company_id = ? AND p.current_flag = 1 "
            "ORDER BY p.expiry_date DESC LIMIT 1",
            (company_id,),
        ).fetchone()
        if not row:
            # permits が無くても companies に許可情報があれば使う
            comp = conn.execute(
                "SELECT company_id, official_name, permit_number, permit_authority "
                "FROM companies WHERE company_id=?",
                (company_id,),
            ).fetchone()
            if not comp or not comp["permit_number"]:
                return None
            return {
                "permit_id": "",
                "company_id": comp["company_id"],
                "official_name": comp["official_name"],
                "contractor_number": comp["permit_number"] or "",
                "permit_authority_name_normalized": comp["permit_authority"] or "",
            }
        return {
            "permit_id": str(row["permit_id"]),
            "company_id": company_id,
            "official_name": row["official_name"],
            "contractor_number": (row["permit_number"] or row["company_permit_num"] or "").strip(),
            "permit_authority_name_normalized": (row["permit_authority"] or row["company_permit_auth"] or "").strip(),
        }
    finally:
        conn.close()


def update_db_after_confirm(db_path: Path, company_id: str, result: str, screenshot: Path) -> None:
    """確認結果を companies.mlit_status / last_confirmed_at に反映"""
    status_map = {
        "一致": "CONFIRMED",
        "不一致": "MISMATCH",
        "確認不可": "ERROR",
    }
    new_status = status_map.get(result, "ERROR")
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "UPDATE companies SET mlit_status=?, last_confirmed_at=datetime('now','localtime'), "
            "updated_at=datetime('now','localtime') WHERE company_id=?",
            (new_status, company_id),
        )
        conn.commit()
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--company-id", required=True, help="例: C0008")
    ap.add_argument("--json", action="store_true", help="JSON で結果出力")
    args = ap.parse_args()

    config = load_config()
    data_root = Path(config.get("DATA_ROOT", str(MLIT_PROJECT_ROOT)))
    db_path = data_root / "data" / "permit_tracker.db"

    permit = fetch_permit_for_company(db_path, args.company_id)
    if not permit:
        out = {"status": "error", "reason": f"company_id={args.company_id} の permits/companies に許可情報なし"}
        print(json.dumps(out, ensure_ascii=False) if args.json else out["reason"], file=sys.stderr if not args.json else sys.stdout)
        sys.exit(2)

    screenshot_path = get_screenshot_path(data_root, permit["contractor_number"] or args.company_id)
    started = datetime.now()
    confirm_result = confirm_permit_with_playwright(permit, screenshot_path)
    elapsed = (datetime.now() - started).total_seconds()

    # ログ追記
    confirmation_log_path = data_root / "logs" / "mlit_confirmation_log.csv"
    append_confirmation_log(
        log_path=confirmation_log_path,
        company_id=args.company_id,
        confirmer="web_viewer_button",
        method="MLIT_SEARCH",
        result=confirm_result,
        permit_authority=permit["permit_authority_name_normalized"],
        contractor_number=permit["contractor_number"],
        screenshot_path=str(screenshot_path),
    )

    # DB 反映
    update_db_after_confirm(db_path, args.company_id, confirm_result, screenshot_path)

    out = {
        "status": "ok",
        "company_id": args.company_id,
        "official_name": permit.get("official_name", ""),
        "result": confirm_result,
        "screenshot": str(screenshot_path),
        "elapsed_sec": round(elapsed, 1),
        "confirmed_at": datetime.now().isoformat(timespec="seconds"),
    }
    if args.json:
        print(json.dumps(out, ensure_ascii=False))
    else:
        print(f"OK [{args.company_id}] {permit.get('official_name')} → {confirm_result} ({elapsed:.1f}秒)")


if __name__ == "__main__":
    main()
