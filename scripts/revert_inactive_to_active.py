"""
誤って INACTIVE 化された 14 社の status を ACTIVE に戻す。

対象（書類保有 or 藤田さんマスタ由来）:
  C0008, C0013, C0048, C0051, C0057, C0074, C0086, C0096,
  C0102, C0108, C0109, C0127, C0130, C0136

INACTIVE_IDS（rebuild_company_master.py の正規リスト 13 件）には含まれていないため
意図しない更新と判断。

手順:
  1. 事前バックアップ（SQLite .backup API）
  2. BEGIN IMMEDIATE
  3. UPDATE companies SET status='ACTIVE' WHERE company_id IN (...)
  4. 影響行数確認 + 期待数 (14) と一致検証
  5. commit or rollback

Usage:
  python scripts/revert_inactive_to_active.py             # dry-run
  python scripts/revert_inactive_to_active.py --execute   # 実行
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"

TARGET_IDS = [
    "C0008", "C0013", "C0048", "C0051", "C0057", "C0074",
    "C0086", "C0096", "C0102", "C0108", "C0109", "C0127", "C0130", "C0136",
]


def backup_db(db_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = db_path.parent / f"permit_tracker.db.bak_pre_status_revert_{ts}"
    src = sqlite3.connect(str(db_path))
    dst = sqlite3.connect(str(bak))
    with dst:
        src.backup(dst)
    dst.close()
    src.close()
    return bak


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="実行（既定 dry-run）")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    placeholders = ",".join("?" * len(TARGET_IDS))
    rows = conn.execute(
        f"SELECT company_id, official_name, status FROM companies "
        f"WHERE company_id IN ({placeholders}) ORDER BY company_id",
        TARGET_IDS,
    ).fetchall()

    print(f"対象 {len(TARGET_IDS)} 社の現在 status:")
    inactive_n = 0
    for r in rows:
        marker = "★INACTIVE" if r["status"] == "INACTIVE" else "  ACTIVE  "
        print(f"  {marker}  {r['company_id']}  {r['official_name']}")
        if r["status"] == "INACTIVE":
            inactive_n += 1
    print(f"\n  INACTIVE: {inactive_n} 社")

    if inactive_n == 0:
        print("\n変更不要（全て ACTIVE）")
        return

    if not args.execute:
        print(f"\n(dry-run) {inactive_n} 社を ACTIVE に戻します。--execute で実行してください。")
        return

    # 実行
    print("\n--- 事前バックアップ ---")
    bak = backup_db(DB)
    print(f"  バックアップ: {bak}")

    print("\n--- トランザクション実行 ---")
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            f"UPDATE companies SET status='ACTIVE', updated_at=? "
            f"WHERE company_id IN ({placeholders}) AND status='INACTIVE'",
            (datetime.now().isoformat(), *TARGET_IDS),
        )
        affected = cur.rowcount
        if affected != inactive_n:
            raise RuntimeError(f"想定外の更新行数: 期待 {inactive_n}, 実際 {affected}")
        print(f"  UPDATE 完了: {affected} 行")

        # 検証
        still_inactive = conn.execute(
            f"SELECT COUNT(*) FROM companies "
            f"WHERE company_id IN ({placeholders}) AND status='INACTIVE'",
            TARGET_IDS,
        ).fetchone()[0]
        if still_inactive > 0:
            raise RuntimeError(f"INACTIVE が残存: {still_inactive}")
        print("  検証: OK")

        conn.commit()
        print("\n✓ commit 完了")
    except Exception as e:
        conn.rollback()
        print(f"\n✗ rollback: {e}", file=sys.stderr)
        print(f"  バックアップ: {bak}")
        raise

    # 事後確認
    rows_after = conn.execute(
        f"SELECT company_id, official_name, status FROM companies "
        f"WHERE company_id IN ({placeholders}) ORDER BY company_id",
        TARGET_IDS,
    ).fetchall()
    active_n = sum(1 for r in rows_after if r["status"] == "ACTIVE")
    print(f"\n事後: {active_n}/{len(TARGET_IDS)} 社が ACTIVE")
    print(f"次のアクション:")
    print(f"  python scripts/generate_partners_list.py  （ダッシュボード再生成）")


if __name__ == "__main__":
    main()
