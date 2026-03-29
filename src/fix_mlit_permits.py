"""
fix_mlit_permits.py — MLIT API検証結果に基づくDB修正スクリプト

国交省API検証で判明した以下の不一致を修正:
1. 期限不一致 6件: API側のexpiry_date/issue_dateでDB更新
2. 業種空 1件: C0059 permit_id=83にpermit_tradesをINSERT
3. NOT_FOUND 3件: permit_numberまたはpermit_authorityの修正

Usage:
    python src/fix_mlit_permits.py              # dry-run (既定)
    python src/fix_mlit_permits.py --execute    # 実行
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH = Path(__file__).parent.parent / "data" / "permit_tracker.db"
BACKUP_DIR = Path(__file__).parent.parent / "data" / "backups"

# ---------------------------------------------------------------------------
# 修正データ（MLIT APIから取得済み）
# ---------------------------------------------------------------------------

# 期限不一致: (permit_id, new_issue_date, new_expiry_date, note)
EXPIRY_FIXES: list[tuple[int, str, str, str]] = [
    (72, "2025-11-28", "2030-11-27", "C0054 鈴木産業 (大臣 28652) 許可更新"),
    (50, "2025-11-28", "2030-11-27", "C0056 福釜電工 (愛知 45685 特) 許可更新"),
    (74, "2025-11-28", "2030-11-27", "C0056 福釜電工 (愛知 45685 般) 許可更新"),
    (59, "2026-02-19", "2031-02-18", "C0070 サムシング (大臣 021635) 許可更新"),
    (1,  "2025-05-26", "2030-05-25", "C0081 イシハラ (愛知 69390) 許可更新"),
    (43, "2024-07-16", "2029-07-15", "C0074 橋本電機 (岐阜 15330) 許可更新"),
]

# 業種空: C0059 permit_id=83 — API結果から補完
TRADES_INSERT_83: list[str] = [
    "建築", "左官", "とび・土工", "石工", "屋根",
    "タイル・れんが・ブロック", "板金", "塗装", "防水",
    "内装仕上", "熱絶縁",
]

# NOT_FOUND修正: (permit_id, fix_field, old_value, new_value, note)
NOT_FOUND_FIXES: list[tuple[int, str, str, str, str]] = [
    # C0078: 番号120009→12009（先頭1は不要）
    (86, "permit_number", "120009", "12009",
     "C0078 大嶽安城 API=23012009, 番号先頭1は不要"),
    # C0079: authority 愛知県知事→国土交通大臣
    (87, "permit_authority", "愛知県知事", "国土交通大臣",
     "C0079 Gテリア API=00024944, 大臣許可"),
    # C0117: authority 愛知県知事→岐阜県知事
    (90, "permit_authority", "愛知県知事", "岐阜県知事",
     "C0117 三富 API=21012365, 岐阜県知事"),
]


def backup_db(db_path: Path) -> Path:
    """DB バックアップを作成"""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"permit_tracker_{ts}.db"
    shutil.copy2(str(db_path), str(backup_path))
    print(f"[BACKUP] {backup_path}")
    return backup_path


def apply_expiry_fixes(conn: sqlite3.Connection, dry_run: bool) -> int:
    """期限不一致の修正"""
    print("\n=== 期限不一致の修正 ===")
    count = 0
    for permit_id, new_issue, new_expiry, note in EXPIRY_FIXES:
        row = conn.execute(
            "SELECT issue_date, expiry_date FROM permits WHERE permit_id = ?",
            (permit_id,),
        ).fetchone()
        if not row:
            print(f"  [SKIP] permit_id={permit_id} not found")
            continue

        old_issue = row[0]
        old_expiry = row[1]
        print(f"  permit_id={permit_id}: {note}")
        print(f"    issue_date:  {old_issue} -> {new_issue}")
        print(f"    expiry_date: {old_expiry} -> {new_expiry}")

        if not dry_run:
            conn.execute(
                "UPDATE permits SET issue_date = ?, expiry_date = ?, "
                "updated_at = datetime('now','localtime') "
                "WHERE permit_id = ?",
                (new_issue, new_expiry, permit_id),
            )
            count += 1

    return count


def apply_trades_fix(conn: sqlite3.Connection, dry_run: bool) -> int:
    """業種空 permit_id=83 の修正"""
    print("\n=== 業種空の修正 (permit_id=83, C0059) ===")
    existing = conn.execute(
        "SELECT COUNT(*) FROM permit_trades WHERE permit_id = 83",
    ).fetchone()[0]

    if existing > 0:
        print(f"  [SKIP] permit_id=83 already has {existing} trades")
        return 0

    print(f"  INSERT {len(TRADES_INSERT_83)} trades: {', '.join(TRADES_INSERT_83)}")
    if not dry_run:
        for trade in TRADES_INSERT_83:
            conn.execute(
                "INSERT INTO permit_trades (permit_id, trade_name) VALUES (?, ?)",
                (83, trade),
            )
    return len(TRADES_INSERT_83)


def apply_not_found_fixes(conn: sqlite3.Connection, dry_run: bool) -> int:
    """NOT_FOUND 3件の修正"""
    print("\n=== NOT_FOUND修正 ===")
    count = 0
    for permit_id, fix_field, old_val, new_val, note in NOT_FOUND_FIXES:
        row = conn.execute(
            f"SELECT {fix_field} FROM permits WHERE permit_id = ?",
            (permit_id,),
        ).fetchone()
        if not row:
            print(f"  [SKIP] permit_id={permit_id} not found")
            continue

        actual = row[0]
        print(f"  permit_id={permit_id}: {note}")
        print(f"    {fix_field}: {actual} -> {new_val}")

        if actual != old_val:
            print(f"    [WARN] expected old={old_val}, actual={actual}")

        if not dry_run:
            conn.execute(
                f"UPDATE permits SET {fix_field} = ?, "
                "updated_at = datetime('now','localtime') "
                "WHERE permit_id = ?",
                (new_val, permit_id),
            )
            count += 1

    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="MLIT API検証結果に基づくDB修正")
    parser.add_argument(
        "--execute", action="store_true",
        help="実際にDBを更新する（省略時はdry-run）",
    )
    args = parser.parse_args()
    dry_run = not args.execute

    print("=" * 60)
    print("  MLIT API検証結果に基づくDB修正")
    print(f"  モード: {'DRY-RUN (変更なし)' if dry_run else 'EXECUTE (DB更新)'}")
    print("=" * 60)

    if not dry_run:
        backup_db(DB_PATH)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        n_expiry = apply_expiry_fixes(conn, dry_run)
        n_trades = apply_trades_fix(conn, dry_run)
        n_notfound = apply_not_found_fixes(conn, dry_run)

        if not dry_run:
            conn.commit()
            print("\n[COMMIT] 全変更をコミットしました")

        print("\n" + "=" * 60)
        print("  サマリー")
        print("=" * 60)
        print(f"  期限更新:      {n_expiry} 件")
        print(f"  業種追加:      {n_trades} 件 (permit_id=83)")
        print(f"  NOT_FOUND修正: {n_notfound} 件")
        if dry_run:
            print("\n  [DRY-RUN] --execute オプションで実際に更新されます")
        print("=" * 60)

    except Exception as e:
        conn.rollback()
        print(f"\n[ERROR] {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
