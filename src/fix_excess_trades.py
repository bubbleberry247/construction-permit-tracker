"""
fix_excess_trades.py — 業種数が異常に多い3社を国交省APIで検証し、正しい業種数に修正する。

対象:
  C0059 三信建材工業（22業種 = permit_id 4+83 重複）— 許可番号14569, 国土交通大臣
  C0078 大嶽安城（15業種）— 許可番号12009, 愛知県知事
  C0073 太田商事（14業種）— 許可番号1152, 愛知県知事

Usage:
    python src/fix_excess_trades.py              # dry-run (既定)
    python src/fix_excess_trades.py --execute    # 実行
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

sys.stdout.reconfigure(encoding="utf-8")

# 既存モジュール再利用
sys.path.insert(0, str(Path(__file__).parent))
from verify_permits_mlit import (
    MLIT_DETAIL_URL,
    MLIT_TRADE_ABBREV,
    fetch_detail,
)
from utils.trade_master import TRADE_CATEGORIES

DB_PATH = Path(__file__).parent.parent / "data" / "permit_tracker.db"
BACKUP_DIR = Path(__file__).parent.parent / "data" / "backups"
REQUEST_INTERVAL_SEC = 1.0

# ---------------------------------------------------------------------------
# 対象3社の定義
# ---------------------------------------------------------------------------

# (company_id, permit_id, sv_licenseNo, 説明)
TARGETS: list[tuple[str, int, str, str]] = [
    ("C0059", 4, "00014569", "三信建材工業 (大臣 14569)"),
    ("C0078", 86, "23012009", "大嶽安城 (愛知 12009)"),
    ("C0073", 12, "23001152", "太田商事 (愛知 1152)"),
]

# C0059の重複permit_id（削除対象）
C0059_DUPLICATE_PERMIT_ID = 83

# MLIT略称 → 正規名称マッピング
ABBREV_TO_CANONICAL: dict[str, str] = {}
for abbrev, canonical in zip(MLIT_TRADE_ABBREV, TRADE_CATEGORIES):
    ABBREV_TO_CANONICAL[abbrev] = canonical


def backup_db(db_path: Path) -> Path:
    """DBバックアップを作成"""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"permit_tracker_{ts}.db"
    shutil.copy2(str(db_path), str(backup_path))
    print(f"[BACKUP] {backup_path}")
    return backup_path


def get_db_trades(conn: sqlite3.Connection, permit_id: int) -> list[str]:
    """DB上のpermit_tradesを取得"""
    rows = conn.execute(
        "SELECT trade_name FROM permit_trades WHERE permit_id = ? ORDER BY trade_name",
        (permit_id,),
    ).fetchall()
    return [r[0] for r in rows]


def api_trades_to_canonical(api_trades: list[str]) -> list[str]:
    """API略称を正規名称に変換"""
    result: list[str] = []
    for t in api_trades:
        canonical = ABBREV_TO_CANONICAL.get(t)
        if canonical:
            result.append(canonical)
        else:
            print(f"  [WARN] 未知の略称: {t}")
            result.append(t)
    return sorted(result)


def update_permit_trades(
    conn: sqlite3.Connection,
    permit_id: int,
    new_trades: list[str],
    dry_run: bool,
) -> tuple[int, int]:
    """permit_tradesを更新。Returns (deleted, inserted)"""
    if dry_run:
        return (0, 0)

    deleted = conn.execute(
        "DELETE FROM permit_trades WHERE permit_id = ?", (permit_id,),
    ).rowcount

    for trade in new_trades:
        conn.execute(
            "INSERT INTO permit_trades (permit_id, trade_name) VALUES (?, ?)",
            (permit_id, trade),
        )

    return (deleted, len(new_trades))


def delete_duplicate_permit(
    conn: sqlite3.Connection,
    permit_id: int,
    dry_run: bool,
) -> int:
    """重複permitレコードを削除（permit_trades含む）"""
    if dry_run:
        return 0

    conn.execute("DELETE FROM permit_trades WHERE permit_id = ?", (permit_id,))
    deleted = conn.execute(
        "DELETE FROM permits WHERE permit_id = ?", (permit_id,),
    ).rowcount
    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(description="業種数異常3社のAPI検証・DB修正")
    parser.add_argument("--execute", action="store_true", help="実際にDBを更新する")
    args = parser.parse_args()
    dry_run = not args.execute

    print("=" * 60)
    print("  業種数異常3社の国交省API検証・DB修正")
    print(f"  モード: {'DRY-RUN (変更なし)' if dry_run else 'EXECUTE (DB更新)'}")
    print("=" * 60)

    # APIセッション
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
    })

    # DB接続
    if not dry_run:
        backup_db(DB_PATH)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        # --- Step 1: API取得 & 比較 ---
        api_results: dict[str, list[str]] = {}  # company_id -> canonical trades

        for i, (cid, pid, sv_license_no, desc) in enumerate(TARGETS):
            print(f"\n--- [{i+1}/{len(TARGETS)}] {desc} (permit_id={pid}) ---")
            print(f"  sv_licenseNo: {sv_license_no}")

            # API詳細取得
            detail = fetch_detail(session, sv_license_no)

            if not detail.found:
                print(f"  [ERROR] API取得失敗")
                conn.close()
                return

            print(f"  API商号: {detail.api_name}")
            print(f"  API有効期間: {detail.api_expiry_wareki}")
            print(f"  API業種(略称): {detail.api_trades}")

            # 正規名称に変換
            canonical = api_trades_to_canonical(detail.api_trades)
            api_results[cid] = canonical
            print(f"  API業種(正規): {canonical}")
            print(f"  API業種数: {len(canonical)}")

            # DB比較
            db_trades = get_db_trades(conn, pid)
            print(f"  DB業種: {db_trades}")
            print(f"  DB業種数: {len(db_trades)}")

            db_set = set(db_trades)
            api_set = set(canonical)

            if db_set == api_set:
                print(f"  => 一致 (変更不要)")
            else:
                only_db = db_set - api_set
                only_api = api_set - db_set
                if only_db:
                    print(f"  => DBのみ: {sorted(only_db)}")
                if only_api:
                    print(f"  => APIのみ: {sorted(only_api)}")

            if i < len(TARGETS) - 1:
                time.sleep(REQUEST_INTERVAL_SEC)

        # --- Step 2: DB更新 ---
        print("\n" + "=" * 60)
        print("  DB更新")
        print("=" * 60)

        # C0059: 重複permit_id=83を削除
        print(f"\n[C0059] 重複permit_id={C0059_DUPLICATE_PERMIT_ID}の削除")
        dup_trades = get_db_trades(conn, C0059_DUPLICATE_PERMIT_ID)
        print(f"  削除対象: permit_id={C0059_DUPLICATE_PERMIT_ID}, trades={len(dup_trades)}件")
        if not dry_run:
            delete_duplicate_permit(conn, C0059_DUPLICATE_PERMIT_ID, dry_run)
            print(f"  => 削除完了")
        else:
            print(f"  => [DRY-RUN] スキップ")

        # 各社のpermit_trades更新
        for cid, pid, _, desc in TARGETS:
            canonical = api_results.get(cid, [])
            db_trades = get_db_trades(conn, pid)
            db_set = set(db_trades)
            api_set = set(canonical)

            print(f"\n[{cid}] {desc} (permit_id={pid})")
            print(f"  DB: {len(db_trades)}業種 -> API: {len(canonical)}業種")

            if db_set == api_set:
                print(f"  => 一致 (変更不要)")
                continue

            print(f"  => 差分あり: DELETE {len(db_trades)} + INSERT {len(canonical)}")
            if not dry_run:
                deleted, inserted = update_permit_trades(conn, pid, canonical, dry_run)
                print(f"  => 更新完了: deleted={deleted}, inserted={inserted}")
            else:
                print(f"  => [DRY-RUN] スキップ")

        if not dry_run:
            conn.commit()
            print("\n[COMMIT] 全変更をコミットしました")

        # --- Step 3: 検証 ---
        print("\n" + "=" * 60)
        print("  検証 (更新後)")
        print("=" * 60)

        for cid, pid, _, desc in TARGETS:
            trades = get_db_trades(conn, pid)
            print(f"  {cid} {desc}: permit_id={pid}, {len(trades)}業種")

        # C0059の重複確認
        dup_check = conn.execute(
            "SELECT COUNT(*) FROM permits WHERE company_id = 'C0059'",
        ).fetchone()[0]
        print(f"  C0059 permits数: {dup_check}")

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
