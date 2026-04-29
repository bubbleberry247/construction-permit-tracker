"""
Stage 2 Phase 2.2: DB を「マスタ正本」に揃えるクリーニングスクリプト。

処理:
  A. 表記揺れ rename 6 件 (official_name 更新 + name_aliases に旧名追加)
  B. 幽霊7社を status='INACTIVE' 化 (name_aliases に GHOST タグ追加)
  C. 検算 (件数、FK整合性、関連テーブル参照件数)

dry-run: DB を temp にコピーして実 SQL 実行 → COMMIT → 検算 → 検算ログ JSON 出力
execute: 本番 DB に transaction で適用、検算 NG なら ROLLBACK

Usage:
  python scripts/cleanup_db_to_master.py --state-json data/stage2_state_*.json   (dry-run)
  python scripts/cleanup_db_to_master.py --state-json data/stage2_state_*.json --execute
"""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
TMP_DB = Path("/tmp") / "cleanup_dryrun.db"

# 処理 A: rename 対象
RENAMES = [
    # (company_id, new_official_name, old_alias)
    ("C0013", "吉田電氣工事株式会社",       "吉田電気工事株式会社"),
    ("C0051", "株式会社横河システム建築",   "株式会社横河ブリッジシステム建築"),
    ("C0057", "豊銕工業株式会社",           "豊鉄工業株式会社"),
    ("C0096", "有限会社尾﨑鋼業",           "有限会社尾崎鋼業"),
    ("C0048", "株式会社山西",               "株式会社山西 豊田店"),
    ("C0074", "株式会社ハシモト電気",       "橋本電機株式会社"),
]

# 処理 B: 幽霊 INACTIVE 化対象
GHOSTS = [
    "C0086", "C0102", "C0108", "C0109", "C0127", "C0130", "C0136",
]
GHOST_TAG = "GHOST_2026-03-27_AUTO_INSERTED"


def add_alias(existing: str | None, new_alias: str) -> str:
    """既存 name_aliases に new_alias を追加 (重複/NULL 安全)。"""
    parts = [p.strip() for p in (existing or "").split("|") if p.strip()]
    if new_alias in parts:
        return existing or ""
    parts.append(new_alias)
    return "|".join(parts)


def fetch_state(conn: sqlite3.Connection) -> dict:
    """検算用に DB の現在状態を取得"""
    c = conn.cursor()
    rs = {}
    rs["status_counts"] = dict(c.execute(
        "SELECT status, COUNT(*) FROM companies GROUP BY status"
    ).fetchall())
    rs["ghost_count"] = c.execute(
        "SELECT COUNT(*) FROM companies WHERE name_aliases LIKE ?",
        (f"%{GHOST_TAG}%",),
    ).fetchone()[0]
    rs["target_companies"] = {}
    target_ids = [r[0] for r in RENAMES] + GHOSTS
    placeholders = ",".join("?" * len(target_ids))
    for r in c.execute(
        f"SELECT company_id, official_name, status, name_aliases FROM companies "
        f"WHERE company_id IN ({placeholders}) ORDER BY company_id",
        target_ids,
    ):
        rs["target_companies"][r[0]] = {
            "official_name": r[1], "status": r[2], "name_aliases": r[3],
        }
    # 関連テーブル参照件数 (rename 6 社のみ)
    rename_ids = [r[0] for r in RENAMES]
    ph = ",".join("?" * len(rename_ids))
    rs["fk_refs"] = {}
    for tbl in ["pages", "files", "permits", "inbound_messages", "company_emails"]:
        rs["fk_refs"][tbl] = c.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE company_id IN ({ph})", rename_ids
        ).fetchone()[0]
    rs["fk_check"] = c.execute("PRAGMA foreign_key_check").fetchall()
    return rs


def apply_changes(conn: sqlite3.Connection) -> dict:
    """rename + 幽霊 INACTIVE 化を適用。返値は更新行数の集計"""
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rename_n = 0
    for cid, new_name, old_alias in RENAMES:
        cur = c.execute(
            "SELECT name_aliases FROM companies WHERE company_id = ?", (cid,)
        ).fetchone()
        if cur is None:
            print(f"  [SKIP] {cid}: DB に存在しない", file=sys.stderr)
            continue
        new_aliases = add_alias(cur[0], old_alias)
        c.execute(
            "UPDATE companies SET official_name=?, name_aliases=?, updated_at=? "
            "WHERE company_id=?",
            (new_name, new_aliases, now, cid),
        )
        rename_n += c.rowcount

    ghost_n = 0
    for cid in GHOSTS:
        cur = c.execute(
            "SELECT name_aliases FROM companies WHERE company_id = ?", (cid,)
        ).fetchone()
        if cur is None:
            continue
        new_aliases = add_alias(cur[0], GHOST_TAG)
        c.execute(
            "UPDATE companies SET status='INACTIVE', name_aliases=?, updated_at=? "
            "WHERE company_id=?",
            (new_aliases, now, cid),
        )
        ghost_n += c.rowcount
    return {"renames": rename_n, "ghosts_inactivated": ghost_n}


def verify(before: dict, after: dict) -> list[str]:
    """検算: 期待値と実値を比較してエラーリスト返す"""
    errors = []
    expected_active = before["status_counts"].get("ACTIVE", 0) - len(GHOSTS)
    actual_active = after["status_counts"].get("ACTIVE", 0)
    if actual_active != expected_active:
        errors.append(f"ACTIVE 件数: actual={actual_active}, expected={expected_active}")

    expected_inactive = before["status_counts"].get("INACTIVE", 0) + len(GHOSTS)
    actual_inactive = after["status_counts"].get("INACTIVE", 0)
    if actual_inactive != expected_inactive:
        errors.append(f"INACTIVE 件数: actual={actual_inactive}, expected={expected_inactive}")

    if after["ghost_count"] != len(GHOSTS):
        errors.append(f"GHOST タグ件数: actual={after['ghost_count']}, expected={len(GHOSTS)}")

    # rename 後の official_name 確認
    for cid, new_name, _ in RENAMES:
        actual = after["target_companies"].get(cid, {}).get("official_name")
        if actual != new_name:
            errors.append(f"{cid} official_name: actual='{actual}', expected='{new_name}'")

    # 幽霊 status 確認
    for cid in GHOSTS:
        actual = after["target_companies"].get(cid, {}).get("status")
        if actual != "INACTIVE":
            errors.append(f"{cid} status: actual='{actual}', expected='INACTIVE'")

    # 関連テーブル参照件数 (rename だけでは件数は不変のはず)
    for tbl, before_cnt in before["fk_refs"].items():
        after_cnt = after["fk_refs"].get(tbl)
        if before_cnt != after_cnt:
            errors.append(f"FK ref {tbl}: actual={after_cnt}, expected={before_cnt}")

    # FK violations: UPDATE で増加していないか (既存 violations は許容)
    before_fk = len(before["fk_check"])
    after_fk = len(after["fk_check"])
    if after_fk > before_fk:
        errors.append(
            f"PRAGMA foreign_key_check 増加: before={before_fk}, after={after_fk} "
            f"(UPDATE で {after_fk - before_fk} 件追加)"
        )
    elif before_fk > 0:
        print(f"  [WARN] DB に既存 FK violations が {before_fk} 件あります "
              f"(UPDATE 前後で変化なし、Stage 2 のスコープ外)")

    return errors


def run(db_path: Path, dry_run: bool, state_json: Path) -> dict:
    """dry-run: temp DB で実行 / execute: 本番 DB で transaction"""
    if dry_run:
        if TMP_DB.exists():
            TMP_DB.unlink()
        TMP_DB.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(db_path, TMP_DB)
        target = TMP_DB
        print(f"[DRY-RUN] temp DB にコピー: {target}")
    else:
        target = db_path

    conn = sqlite3.connect(str(target))
    conn.execute("PRAGMA foreign_keys = ON")

    before = fetch_state(conn)

    try:
        conn.execute("BEGIN IMMEDIATE")
        change_n = apply_changes(conn)
        after = fetch_state(conn)
        errors = verify(before, after)

        result = {
            "mode": "dry-run" if dry_run else "execute",
            "target_db": str(target),
            "before": before,
            "after": after,
            "changes": change_n,
            "errors": errors,
        }

        if errors:
            print("\n!! 検算エラー:", file=sys.stderr)
            for e in errors:
                print(f"  - {e}", file=sys.stderr)
            conn.execute("ROLLBACK")
            print("\nROLLBACK 実行 (本番 DB は不変)", file=sys.stderr)
        else:
            conn.execute("COMMIT")
            print("\nCOMMIT 完了" if not dry_run else "\nCOMMIT (temp DB のみ、本番 DB は不変)")
    finally:
        conn.close()

    return result


def update_state_json(state_json: Path, result: dict, dry_run: bool):
    state = json.loads(state_json.read_text(encoding="utf-8"))
    phase_key = "2.2_db_cleanup"
    if dry_run:
        state["phases"][phase_key]["dry_run_log"] = result
    else:
        state["phases"][phase_key]["completed_at"] = datetime.now().isoformat()
        state["phases"][phase_key]["rows_updated"] = (
            result["changes"]["renames"] + result["changes"]["ghosts_inactivated"]
        )
        state["phases"][phase_key]["execute_log"] = result
    state_json.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default=str(DB))
    ap.add_argument("--state-json", required=True, type=Path)
    ap.add_argument("--execute", action="store_true",
                    help="本実行（既定 dry-run）")
    ap.add_argument("--require-backup", action="store_true", default=True)
    args = ap.parse_args()

    state = json.loads(args.state_json.read_text(encoding="utf-8"))

    # バックアップ確認
    if args.require_backup:
        bak_artifacts = [a for a in state["phases"]["2.0_backup"]["artifacts"]
                         if a.get("type") == "db_backup"]
        if not bak_artifacts:
            print("ERROR: DB バックアップが state JSON に記録されていません", file=sys.stderr)
            sys.exit(2)
        bak_path = Path(bak_artifacts[0]["path"])
        if not bak_path.exists():
            print(f"ERROR: DB バックアップファイル不在: {bak_path}", file=sys.stderr)
            sys.exit(2)
        print(f"[OK] DB バックアップ確認: {bak_path.name}")

    is_dry_run = not args.execute
    db_path = Path(args.db_path)

    print(f"\n{'='*60}")
    print(f"  Mode: {'DRY-RUN' if is_dry_run else 'EXECUTE'}")
    print(f"  Target DB: {db_path}")
    print(f"{'='*60}\n")

    result = run(db_path, is_dry_run, args.state_json)
    update_state_json(args.state_json, result, is_dry_run)

    print(f"\n=== 結果サマリ ===")
    print(f"  Before status: {result['before']['status_counts']}")
    print(f"  After  status: {result['after']['status_counts']}")
    print(f"  Renames: {result['changes']['renames']} 件")
    print(f"  Ghosts→INACTIVE: {result['changes']['ghosts_inactivated']} 件")
    print(f"  検算エラー: {len(result['errors'])} 件")

    if result["errors"]:
        sys.exit(2)
    if is_dry_run:
        print(f"\n※ dry-run 完了。本実行は --execute オプションを付けてください。")


if __name__ == "__main__":
    main()
