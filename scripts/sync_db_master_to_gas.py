"""
Stage 2 Phase 2.4: DB → GAS Sheets 再同期 (rename 反映)。

スコープ:
  - Companies シート: ほぼ空 (1 行のみ) のため同期 skip。注記のみ。
  - MLITPermits シート: rename 6 社の company_name 列を DB の official_name に更新。
  - 全シート (Companies/Permits/MLITPermits) のスナップショットを Phase 2.0 で取得済の場合は skip、
    なければ取得して state JSON に記録。

dry-run: 更新予定行を stdout 表示、実書込しない。
execute: スナップショット取得 → MLITPermits 6 行 UPDATE → 検算。

Usage:
  python scripts/sync_db_master_to_gas.py --state-json data/stage2_state_*.json
  python scripts/sync_db_master_to_gas.py --state-json data/stage2_state_*.json --execute
  python scripts/sync_db_master_to_gas.py --restore-from data/sheet_snapshots/sheet_snapshot_pre_stage2_*.json --execute
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
CONFIG = PROJECT / "config.json"
SNAPSHOT_DIR = PROJECT / "data" / "sheet_snapshots"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# rename 6 社の planned new name (cleanup_db_to_master.py の RENAMES と同期)
# dry-run 時はこの planned name を使って Phase 2.2 execute 後の予測を出す
PLANNED_RENAMES = {
    "C0013": "吉田電氣工事株式会社",
    "C0051": "株式会社横河システム建築",
    "C0057": "豊銕工業株式会社",
    "C0096": "有限会社尾﨑鋼業",
    "C0048": "株式会社山西",
    "C0074": "株式会社ハシモト電気",
}
RENAME_TARGETS = list(PLANNED_RENAMES.keys())


def open_db_readonly():
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = 1")
    return conn


def take_snapshot(sh, sheet_titles: list[str]) -> dict:
    snapshot = {
        "taken_at": datetime.now().isoformat(),
        "sheet_id": sh.id,
        "sheets": {},
    }
    for title in sheet_titles:
        ws = sh.worksheet(title)
        values = ws.get_all_values()
        snapshot["sheets"][title] = {
            "worksheet_id": ws.id,
            "title": title,
            "headers": values[0] if values else [],
            "row_count": len(values),
            "col_count": len(values[0]) if values else 0,
            "values": values,
        }
    return snapshot


def restore_from_snapshot(sh, snapshot_path: Path) -> list[str]:
    """各シートを clear → snapshot の values で update → row count 検算"""
    snap = json.loads(snapshot_path.read_text(encoding="utf-8"))
    errors = []
    for title, sdata in snap["sheets"].items():
        try:
            ws = sh.worksheet(title)
            ws.clear()
            if sdata["values"]:
                ws.update(values=sdata["values"], range_name=None,
                          value_input_option="RAW")
            new_values = ws.get_all_values()
            if len(new_values) != sdata["row_count"]:
                errors.append(f"{title}: row_count mismatch "
                              f"expected={sdata['row_count']}, actual={len(new_values)}")
            print(f"  [OK] {title}: restored {sdata['row_count']} rows")
        except Exception as e:
            errors.append(f"{title}: {e}")
    return errors


def update_mlit_company_names(sh, db_official_names: dict[str, str], dry_run: bool) -> dict:
    """MLITPermits シートの company_name 列を rename 反映"""
    ws = sh.worksheet("MLITPermits")
    rows = ws.get_all_values()
    hdr = rows[0]
    cid_i = hdr.index("company_id")
    cnam_i = hdr.index("company_name")

    updates = []  # (row_index_1based, old_name, new_name)
    for i, r in enumerate(rows[1:], start=2):
        if len(r) <= max(cid_i, cnam_i):
            continue
        cid = r[cid_i]
        if cid in db_official_names:
            new_name = db_official_names[cid]
            if r[cnam_i] != new_name:
                updates.append((i, r[cnam_i], new_name))

    print(f"\nMLITPermits 更新予定: {len(updates)} 行")
    for row_idx, old, new in updates:
        print(f"  row {row_idx}: '{old}' → '{new}'")

    if not dry_run and updates:
        # batch update
        cell_updates = []
        for row_idx, _, new in updates:
            cell_updates.append({
                "range": gspread.utils.rowcol_to_a1(row_idx, cnam_i + 1),
                "values": [[new]],
            })
        ws.batch_update(cell_updates, value_input_option="RAW")
        print(f"\n[OK] {len(updates)} 行を MLITPermits に書き込み完了")

    return {"target_count": len(updates), "updates": [(i, o, n) for i, o, n in updates]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-json", type=Path, required=False)
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--restore-from", type=Path, default=None)
    args = ap.parse_args()

    is_dry_run = not args.execute
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    sa_path = cfg["GOOGLE_SERVICE_ACCOUNT_FILE"]
    sheet_id = cfg["GOOGLE_SHEETS_ID"]

    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)

    if args.restore_from:
        print(f"=== RESTORE モード ({args.restore_from.name}) ===")
        if is_dry_run:
            print("dry-run では restore せず、対象シートのみ表示")
            snap = json.loads(args.restore_from.read_text(encoding="utf-8"))
            for title, sdata in snap["sheets"].items():
                print(f"  {title}: {sdata['row_count']} rows")
            print("\n※ 本実行は --execute を付けてください。")
            return
        errors = restore_from_snapshot(sh, args.restore_from)
        if errors:
            print("\n!! restore エラー:", file=sys.stderr)
            for e in errors:
                print(f"  - {e}", file=sys.stderr)
            sys.exit(2)
        print("\n✓ restore 完了。")
        return

    print(f"=== Phase 2.4 GAS Sheets 同期 ({'DRY-RUN' if is_dry_run else 'EXECUTE'}) ===\n")

    # Phase A: スナップショット取得 (Phase 2.0 で取得済か確認)
    snapshot_path = SNAPSHOT_DIR / f"sheet_snapshot_pre_stage2_{ts}.json"
    if not is_dry_run:
        SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
        snapshot = take_snapshot(sh, ["Companies", "Permits", "MLITPermits"])
        snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False),
                                 encoding="utf-8")
        print(f"[OK] スナップショット保存: {snapshot_path.name}")

    # Phase B: rename 6 社の official_name を取得
    if is_dry_run:
        # dry-run: Phase 2.2 execute 後の予測 = PLANNED_RENAMES を使う
        db_names = dict(PLANNED_RENAMES)
        print(f"\n[DRY-RUN] Phase 2.2 execute 後の予測 official_name (PLANNED_RENAMES):")
    else:
        # execute: 本番 DB から取得 (Phase 2.2 で rename 済の前提)
        conn = open_db_readonly()
        placeholders = ",".join("?" * len(RENAME_TARGETS))
        db_names = dict(conn.execute(
            f"SELECT company_id, official_name FROM companies "
            f"WHERE company_id IN ({placeholders})",
            RENAME_TARGETS,
        ).fetchall())
        conn.close()
        print(f"\nDB の rename 対象社 official_name (Phase 2.2 後):")
        unchanged = [cid for cid, n in db_names.items() if n != PLANNED_RENAMES.get(cid)]
        if unchanged:
            print(f"\n!! WARN: 以下の社は Phase 2.2 で未 rename です: {unchanged}", file=sys.stderr)
            print(f"   Phase 2.2 を --execute で先に実行してから本スクリプトを実行してください。",
                  file=sys.stderr)
            sys.exit(2)
    for cid, name in db_names.items():
        print(f"  {cid}: {name}")

    # Phase C: Companies は実質空 (1 行) のため skip
    print(f"\n[NOTE] Companies シートは 1 行のみで実質未使用、同期 skip")

    # Phase D: MLITPermits の company_name 更新
    result = update_mlit_company_names(sh, db_names, is_dry_run)

    # state JSON 更新
    if args.state_json:
        state = json.loads(args.state_json.read_text(encoding="utf-8"))
        phase_key = "2.4_gas_sync"
        if is_dry_run:
            state["phases"][phase_key]["dry_run_log"] = {
                "mlit_target_count": result["target_count"],
                "updates": [{"row": i, "old": o, "new": n} for i, o, n in result["updates"]],
            }
        else:
            state["phases"][phase_key]["completed_at"] = datetime.now().isoformat()
            state["phases"][phase_key]["rows_updated"] = result["target_count"]
            state["phases"][phase_key]["snapshot_path"] = str(snapshot_path)
        args.state_json.write_text(
            json.dumps(state, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    if is_dry_run:
        print(f"\n※ dry-run 完了。本実行は --execute を付けてください。")
    else:
        print(f"\n✓ Phase 2.4 完了")


if __name__ == "__main__":
    main()
