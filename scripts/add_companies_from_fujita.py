"""
Phase R-B: 藤田だけにあって当方マスタにない会社を staging テーブル経由で追加。

GPT-5.5 レビュー反映 (Issue 1):
  本体 companies への直接 INSERT を廃止。
  company_additions_staging に積み、別フェーズで promote する 2 段階設計に変更。

3 つのモード:
  1. (デフォルト) dry-run: 何が staging に入るかを CSV/標準出力で表示
  2. --execute: company_additions_staging に INSERT (本体 companies は触らない)
  3. --promote: staging から「approved」のものを companies に INSERT (採番 + transaction)

入力: Phase R-A シート 2 (output/FDE_MANAGED/09_master_diff_<TS>.xlsx)

source_row_hash で行単位の重複検出 + UNIQUE 制約 (UNIQUE(source_row_hash, workflow_id)) で
同じ入力の二重 staging を防止。

Usage:
  python scripts/add_companies_from_fujita.py <09_master_diff_*.xlsx>            # dry-run
  python scripts/add_companies_from_fujita.py <09_master_diff_*.xlsx> --execute  # staging に積む
  python scripts/add_companies_from_fujita.py --promote                          # staging→本体
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import re
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path

import openpyxl

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "permit_tracker.db"


def stable_hash(*parts) -> str:
    s = "|".join(str(p) if p is not None else "" for p in parts)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def get_max_company_id(conn: sqlite3.Connection) -> int:
    """C0XXX 形式の最大番号を返す。BEGIN 後に呼ぶこと"""
    rows = conn.execute("SELECT company_id FROM companies WHERE company_id LIKE 'C0%'")
    max_n = 0
    for (cid,) in rows:
        m = re.match(r"^C0(\d+)$", cid)
        if m:
            n = int(m.group(1))
            if n > max_n:
                max_n = n
    return max_n


def load_diff_sheet2(xlsx: Path) -> list[dict]:
    wb = openpyxl.load_workbook(xlsx, data_only=True)
    ws = wb["2_藤田のみ"]
    rows = []
    headers = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
    for r in range(2, ws.max_row + 1):
        row = {h: ws.cell(r, c).value for c, h in enumerate(headers, 1)}
        if not row.get("藤田会社名"):
            continue
        rows.append(row)
    return rows


def cmd_stage(args):
    """default + --execute モード: staging に INSERT"""
    if not args.diff_xlsx or not args.diff_xlsx.exists():
        print(f"ERROR: --diff-xlsx を指定してください", file=sys.stderr)
        sys.exit(1)

    print(f"=== Phase R-B (Stage) ===")
    print(f"  TS: {args.ts}")
    print(f"  diff_xlsx: {args.diff_xlsx.name}")
    print(f"  mode: {'EXECUTE' if args.execute else 'DRY-RUN'}")

    workflow_id = str(uuid.uuid4())
    print(f"  workflow_id: {workflow_id}")

    rows = load_diff_sheet2(args.diff_xlsx)
    print(f"\nシート 2「藤田のみ」: {len(rows)} 社")

    to_stage = []
    skipped = {"対象外": 0, "保留": 0, "別名疑い": 0, "未判定": 0}
    for row in rows:
        user_judgment = (row.get("ユーザー最終判定 [追加/別名疑い/対象外/保留]") or "").strip()
        recommended = (row.get("推奨アクション") or "").strip()
        effective = user_judgment if user_judgment else recommended
        if not effective:
            skipped["未判定"] += 1
            continue
        if effective.startswith("対象外"):
            skipped["対象外"] += 1
            continue
        if effective.startswith("別名疑い"):
            skipped["別名疑い"] += 1
            continue
        if effective in ("保留",):
            skipped["保留"] += 1
            continue
        if not effective.startswith("追加"):
            skipped["未判定"] += 1
            continue
        # 追加候補
        src_hash = stable_hash(
            row.get("藤田行番号"), row.get("藤田会社名"),
            row.get("藤田status"), row.get("許可番号"), row.get("行政庁"),
        )
        to_stage.append({
            "fujita_row_id": row.get("藤田行番号"),
            "fujita_name": row.get("藤田会社名"),
            "permit_number": row.get("許可番号"),
            "permit_authority": row.get("行政庁"),
            "fujita_status": row.get("藤田status"),
            "user_judgment": effective,
            "source_row_hash": src_hash,
            "source_file_name": args.diff_xlsx.name,
            "workflow_id": workflow_id,
            "notes": row.get("コメント") or "",
        })

    print(f"\nstaging 対象:        {len(to_stage)} 社")
    for k, v in skipped.items():
        if v:
            print(f"  skip ({k}): {v} 社")

    if not to_stage:
        print("\n対象なし。終了。")
        return

    # CSV
    csv_path = PROJECT / "output" / "FDE_MANAGED" / f"phase_rb_staging_plan_{args.ts}.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["fujita_row_id", "fujita_name", "permit_number", "permit_authority",
                    "fujita_status", "user_judgment", "source_row_hash", "workflow_id"])
        for s in to_stage:
            w.writerow([s["fujita_row_id"], s["fujita_name"], s["permit_number"],
                        s["permit_authority"], s["fujita_status"], s["user_judgment"],
                        s["source_row_hash"][:16], s["workflow_id"]])
    print(f"\n計画 CSV: {csv_path.relative_to(PROJECT)}")
    print(f"\nstaging 予定 (上位 10):")
    for s in to_stage[:10]:
        print(f"  {s['fujita_name'][:25]:<26}  perm={s['permit_number'] or '-'}  auth={s['permit_authority'] or '-'}")

    if not args.execute:
        print("\n[DRY-RUN] DB は変更されていません。--execute で staging に INSERT。")
        return

    # Execute: staging INSERT (transaction)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("BEGIN")
        inserted = 0
        skipped_dup = 0
        for s in to_stage:
            try:
                conn.execute(
                    "INSERT INTO company_additions_staging "
                    "(fujita_row_id, fujita_name, permit_number, permit_authority, "
                    " fujita_status, user_judgment, source_row_hash, source_file_name, "
                    " workflow_id, notes) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (s["fujita_row_id"], s["fujita_name"], s["permit_number"],
                     s["permit_authority"], s["fujita_status"], s["user_judgment"],
                     s["source_row_hash"], s["source_file_name"], s["workflow_id"],
                     s["notes"]),
                )
                inserted += 1
            except sqlite3.IntegrityError as e:
                # UNIQUE(source_row_hash, workflow_id) 違反 = 二重 staging
                skipped_dup += 1
                print(f"  [DUP] {s['fujita_name']}: {e}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"  EXCEPTION: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()
    print(f"\n=== EXECUTE 完了 ===")
    print(f"  staging INSERT: {inserted} 社")
    print(f"  duplicate skip: {skipped_dup} 社")
    print()
    print("次のステップ: ユーザーが staging を確認 → approved にして --promote")


def cmd_promote(args):
    """staging から approved のものを本体 companies に INSERT (採番 + transaction)"""
    print(f"=== Phase R-B (Promote) ===")
    print(f"  TS: {args.ts}")
    print(f"  mode: {'EXECUTE' if args.execute else 'DRY-RUN'}")

    conn = sqlite3.connect(DB_PATH)
    rows = list(conn.execute(
        "SELECT staging_id, fujita_row_id, fujita_name, permit_number, permit_authority, "
        "       workflow_id, approval_status FROM company_additions_staging "
        "WHERE approval_status='approved' AND promoted_at IS NULL "
        "ORDER BY staging_id"
    ))
    print(f"\npromote 対象 (approval_status=approved AND promoted_at IS NULL): {len(rows)} 社")

    if not rows:
        print("対象なし。staging を approved に更新するには別途 SQL 実行:")
        print("  UPDATE company_additions_staging SET approval_status='approved', approved_by='masaru', "
              "approved_at=datetime('now','localtime') WHERE staging_id IN (...)")
        conn.close()
        return

    if not args.execute:
        for r in rows[:10]:
            print(f"  staging_id={r[0]} {r[2][:25]:<26} perm={r[3] or '-'}")
        print("\n[DRY-RUN] --execute で本体 companies に promote")
        conn.close()
        return

    # 採番 + INSERT (transaction)
    try:
        conn.execute("BEGIN IMMEDIATE")  # 競合防止 (採番中の write lock)
        max_n = get_max_company_id(conn)
        promoted = 0
        for r in rows:
            staging_id, fr_id, name, pnum, pauth, wf_id, _ = r
            max_n += 1
            new_cid = f"C0{max_n:03d}"
            # 念のため UNIQUE 衝突チェック
            existing = conn.execute(
                "SELECT 1 FROM companies WHERE company_id=?", (new_cid,)
            ).fetchone()
            if existing:
                print(f"  [SKIP] {new_cid} 既存、{name} skip", file=sys.stderr)
                continue
            conn.execute(
                "INSERT INTO companies (company_id, official_name, permit_number, "
                "permit_authority, status) VALUES (?, ?, ?, ?, ?)",
                (new_cid, name, pnum, pauth, "ACTIVE"),
            )
            conn.execute(
                "UPDATE company_additions_staging SET promoted_at=datetime('now','localtime'), "
                "promoted_company_id=?, approval_status='promoted' WHERE staging_id=?",
                (new_cid, staging_id),
            )
            # fujita_mapping も更新
            conn.execute(
                "UPDATE fujita_mapping SET company_id=?, confidence='HIGH', "
                "confirmed_at=datetime('now','localtime'), confirmed_by='phase_rb_promote' "
                "WHERE fujita_row_id=?",
                (new_cid, fr_id),
            )
            promoted += 1
            print(f"  promoted: {new_cid}  {name}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"  EXCEPTION: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()

    print(f"\n=== Promote 完了: {promoted} 社 ===")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("diff_xlsx", nargs="?", type=Path,
                    help="09_master_diff_*.xlsx (stage モード時必須)")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--promote", action="store_true",
                    help="staging から本体 companies に promote")
    ap.add_argument("--ts", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    args = ap.parse_args()

    if args.promote:
        cmd_promote(args)
    else:
        cmd_stage(args)


if __name__ == "__main__":
    main()
