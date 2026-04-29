"""
Phase R-G-Audit: 藤田 81 件修正依頼の処理状況を機械生成 (GPT-5.5 v4 Issue 4 反映)。

apply_editor_decisions_v2.py 実行後に呼び出し、`decisions_log` を読んで
各 shusei_row が「処理済」「未反映」「却下」のいずれかを判定。
結果を 06_担当者編集_v2_*.xlsx の Sheet 2b「想定処理状況」列に書き戻す。

判定ロジック:
  - shusei_row に紐づく decisions_log の最新 decision_kind/decision_value を集計
  - 該当 decision が apply_status=applied → 「処理済」
  - 該当 decision が「保留」「却下」 → そのまま表示
  - 該当 decision なし → 「未反映」
  - 該当 decision が複数 → 「処理済 (N件)」

Usage:
  python scripts/audit_corrections_status.py <06_担当者編集_v2_*.xlsx> [--write]

--write 指定で wb の Sheet 2b を更新 (read-only 解除して書き込み)。
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import PatternFill, Font

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "permit_tracker.db"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("workbook", type=Path)
    ap.add_argument("--write", action="store_true",
                    help="Sheet 2b に書き戻す (read-only を一時解除)")
    args = ap.parse_args()

    if not args.workbook.exists():
        print(f"ERROR: {args.workbook} not found", file=sys.stderr)
        sys.exit(1)

    print(f"=== Phase R-G-Audit: 修正依頼処理状況の audit ===")
    print(f"  workbook: {args.workbook.name}")
    print(f"  mode: {'WRITE' if args.write else 'DRY-RUN'}")

    wb = openpyxl.load_workbook(args.workbook)
    if "2b_全修正依頼トラッキング" not in wb.sheetnames:
        print(f"ERROR: Sheet 2b not found in workbook", file=sys.stderr)
        sys.exit(1)

    # workflow_id 取得
    meta_ws = wb["0_workflow_meta"]
    wf_id = None
    for r in range(1, meta_ws.max_row + 1):
        if meta_ws.cell(r, 1).value == "workflow_id":
            wf_id = meta_ws.cell(r, 2).value
            break
    if not wf_id:
        print(f"ERROR: workflow_id not in meta", file=sys.stderr)
        sys.exit(1)

    # decisions_log から本 workflow の全 decision を取得
    conn = sqlite3.connect(DB_PATH)
    decisions_by_cid = {}
    for row in conn.execute(
        "SELECT decision_id, company_id, document_type, decision_kind, "
        "       decision_value, applied_at "
        "FROM decisions_log WHERE workflow_id=?", (wf_id,)
    ):
        cid = row[1]
        decisions_by_cid.setdefault(cid, []).append({
            "decision_id": row[0], "doc": row[2], "kind": row[3],
            "value": row[4], "applied_at": row[5],
        })
    conn.close()
    print(f"  workflow_id: {wf_id[:8]}...  decisions: {sum(len(v) for v in decisions_by_cid.values())}")

    # Sheet 2b を audit
    ws = wb["2b_全修正依頼トラッキング"]
    headers = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
    cid_col = headers.index("company_id") + 1
    cat_col = headers.index("自動分類カテゴリ") + 1
    status_col = headers.index("想定処理状況 (audit 後更新)") + 1
    note_col = headers.index("備考 (apply 後 audit が更新)") + 1

    n_processed = 0
    n_unprocessed = 0
    n_partial = 0
    for r in range(2, ws.max_row + 1):
        cid = ws.cell(r, cid_col).value
        cats = ws.cell(r, cat_col).value or ""
        if not cid:
            continue
        decisions = decisions_by_cid.get(cid, [])
        if not decisions:
            status = "未反映"
            note = "decisions_log に該当なし"
            n_unprocessed += 1
        else:
            n_applied = sum(1 for d in decisions if d["value"] not in ("保留", "却下"))
            n_held = sum(1 for d in decisions if d["value"] == "保留")
            n_rejected = sum(1 for d in decisions if d["value"] == "却下")
            if n_applied > 0 and n_held == 0 and n_rejected == 0:
                status = f"処理済 ({n_applied}件)"
                note = " / ".join(set(d["kind"] for d in decisions))
                n_processed += 1
            elif n_applied > 0:
                status = f"部分処理 (反映{n_applied}/保留{n_held}/却下{n_rejected})"
                note = " / ".join(set(d["kind"] for d in decisions))
                n_partial += 1
            elif n_rejected > 0:
                status = f"却下 ({n_rejected}件)"
                note = "藤田指摘を却下"
                n_partial += 1
            else:
                status = f"保留 ({n_held}件)"
                note = "保留中"
                n_partial += 1

        if args.write:
            ws.protection.sheet = False
            ws.cell(r, status_col).value = status
            ws.cell(r, note_col).value = note
            # ハイライト
            color = {"処理済": "C6EFCE", "未反映": "FFC7CE",
                     "部分処理": "FFEB9C"}.get(status.split(" ")[0], "FFFFFF")
            ws.cell(r, status_col).fill = PatternFill("solid", fgColor=color)

    if args.write:
        ws.protection.sheet = True
        ws.protection.password = "fde_view_lock"
        wb.save(args.workbook)
        print(f"  Sheet 2b 更新済 → {args.workbook.name}")

    print(f"\n=== Audit 結果 ===")
    print(f"  処理済:    {n_processed}")
    print(f"  部分処理:   {n_partial}")
    print(f"  未反映:    {n_unprocessed}")


if __name__ == "__main__":
    main()
