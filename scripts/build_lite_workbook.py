"""
Phase R-E-Lite: 藤田送付用 Lite ワークブック生成 (GPT-5.5 Issue 7 反映)。

Full workbook (06_担当者編集_v2_*.xlsx) から、藤田が必須判断する 4 シートのみ抽出。
藤田負担を最小化し、必須確認領域を明確化。

抽出シート:
  0. workflow_meta (Full と同じ workflow_id を継承)
  1. 突合せ確認    (Full Sheet 1 そのまま)
  2. 受領状況編集  (Full Sheet 2 そのまま)
  3. 期限管理      (Full Sheet 3 そのまま)

除外シート:
  4. 新規追加済確認 → Phase R-B promote 後の自動取り込みで完結
  5. 既存対象外化候補 → 当方判定で済むケースが多い、必要なら別途確認
  6. 揃いvs有効 並列 → 参照のみ、判断不要
  7. ページ別分類確認 → 当方目視で完結 (Phase R-C-2)
  8. 会社基本情報修正 → DB 反映可能項目のみ Lite に含める or 当方判断
  9. 新規受領ファイル追加 → Lite に含める (新規 PDF 提供受付)

藤田の作業時間目安: 30分〜1時間 (Full は 8〜10 時間)

Usage:
  python scripts/build_lite_workbook.py <Full wb path>
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Protection
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.utils import get_column_letter
from copy import copy

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
LITE_SHEETS = ["0_workflow_meta", "1_突合せ確認", "2_受領状況編集",
                 "3_期限管理",
                 "9_新規受領ファイル追加", "99_lookup_hidden"]
# GPT-5.5 v4 Issue 5: Sheet 2b は内部監査専用、Lite に含めない (責任境界破壊防止)


def copy_sheet(src_ws, dst_wb, new_name: str):
    """シートを完全コピー (値、書式、ドロップダウン、保護)"""
    dst_ws = dst_wb.create_sheet(new_name)
    # values + formats
    for row in src_ws.iter_rows():
        for cell in row:
            dst_cell = dst_ws.cell(cell.row, cell.column, cell.value)
            if cell.has_style:
                dst_cell.font = copy(cell.font)
                dst_cell.fill = copy(cell.fill)
                dst_cell.border = copy(cell.border)
                dst_cell.alignment = copy(cell.alignment)
                dst_cell.protection = copy(cell.protection)
                dst_cell.number_format = cell.number_format
    # column widths
    for col_letter, col_dim in src_ws.column_dimensions.items():
        dst_ws.column_dimensions[col_letter].width = col_dim.width
        dst_ws.column_dimensions[col_letter].hidden = col_dim.hidden
    # freeze panes
    dst_ws.freeze_panes = src_ws.freeze_panes
    # protection
    dst_ws.protection.sheet = src_ws.protection.sheet
    if src_ws.protection.password:
        dst_ws.protection.password = "fde_lite_lock"
    # data validation (dropdown)
    for dv in src_ws.data_validations.dataValidation:
        new_dv = DataValidation(
            type=dv.type, formula1=dv.formula1, allow_blank=dv.allowBlank,
            showDropDown=False,
        )
        new_dv.error = dv.error
        new_dv.errorTitle = dv.errorTitle
        dst_ws.add_data_validation(new_dv)
        for sqr in dv.sqref.ranges:
            new_dv.add(str(sqr))
    # hidden state
    dst_ws.sheet_state = src_ws.sheet_state


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("full_wb", type=Path, help="Full workbook (06_担当者編集_v2_*.xlsx)")
    ap.add_argument("--ts", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    args = ap.parse_args()

    if not args.full_wb.exists():
        print(f"ERROR: {args.full_wb} not found", file=sys.stderr)
        sys.exit(1)

    print(f"=== Phase R-E-Lite: 藤田向け Lite workbook 生成 ===")
    print(f"  TS: {args.ts}")
    print(f"  source: {args.full_wb.name}")

    src_wb = openpyxl.load_workbook(args.full_wb)
    dst_wb = openpyxl.Workbook()
    dst_wb.remove(dst_wb.active)

    n_copied = 0
    for sheet_name in LITE_SHEETS:
        if sheet_name not in src_wb.sheetnames:
            print(f"  [SKIP] sheet not found: {sheet_name}")
            continue
        copy_sheet(src_wb[sheet_name], dst_wb, sheet_name)
        n_copied += 1
        print(f"  copied: {sheet_name}")

    # workflow_meta に Lite フラグ追加
    if "0_workflow_meta" in dst_wb.sheetnames:
        ws = dst_wb["0_workflow_meta"]
        # 保護を一時解除
        ws.protection.sheet = False
        next_row = ws.max_row + 1
        ws.cell(next_row, 1, "lite_mode")
        ws.cell(next_row, 2, "true (Sheets 4-8 are in Full workbook only)")
        ws.cell(next_row, 1).fill = PatternFill("solid", fgColor="BDD7EE")
        ws.cell(next_row, 1).font = Font(bold=True)
        ws.protection.sheet = True
        ws.protection.password = "fde_meta_lock"

    # 出力
    src_name = args.full_wb.stem  # "06_担当者編集_v2_<TS>_<wf>"
    wf_short = src_name.split("_")[-1]   # "<wf>"
    out = PROJECT / "output" / "FDE_MANAGED" / f"06_藤田向けLite_{args.ts}_{wf_short}.xlsx"
    out.parent.mkdir(parents=True, exist_ok=True)
    dst_wb.save(out)

    print(f"\n=== Output ===")
    print(f"  {out.relative_to(PROJECT)}")
    print(f"  copied sheets: {n_copied}")
    print()
    print("藤田送付前チェック:")
    print(f"  1. ファイル名 (workflow_id) を変更しないこと")
    print(f"  2. シート 0 は機械保護、シート 1〜3+9 のみ編集可能")
    print(f"  3. 黄色背景の列だけ編集、灰色 (機械列) は触らない")
    print(f"  4. 空欄返送 = 現状維持 として扱われる")


if __name__ == "__main__":
    main()
