"""
客先編集領域 CLIENT_LOCAL/notes.xlsx の空テンプレートを生成。

初回出向時に FDE が手動で配置するもの。
納品 zip には含めない（更新時に上書きしないため）。

出力:
  output/CLIENT_LOCAL_template/CLIENT_LOCAL/notes.xlsx
  output/CLIENT_LOCAL_template/CLIENT_LOCAL/backup_previous/.gitkeep
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
TEMPLATE_ROOT = PROJECT / "output" / "CLIENT_LOCAL_template" / "CLIENT_LOCAL"

HEADER_FILL = PatternFill(start_color="8B4513", end_color="8B4513", fill_type="solid")
HEADER_FONT = Font(color="FFFFFF", bold=True)
INFO_FILL = PatternFill(start_color="FFF8E1", end_color="FFF8E1", fill_type="solid")


def main():
    TEMPLATE_ROOT.mkdir(parents=True, exist_ok=True)
    backup_dir = TEMPLATE_ROOT / "backup_previous"
    backup_dir.mkdir(exist_ok=True)
    (backup_dir / "README.txt").write_text(
        "このフォルダには、更新時に退避した旧 FDE_MANAGED/ を配置します。\n"
        "詳細は README_運用規程.txt を参照。\n",
        encoding="utf-8"
    )

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    companies = [dict(r) for r in conn.execute(
        "SELECT company_id, official_name FROM companies WHERE status='ACTIVE' ORDER BY company_id"
    )]

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Notes"

    # 注意書き行
    ws.append(["⚠ この Excel は客先専用のメモ帳です。FDE_MANAGED/ 内のファイルは触らないでください。"])
    ws.cell(1, 1).fill = INFO_FILL
    ws.cell(1, 1).font = Font(bold=True, color="8B4513")
    ws.merge_cells("A1:E1")

    ws.append([])  # 空行

    headers = ["company_id", "会社名", "担当者メモ", "判断", "最終更新日"]
    ws.append(headers)
    for col in range(1, len(headers) + 1):
        cell = ws.cell(3, col)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center")

    for c in companies:
        ws.append([c["company_id"], c["official_name"], "", "", ""])

    for i, w in enumerate([10, 30, 40, 12, 14], 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    ws.freeze_panes = "A4"
    ws.auto_filter.ref = f"A3:{get_column_letter(len(headers))}{ws.max_row}"

    out_path = TEMPLATE_ROOT / "notes.xlsx"
    wb.save(out_path)
    print(f"✓ {out_path}")
    print(f"  ACTIVE 会社 {len(companies)} 行の空テンプレート")
    print(f"\n初回出向時の配置手順:")
    print(f"  1. 客先 PC に '協力会社書類管理' フォルダを作成")
    print(f"  2. この CLIENT_LOCAL/ を '協力会社書類管理/' 直下に配置")
    print(f"  3. 納品 zip を別途解凍して FDE_MANAGED/ を配置")


if __name__ == "__main__":
    main()
