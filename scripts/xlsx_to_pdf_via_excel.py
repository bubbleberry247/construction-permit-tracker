"""
xlsx を Excel COM 経由で PDF 化する。
レイアウトを保持する。

Usage:
  python scripts/xlsx_to_pdf_via_excel.py <input.xlsx> [<output.pdf>]
  python scripts/xlsx_to_pdf_via_excel.py --company-id C0149   # 自動検出
  python scripts/xlsx_to_pdf_via_excel.py --all-broken         # 全社の xlsx 取込済 → PDF 化
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
sys.stdout.reconfigure(encoding="utf-8")


def xlsx_to_pdf(xlsx_path: Path, pdf_path: Path) -> bool:
    """Excel COM で xlsx を pdf 出力。"""
    import pythoncom
    from win32com import client

    pythoncom.CoInitialize()
    excel = None
    wb = None
    try:
        excel = client.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        wb = excel.Workbooks.Open(str(xlsx_path.resolve()))
        # PDF: xlTypePDF=0
        wb.ExportAsFixedFormat(0, str(pdf_path.resolve()))
        return True
    finally:
        if wb is not None:
            wb.Close(False)
        if excel is not None:
            excel.Quit()
        pythoncom.CoUninitialize()


def find_xlsx_for_company(cid: str) -> list[Path]:
    return list((PROJECT / "data/originals").glob(f"{cid}_*/**/*.xlsx"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input", nargs="?")
    ap.add_argument("output", nargs="?")
    ap.add_argument("--company-id")
    ap.add_argument("--all-broken", action="store_true",
                    help="DB の xlsx ファイル全部を PDF 化")
    args = ap.parse_args()

    targets: list[tuple[Path, Path]] = []

    if args.all_broken:
        conn = sqlite3.connect(str(DB))
        for r in conn.execute(
            "SELECT DISTINCT company_id, file_name FROM pages WHERE file_name LIKE '%.xlsx'"
        ):
            cid, fn = r[0], r[1]
            for xp in (PROJECT / "data/originals").glob(f"{cid}_*/**/{fn}"):
                pdf = xp.with_suffix(".pdf")
                targets.append((xp, pdf))
    elif args.company_id:
        for xp in find_xlsx_for_company(args.company_id):
            pdf = xp.with_suffix(".pdf")
            targets.append((xp, pdf))
    elif args.input:
        ip = Path(args.input)
        op = Path(args.output) if args.output else ip.with_suffix(".pdf")
        targets.append((ip, op))
    else:
        ap.print_help()
        return

    print(f"対象: {len(targets)} ファイル", file=sys.stderr)
    ok, ng = 0, 0
    for xp, pp in targets:
        if pp.exists():
            print(f"  SKIP (already): {pp.name}", file=sys.stderr)
            continue
        try:
            xlsx_to_pdf(xp, pp)
            print(f"  OK: {xp.name} -> {pp.name}", file=sys.stderr)
            ok += 1
        except Exception as e:
            print(f"  NG: {xp.name}: {e}", file=sys.stderr)
            ng += 1
    print(f"\nresult: ok={ok}, ng={ng}, skip={len(targets) - ok - ng}")


if __name__ == "__main__":
    main()
