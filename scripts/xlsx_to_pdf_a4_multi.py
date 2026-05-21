"""
複数社対応: xlsx をシート別に A4 縦で PDF 化、労働者名簿系シートのみ A4 横。
xlsx の PrintArea/Fit/改ページ設定はそのまま尊重 (Orientation+PaperSize のみ override)。

Usage:
  python scripts/xlsx_to_pdf_a4_multi.py --company-ids C0071,C0043,C0062,C0118 --execute
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
ORIGINALS = PROJECT / "data" / "originals"
sys.stdout.reconfigure(encoding="utf-8")




def safe_name(name: str) -> str:
    s = re.sub(r'[\\/:*?"<>|]', "_", name)
    return s.strip(". ")[:80] or "sheet"


def find_last_data_cell(sheet) -> tuple[int, int]:
    last_row = 0
    last_col = 0
    try:
        found_row = sheet.Cells.Find(
            What="*", After=sheet.Cells(1, 1),
            LookIn=-4163, LookAt=2, SearchOrder=1, SearchDirection=2, MatchCase=False,
        )
        if found_row is not None:
            last_row = int(found_row.Row)
        found_col = sheet.Cells.Find(
            What="*", After=sheet.Cells(1, 1),
            LookIn=-4163, LookAt=2, SearchOrder=2, SearchDirection=2, MatchCase=False,
        )
        if found_col is not None:
            last_col = int(found_col.Column)
    except Exception:
        pass
    if last_row == 0 or last_col == 0:
        try:
            used = sheet.UsedRange
            last_row = used.Rows.Count
            last_col = used.Columns.Count
        except Exception:
            last_row = last_col = 0
    return (last_row, last_col)


def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def export_xlsx(xlsx_path: Path, conversion_run_id: str, dry_run: bool) -> list[dict]:
    import pythoncom
    from win32com import client
    pythoncom.CoInitialize()
    excel = None
    wb = None
    results = []
    try:
        try:
            excel = client.DispatchEx("Excel.Application")
        except Exception:
            excel = client.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        try: excel.ScreenUpdating = False
        except Exception: pass
        wb = excel.Workbooks.Open(str(xlsx_path.resolve()))
        n_sheets = wb.Sheets.Count
        print(f"[{xlsx_path.name}] sheets={n_sheets}")
        for i in range(1, n_sheets + 1):
            sheet = wb.Sheets(i)
            sheet_name = sheet.Name
            safe_sheet = safe_name(sheet_name)
            last_row, last_col = find_last_data_cell(sheet)
            print(f"  s{i:02d} '{sheet_name}' last=R{last_row}C{last_col}")
            if last_row < 4 or last_col < 2:
                print(f"     SKIP (empty)")
                results.append({"index": i, "sheet": sheet_name, "skipped": "empty"})
                continue

            # ユーザー指示:
            #  - 全シート A4 用紙、xlsx の印刷設定 (PrintArea / Fit / 改ページ) はそのまま
            #  - 労働者名簿シートのみ A4 横 (Landscape)
            #  - その他は A4 縦 (Portrait)
            is_worker_list = ("労働者名簿" in sheet_name) or (i == n_sheets)
            try:
                ps = sheet.PageSetup
                if is_worker_list:
                    ps.Orientation = 2  # xlLandscape
                    ps.PaperSize = 9    # xlPaperA4
                    print(f"     PageSetup: A4 Landscape (労働者名簿)")
                else:
                    ps.Orientation = 1  # xlPortrait
                    ps.PaperSize = 9    # xlPaperA4
                    print(f"     PageSetup: A4 Portrait")
            except Exception as e:
                print(f"     WARN PageSetup: {e}")

            out_pdf = xlsx_path.parent / f"{xlsx_path.stem}__s{i:02d}_{safe_sheet}_a4portrait.pdf"
            if dry_run:
                print(f"     [dry] → {out_pdf.name}")
                results.append({"index": i, "sheet": sheet_name, "path": str(out_pdf), "dry_run": True})
                continue

            try:
                sheet.ExportAsFixedFormat(0, str(out_pdf.resolve()))
                # 検証
                import pymupdf as fitz
                doc = fitz.open(out_pdf)
                npg = len(doc)
                p0_text = len((doc[0].get_text() or "").strip()) if npg else 0
                doc.close()
                size = out_pdf.stat().st_size
                ok = npg > 0 and (p0_text > 5 or size > 30000)
                marker = "OK" if ok else "FAIL"
                print(f"     {marker} → {out_pdf.name}  ({size}B, {npg}p, p0_text={p0_text})")
                results.append({
                    "index": i, "sheet": sheet_name, "path": str(out_pdf),
                    "ok": ok, "size": size, "pages": npg, "p0_text": p0_text,
                })
            except Exception as e:
                print(f"     FAIL export: {e}")
                results.append({"index": i, "sheet": sheet_name, "ok": False, "error": str(e)})
    finally:
        try:
            if wb: wb.Close(False)
        except Exception: pass
        try:
            if excel: excel.Quit()
        except Exception: pass
        try: pythoncom.CoUninitialize()
        except Exception: pass
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--company-ids", required=True, help="comma-separated cids: C0071,C0043,...")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()
    if not args.dry_run and not args.execute:
        print("ERROR: --dry-run か --execute", file=sys.stderr); sys.exit(1)
    cids = [c.strip() for c in args.company_ids.split(",") if c.strip()]
    conversion_run_id = f"xlsx2pdf_a4_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"run_id: {conversion_run_id}")
    all_results = []
    for cid in cids:
        xlsx_paths = list((ORIGINALS).glob(f"{cid}_*/**/*.xlsx"))
        if not xlsx_paths:
            print(f"  [{cid}] xlsx not found, skip"); continue
        for xp in xlsx_paths:
            res = export_xlsx(xp, conversion_run_id, args.dry_run)
            for r in res:
                r["company_id"] = cid
                r["conversion_run_id"] = conversion_run_id
                r["xlsx"] = xp.name
            all_results += res
    if not args.dry_run:
        import json
        log = PROJECT / "data" / f"xlsx_conversion_{conversion_run_id}.json"
        log.write_text(json.dumps(all_results, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"ログ: {log.name}")
        print(f"次: python scripts/register_replacement_pdfs.py --run-id {conversion_run_id} --execute")


if __name__ == "__main__":
    main()
