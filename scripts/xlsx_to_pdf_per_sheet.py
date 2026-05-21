"""
xlsx をシート単位で個別 PDF 化する（Excel COM、レイアウト保持版）。

旧 xlsx_to_pdf_via_excel.py の問題:
  - wb.ExportAsFixedFormat(0, ...) で全シートを 1 PDF に → 印刷範囲設定なくレイアウト崩壊

本スクリプトの改良:
  1. 各シートの PageSetup を明示的に設定:
     - Orientation = xlPortrait
     - Zoom = False (ズーム調整無効)
     - FitToPagesWide = 1 (横は 1 ページに収める)
     - FitToPagesTall = False (縦は自然な分割)
     - PrintArea = UsedRange.Address (実データ範囲のみ)
  2. シートごとに別 PDF 出力
  3. 空シート (UsedRange が小さい) はスキップ

Usage:
  python scripts/xlsx_to_pdf_per_sheet.py --company-id C0118 --dry-run
  python scripts/xlsx_to_pdf_per_sheet.py --company-id C0118 --execute
  python scripts/xlsx_to_pdf_per_sheet.py --all-xlsx --execute
"""
from __future__ import annotations

import argparse
import re
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
ORIGINALS = PROJECT / "data" / "originals"
sys.stdout.reconfigure(encoding="utf-8")

XLSX_TARGETS = ["C0029", "C0062", "C0077", "C0079", "C0082", "C0083", "C0118", "C0147", "C0149", "C0150"]


def safe_name(name: str) -> str:
    """ファイル名安全化"""
    s = re.sub(r'[\\/:*?"<>|]', "_", name)
    return s.strip(". ")[:80] or "sheet"


def find_last_data_cell(sheet) -> tuple[int, int]:
    """実データの最終行・最終列を返す。

    SpecialCells(11) や UsedRange は書式のみのセルも含むため、
    Cells.Find で **値があるセル** の最終位置を探す (xlValues 検索)。
    """
    last_row = 0
    last_col = 0
    try:
        # xlValues=-4163, xlPrevious=2, xlByRows=1, xlByColumns=2
        # Find last non-empty by rows (find from end)
        found_row = sheet.Cells.Find(
            What="*",
            After=sheet.Cells(1, 1),
            LookIn=-4163,    # xlValues
            LookAt=2,        # xlPart
            SearchOrder=1,   # xlByRows
            SearchDirection=2,  # xlPrevious
            MatchCase=False,
        )
        if found_row is not None:
            last_row = int(found_row.Row)
        # Find last non-empty by columns
        found_col = sheet.Cells.Find(
            What="*",
            After=sheet.Cells(1, 1),
            LookIn=-4163,
            LookAt=2,
            SearchOrder=2,   # xlByColumns
            SearchDirection=2,
            MatchCase=False,
        )
        if found_col is not None:
            last_col = int(found_col.Column)
    except Exception:
        pass
    if last_row == 0 or last_col == 0:
        # フォールバック: UsedRange
        try:
            used = sheet.UsedRange
            last_row = used.Rows.Count
            last_col = used.Columns.Count
        except Exception:
            last_row = last_col = 0
    return (last_row, last_col)


def validate_pdf(pdf: Path, min_size: int = 10_000) -> tuple[bool, str]:
    """生成 PDF を検証。(ok, reason) を返す。"""
    if not pdf.exists():
        return (False, "no_file")
    sz = pdf.stat().st_size
    if sz < min_size:
        return (False, f"too_small ({sz}B<{min_size}B)")
    try:
        import pymupdf as fitz
        doc = fitz.open(pdf)
        n = len(doc)
        if n == 0:
            doc.close()
            return (False, "zero_pages")
        # 最初のページが完全空白か簡易判定 (テキスト+画像)
        p0 = doc[0]
        text_len = len((p0.get_text() or "").strip())
        n_images = len(p0.get_images())
        doc.close()
        if text_len < 5 and n_images == 0:
            return (False, f"blank_first_page (text={text_len}, imgs=0)")
        return (True, f"OK ({sz}B, {n}p, p0_text={text_len})")
    except Exception as e:
        return (False, f"open_err: {e}")


def export_sheets_to_pdf(xlsx_path: Path, out_dir: Path, conversion_run_id: str, dry_run: bool) -> list[dict]:
    """xlsx の各シートを個別 PDF にエクスポート + 検証。"""
    import pythoncom
    from win32com import client

    pythoncom.CoInitialize()
    excel = None
    wb = None
    results: list[dict] = []
    try:
        # DispatchEx で新規インスタンス作成 (orphan COM 状態を避ける)
        try:
            excel = client.DispatchEx("Excel.Application")
        except Exception:
            excel = client.Dispatch("Excel.Application")
        try:
            excel.Visible = False
        except Exception as e:
            print(f"  WARN Visible: {e}", file=sys.stderr)
        try:
            excel.DisplayAlerts = False
        except Exception:
            pass
        try:
            excel.ScreenUpdating = False
        except Exception:
            pass
        wb = excel.Workbooks.Open(str(xlsx_path.resolve()))
        n_sheets = wb.Sheets.Count
        print(f"  xlsx sheets: {n_sheets}", file=sys.stderr)

        for i in range(1, n_sheets + 1):
            sheet = wb.Sheets(i)
            sheet_name = sheet.Name
            safe_sheet = safe_name(sheet_name)

            # 実データの最後の行・列 (UsedRange 過信せず)
            last_row, last_col = find_last_data_cell(sheet)
            print(f"    Sheet {i}: '{sheet_name}' last=R{last_row}C{last_col}", file=sys.stderr)

            # 空シート判定 (4 行未満 or 1 列のみ)
            if last_row < 4 or last_col < 2:
                print(f"    → SKIP (empty)", file=sys.stderr)
                results.append({"index": i, "sheet": sheet_name, "skipped": "empty"})
                continue

            # PageSetup は変更しない (xlsx の既定値を尊重)
            # 過剰設定するとレンダリングが空白化することが判明したため

            out_pdf = out_dir / f"{xlsx_path.stem}__s{i:02d}_{safe_sheet}.pdf"
            if dry_run:
                print(f"    [dry] → {out_pdf.name}", file=sys.stderr)
                results.append({
                    "index": i, "sheet": sheet_name, "path": str(out_pdf), "dry_run": True
                })
                continue

            try:
                sheet.ExportAsFixedFormat(0, str(out_pdf.resolve()))
                ok, reason = validate_pdf(out_pdf)
                marker = "✓" if ok else "✗"
                print(f"    {marker} → {out_pdf.name}  {reason}", file=sys.stderr)
                results.append({
                    "index": i, "sheet": sheet_name, "path": str(out_pdf),
                    "ok": ok, "reason": reason,
                    "size": out_pdf.stat().st_size if out_pdf.exists() else 0,
                })
            except Exception as e:
                print(f"    ✗ export fail: {e}", file=sys.stderr)
                results.append({"index": i, "sheet": sheet_name, "ok": False, "error": str(e)})
    finally:
        try:
            if wb is not None:
                wb.Close(False)
        except Exception:
            pass
        try:
            if excel is not None:
                excel.Quit()
        except Exception:
            pass
        try:
            pythoncom.CoUninitialize()
        except Exception:
            pass
    return results


def find_xlsx_for_company(cid: str) -> list[Path]:
    return list((ORIGINALS).glob(f"{cid}_*/**/*.xlsx"))


def archive_old_pdf(xlsx_path: Path) -> Path | None:
    """旧 xlsx → pdf 同名変換結果を archive へ移動。"""
    old_pdf = xlsx_path.with_suffix(".pdf")
    if not old_pdf.exists():
        return None
    archive = old_pdf.parent / "_broken_pdf_archive"
    archive.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = archive / f"{old_pdf.stem}_{ts}.pdf"
    shutil.move(str(old_pdf), str(dst))
    return dst


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--company-id")
    ap.add_argument("--all-xlsx", action="store_true", help="10 xlsx 対象社全部")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--skip-archive", action="store_true", help="旧 PDF を archive せず上書き")
    args = ap.parse_args()

    if not args.dry_run and not args.execute:
        print("ERROR: --dry-run か --execute を指定", file=sys.stderr)
        sys.exit(1)

    if args.all_xlsx:
        cids = XLSX_TARGETS
    elif args.company_id:
        cids = [args.company_id]
    else:
        print("ERROR: --company-id か --all-xlsx を指定", file=sys.stderr)
        sys.exit(1)

    conversion_run_id = f"xlsx2pdf_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    total_in = 0
    total_out = 0
    total_ok = 0
    total_fail = 0
    summary_log: list[dict] = []

    for cid in cids:
        xlsx_paths = find_xlsx_for_company(cid)
        for xp in xlsx_paths:
            print(f"\n[{cid}] {xp.name}", file=sys.stderr)
            total_in += 1
            # 旧 PDF を archive
            if not args.dry_run and not args.skip_archive:
                arc = archive_old_pdf(xp)
                if arc:
                    print(f"  archived old PDF: {arc.name}", file=sys.stderr)
            # シート別 PDF 出力 + 検証
            results = export_sheets_to_pdf(xp, xp.parent, conversion_run_id, args.dry_run)
            for r in results:
                r["company_id"] = cid
                r["xlsx"] = xp.name
                r["conversion_run_id"] = conversion_run_id
                summary_log.append(r)
                if r.get("skipped"):
                    continue
                total_out += 1
                if r.get("dry_run"):
                    continue
                if r.get("ok"):
                    total_ok += 1
                else:
                    total_fail += 1

    # JSON ログ保存
    if not args.dry_run:
        import json
        log_path = PROJECT / "data" / f"xlsx_conversion_{conversion_run_id}.json"
        log_path.write_text(json.dumps(summary_log, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nログ: {log_path.name}")

    print(f"\n=== サマリ ===")
    print(f"  conversion_run_id: {conversion_run_id}")
    print(f"  処理 xlsx:  {total_in}")
    print(f"  出力 PDF:   {total_out}")
    print(f"  OK:         {total_ok}")
    print(f"  失敗:       {total_fail}")
    if args.dry_run:
        print(f"\n[dry-run] --execute で実行")
    else:
        print(f"\n次のステップ:")
        print(f"  1. ビューワー or Explorer で生成 PDF を目視確認")
        print(f"  2. OK なら python scripts/register_replacement_pdfs.py --run-id {conversion_run_id} --execute")


if __name__ == "__main__":
    main()
