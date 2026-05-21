"""
生成された PDF の空白ページを自動削除する。

判定:
  - テキスト長 < 30 字 AND 白ピクセル比 >= 0.99 → 空白扱い

Usage:
  python scripts/trim_blank_pages_in_pdfs.py --run-id xlsx2pdf_<TS> --dry-run
  python scripts/trim_blank_pages_in_pdfs.py --run-id xlsx2pdf_<TS> --execute
  python scripts/trim_blank_pages_in_pdfs.py --pdf <path> --execute
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import pymupdf as fitz

PROJECT = Path(__file__).resolve().parent.parent
sys.stdout.reconfigure(encoding="utf-8")

WHITE_RATIO_THRESHOLD = 0.99
TEXT_LEN_THRESHOLD = 30
DETECTION_DPI = 100


def is_blank_page(doc: fitz.Document, page_no: int) -> tuple[bool, str]:
    """0-indexed page_no の空白判定。

    判定基準:
      A. white_ratio >= 0.99 → blank (視覚的に空白、テキスト量無関係)
         footer 等のみ残るページもこれで検知
      B. white_ratio >= 0.95 AND text < 30 → blank (極小内容)
    """
    page = doc[page_no]
    text = (page.get_text() or "").strip()
    mat = fitz.Matrix(DETECTION_DPI / 72, DETECTION_DPI / 72)
    pix = page.get_pixmap(matrix=mat, colorspace=fitz.csGRAY)
    samples = pix.samples
    if not samples:
        return False, "no_pixels"
    n_white = sum(1 for b in samples if b >= 240)
    ratio = n_white / len(samples)

    if ratio >= WHITE_RATIO_THRESHOLD:
        return True, f"blank (white={ratio:.4f}, text={len(text)})"
    if ratio >= 0.95 and len(text) < TEXT_LEN_THRESHOLD:
        return True, f"near-blank (white={ratio:.4f}, text={len(text)})"
    return False, f"text={len(text)} white={ratio:.4f}"


def trim_pdf(pdf: Path, dry_run: bool) -> dict:
    """PDF の空白ページを削除。元ファイルを上書き。"""
    try:
        doc = fitz.open(pdf)
    except Exception as e:
        return {"path": str(pdf), "error": f"open: {e}"}

    n_orig = len(doc)
    blank_pages: list[int] = []
    reasons: list[str] = []
    for i in range(n_orig):
        is_blank, reason = is_blank_page(doc, i)
        reasons.append(reason)
        if is_blank:
            blank_pages.append(i)

    if not blank_pages:
        doc.close()
        return {"path": str(pdf), "n_orig": n_orig, "removed": 0, "kept": n_orig, "reasons": reasons}

    if dry_run:
        doc.close()
        return {
            "path": str(pdf), "n_orig": n_orig,
            "removed_pages": [p + 1 for p in blank_pages],
            "removed": len(blank_pages),
            "kept": n_orig - len(blank_pages),
            "reasons": reasons,
            "dry_run": True,
        }

    # 削除 (降順で削除しないとインデックスがずれる)
    for p in sorted(blank_pages, reverse=True):
        doc.delete_page(p)

    # 上書き保存 (clean=True で最適化)
    tmp = pdf.with_suffix(".pdf.tmp")
    doc.save(str(tmp), garbage=4, deflate=True)
    doc.close()
    tmp.replace(pdf)

    return {
        "path": str(pdf), "n_orig": n_orig,
        "removed_pages": [p + 1 for p in blank_pages],
        "removed": len(blank_pages),
        "kept": n_orig - len(blank_pages),
        "reasons": reasons,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", help="xlsx_conversion_<run_id>.json の run_id")
    ap.add_argument("--pdf", help="単一 PDF を処理")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    if not args.dry_run and not args.execute:
        print("ERROR: --dry-run か --execute", file=sys.stderr)
        sys.exit(1)

    pdfs: list[Path] = []
    if args.pdf:
        pdfs = [Path(args.pdf)]
    elif args.run_id:
        log_path = PROJECT / "data" / f"xlsx_conversion_{args.run_id}.json"
        log = json.loads(log_path.read_text(encoding="utf-8"))
        for r in log:
            if r.get("ok") and r.get("path"):
                pdfs.append(Path(r["path"]))
    else:
        print("ERROR: --run-id か --pdf", file=sys.stderr)
        sys.exit(1)

    print(f"対象 PDF: {len(pdfs)}", file=sys.stderr)

    total_orig = 0
    total_removed = 0
    total_kept = 0
    n_changed = 0
    results = []
    for pdf in pdfs:
        if not pdf.exists():
            continue
        r = trim_pdf(pdf, args.dry_run)
        results.append(r)
        if "error" in r:
            print(f"  ERR {pdf.name}: {r['error']}", file=sys.stderr)
            continue
        total_orig += r["n_orig"]
        total_removed += r["removed"]
        total_kept += r["kept"]
        if r["removed"] > 0:
            n_changed += 1
            print(f"  {pdf.name}: {r['n_orig']}p → {r['kept']}p (削除 {r['removed_pages']})", file=sys.stderr)

    print(f"\n=== サマリ ===")
    print(f"  対象 PDF:   {len(results)}")
    print(f"  変更あり:   {n_changed}")
    print(f"  元 ページ:  {total_orig}")
    print(f"  削除:       {total_removed}")
    print(f"  残ページ:   {total_kept}")

    if args.dry_run:
        print(f"\n[dry-run] --execute で実行")


if __name__ == "__main__":
    main()
