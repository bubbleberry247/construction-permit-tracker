"""
「その他/不明」分類のページのうち、テキスト/画像が空 (or 極小) のページを「空白」に分類。

判定 (GPT-5.5 推奨):
  1. PyMuPDF text < 10 文字 AND
  2. ページを 100dpi で画像化して white pixel ratio >= 0.99 (= 黒インクほぼなし)
  → 「空白」

PDF 合体時に末尾や境界に空白ページが入るケースを除外。
OCR 対象から外せる = Azure DI コスト節約。

Usage:
  python scripts/classify_blank_pages.py --dry-run
  python scripts/classify_blank_pages.py --execute
  python scripts/classify_blank_pages.py --execute --threshold 0.985
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path

import pymupdf as fitz

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
sys.stdout.reconfigure(encoding="utf-8")

DEFAULT_THRESHOLD = 0.995
TEXT_LEN_THRESHOLD = 10
DETECTION_DPI = 100


def find_pdf(file_name: str, cid: str) -> Path | None:
    for cdir in (PROJECT / "data/originals").glob(f"{cid}_*"):
        if cdir.is_dir():
            for p in cdir.rglob(file_name):
                if p.is_file():
                    return p
            for p in cdir.rglob(f"*{file_name}"):
                if p.is_file():
                    return p
    return None


def is_blank_page(pdf: Path, page_no: int, threshold: float) -> tuple[bool, str]:
    try:
        doc = fitz.open(pdf)
        if page_no > len(doc):
            doc.close()
            return False, "page_out_of_range"
        page = doc[page_no - 1]
        text = (page.get_text() or "").strip()
        text_len = len(text)
        if text_len >= TEXT_LEN_THRESHOLD:
            doc.close()
            return False, f"has_text({text_len})"

        mat = fitz.Matrix(DETECTION_DPI / 72, DETECTION_DPI / 72)
        pix = page.get_pixmap(matrix=mat, colorspace=fitz.csGRAY)
        samples = pix.samples
        n_total = len(samples)
        if n_total == 0:
            doc.close()
            return False, "no_pixels"
        n_white = sum(1 for b in samples if b >= 240)
        white_ratio = n_white / n_total
        doc.close()

        if white_ratio >= threshold:
            return True, f"blank: text={text_len}, white={white_ratio:.4f}"
        return False, f"not_blank: text={text_len}, white={white_ratio:.4f}"
    except Exception as e:
        return False, f"err:{e}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--company-id")
    ap.add_argument("--company-ids")
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    ap.add_argument("--show-samples", type=int, default=10)
    args = ap.parse_args()
    is_execute = args.execute and not args.dry_run
    threshold = args.threshold

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    q = """SELECT page_id, company_id, file_name, page_no
           FROM pages
           WHERE doc_type_name='その他/不明'
             AND NOT EXISTS (
               SELECT 1 FROM page_doc_type_history h
               WHERE h.page_id = pages.page_id
                 AND h.confirmed_by IN ('web_viewer','user_visual')
             )"""
    params: tuple = ()
    if args.company_id:
        q += " AND company_id=?"
        params = (args.company_id,)
    elif args.company_ids:
        cids = [c.strip() for c in args.company_ids.split(",") if c.strip()]
        if cids:
            placeholders = ",".join("?" for _ in cids)
            q += f" AND company_id IN ({placeholders})"
            params = tuple(cids)
    q += " ORDER BY company_id, file_name, page_no"

    targets = list(conn.execute(q, params).fetchall())
    print(f"対象: {len(targets)} ページ (white ratio threshold {threshold})", file=sys.stderr)

    workflow = f"classify_blank_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    blank_count = 0
    not_blank_count = 0
    skip_count = 0
    samples_shown = []

    if is_execute:
        conn.execute("BEGIN")

    for i, r in enumerate(targets, 1):
        if not r["file_name"].lower().endswith(".pdf"):
            skip_count += 1
            continue
        pdf = find_pdf(r["file_name"], r["company_id"])
        if not pdf:
            skip_count += 1
            continue

        is_blank, reason = is_blank_page(pdf, r["page_no"], threshold)
        if is_blank:
            blank_count += 1
            if len(samples_shown) < args.show_samples:
                samples_shown.append((r["company_id"], r["file_name"][:40], r["page_no"], reason))
            if is_execute:
                conn.execute(
                    "UPDATE pages SET doc_type_name='空白', confidence=0.95 WHERE page_id=?",
                    (r["page_id"],)
                )
                conn.execute(
                    """INSERT INTO page_doc_type_history
                       (page_id, company_id, file_name, page_no,
                        old_doc_type_name, new_doc_type_name,
                        reason, workflow_id, decision_id, confirmed_by)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'classify_blank')""",
                    (r["page_id"], r["company_id"], r["file_name"], r["page_no"],
                     "その他/不明", "空白", reason, workflow, str(uuid.uuid4()))
                )
        else:
            not_blank_count += 1

        if i % 200 == 0:
            print(f"  進捗 {i}/{len(targets)} (blank={blank_count})", file=sys.stderr, flush=True)

    if is_execute:
        conn.commit()
        print("COMMIT OK", file=sys.stderr)

    print(f"\n=== {'EXECUTE' if is_execute else 'DRY-RUN'} 結果 ===")
    print(f"  空白判定: {blank_count}")
    print(f"  非空白:   {not_blank_count}")
    print(f"  skip:     {skip_count}")
    if samples_shown:
        print(f"\n=== サンプル ({len(samples_shown)} 件) ===")
        for s in samples_shown:
            print(f"  {s[0]} {s[1]} P{s[2]}: {s[3]}")


if __name__ == "__main__":
    main()
