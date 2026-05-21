"""
PyMuPDF テキスト抽出ベースの bundled PDF 再分類 (Azure DI 不要、無料・高速)。

reclassify_bundled_pdfs_azure.py の classify_page ロジックを流用。
PDF にテキストレイヤーがあれば動作（Excel/Word 由来 PDF は通常 OK）。

Usage:
  python scripts/reclassify_bundled_pdfs_pymupdf.py --dry-run
  python scripts/reclassify_bundled_pdfs_pymupdf.py --execute
  python scripts/reclassify_bundled_pdfs_pymupdf.py --execute --company-id C0085
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path

import pymupdf as fitz

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent))
from reclassify_bundled_pdfs_azure import classify_page, BUNDLE_PATTERN

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"


def find_pdf(file_name: str, cid: str) -> Path | None:
    for cdir in (PROJECT / "data" / "originals").glob(f"{cid}_*"):
        if cdir.is_dir():
            for p in cdir.rglob(file_name):
                if p.is_file():
                    return p
            # 名前先頭に timestamp prefix 付いてる場合の fuzzy match
            for p in cdir.rglob(f"*{file_name}"):
                if p.is_file():
                    return p
    return None


def extract_pages_text(pdf_path: Path) -> list[str]:
    doc = fitz.open(pdf_path)
    out = []
    try:
        for page in doc:
            out.append(page.get_text())
    finally:
        doc.close()
    return out


def list_targets(conn, company_id: str | None = None) -> list[tuple]:
    out = []
    seen = set()
    q = "SELECT DISTINCT company_id, file_name FROM pages"
    rows = conn.execute(q).fetchall()
    for r in rows:
        if not BUNDLE_PATTERN.search(r["file_name"]):
            continue
        if company_id and r["company_id"] != company_id:
            continue
        key = (r["company_id"], r["file_name"])
        if key in seen:
            continue
        seen.add(key)
        max_pn = conn.execute(
            "SELECT MAX(page_no) FROM pages WHERE company_id=? AND file_name=?",
            (r["company_id"], r["file_name"]),
        ).fetchone()[0]
        out.append((r["company_id"], r["file_name"], max_pn or 1))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--company-id", help="特定 cid のみ")
    ap.add_argument("--only-undersized", action="store_true",
                    help="DB の page 数が PDF ページ数より少ないものだけ対象")
    args = ap.parse_args()
    is_execute = args.execute and not args.dry_run

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    targets = list_targets(conn, args.company_id)
    print(f"対象 bundle: {len(targets)} ファイル", file=sys.stderr)

    workflow = f"reclassify_pymupdf_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    total_inserts = 0
    total_updates = 0
    total_unchanged = 0
    skipped_pdf = 0
    skipped_complete = 0

    if is_execute:
        conn.execute("BEGIN")
    try:
        for i, (cid, fn, db_max_pn) in enumerate(targets, 1):
            pdf = find_pdf(fn, cid)
            if not pdf:
                skipped_pdf += 1
                continue
            try:
                page_texts = extract_pages_text(pdf)
            except Exception as e:
                print(f"  [{i}/{len(targets)}] OCR fail {cid} {fn[:40]}: {e}", file=sys.stderr)
                continue
            pdf_pages = len(page_texts)

            if args.only_undersized and pdf_pages <= db_max_pn:
                skipped_complete += 1
                continue

            print(f"[{i}/{len(targets)}] {cid} {fn[:50]} ({pdf_pages}p, db_max={db_max_pn})", file=sys.stderr)

            for pno, ptxt in enumerate(page_texts, 1):
                new_type, new_conf = classify_page(ptxt)
                if not new_type:
                    new_type = ""  # uncategorized
                    new_conf = 0.0
                existing = conn.execute(
                    "SELECT page_id, doc_type_name FROM pages WHERE company_id=? AND file_name=? AND page_no=?",
                    (cid, fn, pno),
                ).fetchone()
                if not existing:
                    if is_execute:
                        conn.execute(
                            "INSERT INTO pages (company_id, file_name, page_no, doc_type_name, confidence, rotation, created_at) "
                            "VALUES (?, ?, ?, ?, ?, 0, datetime('now','localtime'))",
                            (cid, fn, pno, new_type, new_conf),
                        )
                    total_inserts += 1
                elif existing["doc_type_name"] != new_type and new_type:
                    if is_execute:
                        conn.execute(
                            "UPDATE pages SET doc_type_name=?, confidence=? WHERE page_id=?",
                            (new_type, new_conf, existing["page_id"]),
                        )
                        conn.execute(
                            "INSERT INTO page_doc_type_history (page_id, company_id, file_name, page_no, "
                            "old_doc_type_name, new_doc_type_name, reason, workflow_id, decision_id, confirmed_by) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'reclassify_pymupdf')",
                            (existing["page_id"], cid, fn, pno,
                             existing["doc_type_name"], new_type,
                             f"PyMuPDF 全ページ再分類 conf={new_conf:.2f}",
                             workflow, str(uuid.uuid4()))
                        )
                    total_updates += 1
                else:
                    total_unchanged += 1
        if is_execute:
            conn.commit()
    except Exception as e:
        if is_execute:
            conn.rollback()
        print(f"ERROR: {e}", file=sys.stderr)
        raise

    print()
    print(f"=== {'EXECUTE' if is_execute else 'DRY-RUN'} 結果 ===")
    print(f"  INSERT: {total_inserts}")
    print(f"  UPDATE: {total_updates}")
    print(f"  unchanged: {total_unchanged}")
    print(f"  skipped (PDF not found): {skipped_pdf}")
    if args.only_undersized:
        print(f"  skipped (already complete): {skipped_complete}")


if __name__ == "__main__":
    main()
