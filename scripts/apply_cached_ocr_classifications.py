"""
既存 OCR キャッシュ (data/ocr_cache/page_*.json) を読み、
「その他/不明」分類の page を再分類して DB UPDATE する高速スクリプト。

Azure DI は呼ばない。キャッシュのみ。
50 件ごとに commit、進捗ログ出力。

設計鉄則:
  - 手動修正 (web_viewer / user_visual) は絶対保護
  - workflow=cached_ocr_apply_<TS>
  - history append-only
  - 改善時のみ UPDATE
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path

import pymupdf as fitz

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
OCR_CACHE = PROJECT / "data" / "ocr_cache"
sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from reclassify_bundled_pdfs_azure import classify_page

VALID_DOC_TYPES = {
    "取引申請書", "建設業許可証", "決算書", "工事経歴書",
    "取引先一覧表", "労働安全衛生誓約書", "資格略字一覧", "労働者名簿",
    "会社案内",
}

COMMIT_EVERY = 50


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


def extract_single_page_pdf_bytes(pdf: Path, page_no: int) -> bytes:
    src = fitz.open(pdf)
    if page_no > len(src):
        src.close()
        raise ValueError(f"page_no {page_no} > {len(src)}")
    out_doc = fitz.open()
    out_doc.insert_pdf(src, from_page=page_no - 1, to_page=page_no - 1)
    data = out_doc.tobytes()
    out_doc.close()
    src.close()
    return data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()
    is_execute = args.execute and not args.dry_run

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    q = """SELECT page_id, company_id, file_name, page_no
           FROM pages
           WHERE doc_type_name='その他/不明'
             AND NOT EXISTS (
               SELECT 1 FROM page_doc_type_history h
               WHERE h.page_id = pages.page_id
                 AND h.confirmed_by IN ('web_viewer','user_visual')
             )
           ORDER BY company_id, file_name, page_no"""

    candidates = list(conn.execute(q).fetchall())
    print(f"DB 候補: {len(candidates)} ページ", file=sys.stderr)

    workflow = f"cached_ocr_apply_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    by_cat = {}
    no_match = 0
    cache_miss = 0
    pdf_not_found = 0
    pending = 0

    if is_execute:
        conn.execute("BEGIN")

    for i, r in enumerate(candidates, 1):
        if not r["file_name"].lower().endswith(".pdf"):
            continue
        pdf = find_pdf(r["file_name"], r["company_id"])
        if not pdf:
            pdf_not_found += 1
            continue
        try:
            pdf_bytes = extract_single_page_pdf_bytes(pdf, r["page_no"])
        except Exception:
            continue
        cache_key = hashlib.sha256(pdf_bytes).hexdigest()
        cache_file = OCR_CACHE / f"page_{cache_key}.json"
        if not cache_file.exists():
            cache_miss += 1
            continue
        try:
            text = json.loads(cache_file.read_text(encoding="utf-8")).get("text", "")
        except Exception:
            continue
        if not text:
            no_match += 1
            continue

        new_type, new_conf = classify_page(text)
        if new_type == "その他/不明" or new_type not in VALID_DOC_TYPES:
            no_match += 1
            continue

        by_cat[new_type] = by_cat.get(new_type, 0) + 1
        if is_execute:
            conn.execute(
                "UPDATE pages SET doc_type_name=?, confidence=? WHERE page_id=?",
                (new_type, new_conf, r["page_id"])
            )
            conn.execute(
                """INSERT INTO page_doc_type_history
                   (page_id, company_id, file_name, page_no,
                    old_doc_type_name, new_doc_type_name,
                    reason, workflow_id, decision_id, confirmed_by)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'cached_ocr_apply')""",
                (r["page_id"], r["company_id"], r["file_name"], r["page_no"],
                 "その他/不明", new_type,
                 f"既存OCRキャッシュから分類 conf={new_conf:.2f}",
                 workflow, str(uuid.uuid4()))
            )
            pending += 1
            if pending >= COMMIT_EVERY:
                conn.commit()
                pending = 0
                conn.execute("BEGIN")
                print(f"  [commit] {i}/{len(candidates)} 改善 {sum(by_cat.values())} miss={cache_miss}",
                      file=sys.stderr, flush=True)

    if is_execute and pending > 0:
        conn.commit()
    print(f"\n=== {'EXECUTE' if is_execute else 'DRY-RUN'} 結果 ===", file=sys.stderr)
    print(f"  改善: {sum(by_cat.values())}")
    print(f"  cache miss (Azure DI 必要): {cache_miss}")
    print(f"  分類不能: {no_match}")
    print(f"  PDF not found: {pdf_not_found}")
    for k, v in sorted(by_cat.items(), key=lambda x: -x[1]):
        print(f"  → {k}: {v}")


if __name__ == "__main__":
    main()
