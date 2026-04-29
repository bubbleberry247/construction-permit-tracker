"""
分類監査用CSV出力：全ページのcompany_id/file_name/page_no/doc_type_name/confidence/OCRテキスト先頭を一覧化。
優先順: 「その他/不明」→ 低confidence → 会社順。
ユーザーが Excel で開いて見ながら修正箇所を特定しやすいよう整形。
"""
from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import sys
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
CACHE = PROJECT / "data" / "ocr_cache"
STAGING = PROJECT / "data" / "staging"
ORIGINALS = PROJECT / "data" / "originals"
OUT = PROJECT / "output"


def sha256_of(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def find_pdf(cid: str, fn: str) -> Path | None:
    for root in [STAGING, ORIGINALS]:
        for cdir in root.glob(f"{cid}_*"):
            p = cdir / fn
            if p.exists():
                return p
    return None


def main():
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT page_id, company_id, file_name, page_no, doc_type_name, confidence "
        "FROM pages ORDER BY "
        "CASE WHEN doc_type_name='その他/不明' THEN 0 "
        "     WHEN doc_type_name='その他' THEN 1 "
        "     WHEN confidence IS NULL THEN 2 "
        "     WHEN confidence < 0.8 THEN 3 "
        "     ELSE 4 END, "
        "company_id, file_name, page_no"
    ).fetchall()

    print(f"監査対象: {len(rows)}ページ")

    # OCR text 先頭150文字を付与
    # PDF → hash → ocr_cache の対応を作る
    pdf_cache_map: dict[tuple[str, str], list[str] | None] = {}

    def get_page_text(cid: str, fn: str, pno: int) -> str:
        key = (cid, fn)
        if key not in pdf_cache_map:
            pdf = find_pdf(cid, fn)
            if not pdf:
                pdf_cache_map[key] = None
            else:
                h = sha256_of(pdf)
                cp = CACHE / f"{h}.json"
                if cp.exists():
                    pdf_cache_map[key] = json.loads(cp.read_text(encoding="utf-8"))
                else:
                    pdf_cache_map[key] = None
        pages = pdf_cache_map[key]
        if pages and pno - 1 < len(pages):
            return pages[pno - 1].replace("\n", " / ")[:200]
        return ""

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT / f"classification_audit_{ts}.csv"
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "page_id", "company_id", "file_name", "page_no",
            "doc_type_name", "confidence", "OCR先頭200字（監査用）",
        ])
        for r in rows:
            txt = get_page_text(r["company_id"], r["file_name"], r["page_no"])
            writer.writerow([
                r["page_id"], r["company_id"], r["file_name"], r["page_no"],
                r["doc_type_name"] or "", r["confidence"] or "",
                txt,
            ])
    print(f"出力: {out_path}")

    # サマリ
    from collections import Counter
    dt = Counter(r["doc_type_name"] for r in rows)
    print("\n=== doc_type_name 分布 ===")
    for k, v in dt.most_common():
        print(f"  {k!r}: {v}")


if __name__ == "__main__":
    main()
