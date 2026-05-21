"""
xlsx ファイルの phantom ページ削除。

xlsx → PDF 変換 (Excel COM) 後、PDF 実ページ数 (8-16) より多い page_no を持つ
DB レコードは元の xlsx Azure DI の擬似ページ。これを削除する。

安全:
  - 手動ラベル (web_viewer/user_visual/masaru_manual) があるページは削除しない
  - 削除前に削除対象を全件 history に記録 (workflow=delete_phantom_xlsx_<TS>)
  - DB バックアップ先行必須

Usage:
  python scripts/delete_phantom_xlsx_pages.py --dry-run
  python scripts/delete_phantom_xlsx_pages.py --execute
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
ORIGINALS = PROJECT / "data" / "originals"
sys.stdout.reconfigure(encoding="utf-8")


def find_pdf(file_name: str, cid: str) -> Path | None:
    pdf_alt = file_name[:-5] + ".pdf" if file_name.lower().endswith(".xlsx") else file_name
    for c in ORIGINALS.glob(f"{cid}_*"):
        for p in c.rglob(pdf_alt):
            if p.is_file():
                return p
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    if not args.dry_run and not args.execute:
        print("ERROR: --dry-run か --execute を指定", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    xlsx_files = list(conn.execute(
        "SELECT DISTINCT company_id, file_name FROM pages WHERE file_name LIKE '%.xlsx'"
    ).fetchall())

    targets: list[dict] = []
    for r in xlsx_files:
        cid = r["company_id"]
        fn = r["file_name"]
        pdf = find_pdf(fn, cid)
        pdf_pages = 0
        if pdf:
            try:
                doc = fitz.open(pdf)
                pdf_pages = len(doc)
                doc.close()
            except Exception:
                pdf_pages = 0

        # PDF が無い、または 0 ページの場合は全 page を candidate
        # それ以外は page_no > pdf_pages
        if pdf_pages == 0:
            phantoms = conn.execute(
                "SELECT page_id, page_no, doc_type_name FROM pages WHERE company_id=? AND file_name=? ORDER BY page_no",
                (cid, fn)).fetchall()
        else:
            phantoms = conn.execute(
                "SELECT page_id, page_no, doc_type_name FROM pages WHERE company_id=? AND file_name=? AND page_no > ? ORDER BY page_no",
                (cid, fn, pdf_pages)).fetchall()

        for p in phantoms:
            # 手動ラベルチェック
            manual = conn.execute(
                "SELECT 1 FROM page_doc_type_history WHERE page_id=? AND confirmed_by IN ('web_viewer','user_visual','masaru_manual') LIMIT 1",
                (p["page_id"],)).fetchone()
            if manual:
                continue
            targets.append({
                "page_id": p["page_id"],
                "company_id": cid,
                "file_name": fn,
                "page_no": p["page_no"],
                "doc_type_name": p["doc_type_name"],
                "pdf_pages": pdf_pages,
            })

    print(f"=== phantom 削除候補 ===")
    print(f"  総数: {len(targets)} ページ (across {len(xlsx_files)} xlsx files)")

    # 内訳
    from collections import Counter
    by_cid = Counter(t["company_id"] for t in targets)
    by_label = Counter(t["doc_type_name"] for t in targets)
    print(f"\n  会社別 削除数:")
    for cid, n in by_cid.most_common():
        pdf_p = next((t["pdf_pages"] for t in targets if t["company_id"] == cid), 0)
        print(f"    {cid}: {n} (PDF={pdf_p}p)")
    print(f"\n  ラベル別 削除数:")
    for l, n in by_label.most_common():
        print(f"    {l:25s} {n}")

    if args.dry_run:
        print(f"\n[dry-run] --execute で実行")
        return

    # 実行
    workflow = f"delete_phantom_xlsx_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"\n[execute] workflow={workflow}", file=sys.stderr)
    conn.execute("BEGIN")
    n_del = 0
    for t in targets:
        # history に削除記録
        conn.execute(
            """INSERT INTO page_doc_type_history
               (page_id, company_id, file_name, page_no,
                old_doc_type_name, new_doc_type_name,
                reason, workflow_id, decision_id, confirmed_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'system')""",
            (t["page_id"], t["company_id"], t["file_name"], t["page_no"],
             t["doc_type_name"], "(deleted)",
             f"phantom xlsx page (PDF actual={t['pdf_pages']}p)",
             workflow, str(uuid.uuid4()))
        )
        # pages 削除
        conn.execute("DELETE FROM pages WHERE page_id=?", (t["page_id"],))
        n_del += 1
    conn.commit()
    print(f"\n=== 完了 ===")
    print(f"  DELETE pages: {n_del}")
    print(f"  history記録:  {n_del} (workflow={workflow})")


if __name__ == "__main__":
    main()
