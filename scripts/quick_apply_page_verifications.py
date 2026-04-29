"""
ユーザー目視確認結果を pages テーブル + page_doc_type_history に直接反映 (Excel 経由なし)。

呼び出し例:
  python scripts/quick_apply_page_verifications.py --execute
"""
from __future__ import annotations
import argparse, sqlite3, sys, uuid
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "permit_tracker.db"

# 確認結果 (cid, file_name, page_no, new_doc_type, source_comment)
VERIFICATIONS = [
    ("C0012", "御取引条件等説明書_協和電気工業株式会社.pdf", 18, "労働者名簿",
     "藤田希望採用 (Masaru 目視確認 2026-04-29)"),
    ("C0055", "御取引条件等説明書_石田設備株式会社.pdf", 4, "労働者名簿",
     "藤田希望採用 (Masaru 目視確認)"),
    ("C0028", "御取引条件等説明書_有限会社トーケン.pdf", 1, "取引申請書",
     "藤田希望採用 (Masaru 目視確認)"),
    ("C0018", "御取引条件等説明書_安成工業株式会社.pdf", 1, "取引申請書",
     "藤田希望採用 (Masaru 目視確認)"),
    ("C0018", "御取引条件等説明書_安成工業株式会社.pdf", 18, "取引先一覧表",
     "藤田希望採用 (Masaru 確認: P18 は取引先一覧表+工事経歴書 両方を含む、取引先一覧表として登録)"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    print(f"=== Quick Apply Page Verifications ===")
    print(f"  mode: {'EXECUTE' if args.execute else 'DRY-RUN'}")
    print(f"  対象: {len(VERIFICATIONS)} 件\n")

    workflow_id = f"masaru_quick_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    conn = sqlite3.connect(DB_PATH)
    operator = "masaru_manual"

    for cid, fname, pno, new_doc, comment in VERIFICATIONS:
        # page_id 取得
        row = conn.execute(
            "SELECT page_id, doc_type_name FROM pages "
            "WHERE company_id=? AND file_name=? AND page_no=?",
            (cid, fname, pno),
        ).fetchone()
        if not row:
            print(f"  [MISS] {cid} {fname} P{pno} not found in pages table")
            continue
        page_id, old_doc = row[0], row[1]
        old_doc = old_doc or "(未登録)"
        print(f"  [{cid} P{pno}] {old_doc} → {new_doc}")
        print(f"       page_id={page_id}, comment={comment}")

        if args.execute:
            try:
                conn.execute("BEGIN")
                # history INSERT
                conn.execute(
                    """INSERT INTO page_doc_type_history
                    (page_id, company_id, file_name, page_no, old_doc_type_name,
                     new_doc_type_name, reason, workflow_id, decision_id, confirmed_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (page_id, cid, fname, pno, old_doc, new_doc, comment,
                     workflow_id, str(uuid.uuid4()), operator),
                )
                # pages UPDATE
                conn.execute(
                    "UPDATE pages SET doc_type_name=? WHERE page_id=?",
                    (new_doc, page_id),
                )
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"  [ERROR] {e}")
                raise

    conn.close()
    print()
    if args.execute:
        print(f"=== 完了 ===")
        print(f"  workflow_id: {workflow_id}")
        print(f"  pages UPDATE + page_doc_type_history INSERT 完了")
    else:
        print(f"[DRY-RUN] --execute で実行")


if __name__ == "__main__":
    main()
