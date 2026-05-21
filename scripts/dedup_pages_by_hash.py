"""
同 file_hash の重複ファイル群を会社ごとに検出して dedup する。

既存 dedup_pages.py は (company_id, file_name, page_no) ベース。
本スクリプトは (company_id, file_hash) ベースで file_name 違いの重複を捕捉する。

原因: fetch_gmail.py が file_hash で dedup チェックしないため、
同じ Gmail 添付が複数回 (timestamp prefix 違いで) DB に登録された。

ロジック:
  1. company_id + file_hash で重複ファイル群を抽出 (n_files >= 2)
  2. canonical = 最古 page_id を持つファイル (最初に取り込まれたもの)
  3. canonical 以外のファイルの全ページを削除候補
  4. 手動ラベル (web_viewer/user_visual/masaru_manual) のあるページは除外
  5. 削除前に history に記録 (workflow=dedup_hash_<TS>)

Usage:
  python scripts/dedup_pages_by_hash.py --dry-run
  python scripts/dedup_pages_by_hash.py --execute
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
sys.stdout.reconfigure(encoding="utf-8")


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

    # 同 (cid, hash) で複数 file_name のグループ
    groups = list(conn.execute("""
        SELECT company_id, file_hash, COUNT(DISTINCT file_name) n_files
        FROM pages
        WHERE file_hash IS NOT NULL AND file_hash != ''
        GROUP BY company_id, file_hash
        HAVING n_files > 1
        ORDER BY company_id
    """).fetchall())

    total_dup_groups = len(groups)
    total_del_pages = 0
    skipped_manual = 0
    plans: list[dict] = []

    for g in groups:
        cid = g["company_id"]
        fh = g["file_hash"]
        # canonical = 最古 page_id を持つ file_name
        files = list(conn.execute("""
            SELECT file_name, MIN(page_id) min_pid, COUNT(*) n_pages
            FROM pages WHERE company_id=? AND file_hash=?
            GROUP BY file_name ORDER BY min_pid
        """, (cid, fh)).fetchall())
        if len(files) < 2:
            continue
        canonical = files[0]["file_name"]
        for f in files[1:]:
            del_fn = f["file_name"]
            del_pages = list(conn.execute("""
                SELECT page_id, page_no, doc_type_name FROM pages
                WHERE company_id=? AND file_name=? ORDER BY page_no
            """, (cid, del_fn)).fetchall())

            # 手動ラベル除外
            safe_to_delete = []
            for p in del_pages:
                manual = conn.execute(
                    """SELECT 1 FROM page_doc_type_history WHERE page_id=?
                       AND confirmed_by IN ('web_viewer','user_visual','masaru_manual') LIMIT 1""",
                    (p["page_id"],)).fetchone()
                if manual:
                    skipped_manual += 1
                    continue
                safe_to_delete.append(p)
            if not safe_to_delete:
                continue
            plans.append({
                "company_id": cid,
                "file_hash": fh,
                "canonical": canonical,
                "delete_file": del_fn,
                "pages": safe_to_delete,
            })
            total_del_pages += len(safe_to_delete)

    print(f"=== hash ベース dedup プラン ===")
    print(f"  重複 hash グループ:     {total_dup_groups}")
    print(f"  削除予定 ページ:        {total_del_pages}")
    print(f"  手動ラベル保護スキップ: {skipped_manual}")
    print()

    # 会社別サマリ
    from collections import Counter
    by_cid = Counter()
    for p in plans:
        by_cid[p["company_id"]] += len(p["pages"])
    print(f"=== 会社別 削除ページ数 (top 15) ===")
    for cid, n in by_cid.most_common(15):
        print(f"  {cid}: {n}")

    if args.dry_run:
        print(f"\n[dry-run] --execute で実行")
        return

    # 実行
    workflow = f"dedup_hash_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"\n[execute] workflow={workflow}", file=sys.stderr)
    conn.execute("BEGIN IMMEDIATE")
    n_del = 0
    for plan in plans:
        for p in plan["pages"]:
            conn.execute(
                """INSERT INTO page_doc_type_history
                   (page_id, company_id, file_name, page_no,
                    old_doc_type_name, new_doc_type_name,
                    reason, workflow_id, decision_id, confirmed_by)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'system')""",
                (p["page_id"], plan["company_id"], plan["delete_file"], p["page_no"],
                 p["doc_type_name"], "(deleted_dup)",
                 f"hash dup: canonical={plan['canonical'][:30]}",
                 workflow, str(uuid.uuid4()))
            )
            conn.execute("DELETE FROM pages WHERE page_id=?", (p["page_id"],))
            n_del += 1
    conn.commit()
    print(f"\n=== 完了 ===")
    print(f"  DELETE pages: {n_del}")
    print(f"  workflow:     {workflow}")
    print(f"  手動ラベル保護スキップ: {skipped_manual}")


if __name__ == "__main__":
    main()
