"""
Azure再分類前のバックアップから doc_type_name を復元。
今日14時以降の手動修正(web_viewer)は可能な限り保持を試みる。

Usage:
    python scripts/restore_doctype_from_backup.py [--dry-run]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
CUR_DB = PROJECT / "data" / "permit_tracker.db"
BAK_DB = PROJECT / "data" / "permit_tracker.db.bak_pre_bundled_reclassify_20260424"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not BAK_DB.exists():
        print(f"バックアップが見つかりません: {BAK_DB}", file=sys.stderr)
        return

    cur = sqlite3.connect(str(CUR_DB))
    bak = sqlite3.connect(str(BAK_DB))
    cur.row_factory = sqlite3.Row
    bak.row_factory = sqlite3.Row

    # 今日14時以降の web_viewer 手動修正を収集（保持対象）
    # ただし page_id がないので company_id 単位でカウントのみ
    protected = {}
    for r in cur.execute(
        "SELECT company_id, confirmed_value, COUNT(*) AS c FROM field_reviews "
        "WHERE confirmed_by='web_viewer' AND reviewed_at >= '2026-04-24 14:00:00' "
        "GROUP BY company_id, confirmed_value"
    ):
        protected.setdefault(r["company_id"], {})[r["confirmed_value"]] = r["c"]

    print(f"保護対象（今日午後の web_viewer 手動修正）: {len(protected)} 会社")
    for cid, vals in protected.items():
        print(f"  [{cid}] {vals}")

    # バックアップからの doc_type_name マップ
    bak_map = {}
    for r in bak.execute("SELECT company_id, file_name, page_no, doc_type_name, confidence FROM pages"):
        key = (r["company_id"], r["file_name"], r["page_no"])
        bak_map[key] = (r["doc_type_name"], r["confidence"])

    print(f"\nバックアップ pages: {len(bak_map)} 件")

    # 現DB pages を走査して復元
    restored = 0
    kept_protected = 0
    same = 0
    no_bak = 0
    for r in cur.execute("SELECT page_id, company_id, file_name, page_no, doc_type_name FROM pages"):
        key = (r["company_id"], r["file_name"], r["page_no"])
        bak_val = bak_map.get(key)
        if not bak_val:
            no_bak += 1
            continue
        bak_doc, bak_conf = bak_val
        current_doc = r["doc_type_name"]

        # 保護対象: 現在の値が protected の confirmed_value と一致する場合、残す
        co_prot = protected.get(r["company_id"], {})
        if current_doc in co_prot and co_prot[current_doc] > 0:
            # 消費（1件保護したらカウント-1）
            co_prot[current_doc] -= 1
            kept_protected += 1
            continue

        if current_doc == bak_doc:
            same += 1
            continue

        # 復元
        if not args.dry_run:
            cur.execute(
                "UPDATE pages SET doc_type_name=?, confidence=? WHERE page_id=?",
                (bak_doc, bak_conf, r["page_id"]),
            )
        restored += 1

    if not args.dry_run:
        cur.commit()

    print(f"\n=== 結果 (dry_run={args.dry_run}) ===")
    print(f"  復元: {restored}")
    print(f"  保護（今日の手動修正保持）: {kept_protected}")
    print(f"  同じ値（変更なし）: {same}")
    print(f"  バックアップなし（削除されたページ等）: {no_bak}")

    # 復元後の分布
    from collections import Counter
    dt = Counter(r["doc_type_name"] for r in cur.execute("SELECT doc_type_name FROM pages"))
    print(f"\n=== 復元後 doc_type_name 分布 ===")
    for k, v in dt.most_common():
        print(f"  {k!r}: {v}")


if __name__ == "__main__":
    main()
