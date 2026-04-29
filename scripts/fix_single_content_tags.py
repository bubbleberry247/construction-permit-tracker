"""
単一内容PDFのdoc_type_name誤分類を自動修正。
ファイル名から明確に種別が判別できる場合のみ UPDATE。

Usage:
    python scripts/fix_single_content_tags.py [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB = PROJECT_ROOT / "data" / "permit_tracker.db"

# 多ページ一括PDFパターン（これらはスキップ）
BUNDLE_PATTERN = re.compile(r"御?お?取引条件|継続取引|新規・継続|取引申請書類|取引申請書(?!.*?)$|取引条件等説明書|継続取引申請書類")

# 単一内容PDFキーワード（優先度順：具体的なものほど上）
SINGLE_CONTENT_RULES = [
    (re.compile(r"^建設業許可|^建築業許可|^許可証(?!明書)|^許可通知|^許可　"), "建設業許可証"),
    (re.compile(r"^会社案内|^会社概要|^会社プロフィール|会社パンフ"), "会社案内"),
    (re.compile(r"^工事経歴|^施工実績|^令和\d+年工事経歴|工事実績"), "工事経歴書"),
    (re.compile(r"^労働者名簿|^従業員名簿"), "労働者名簿"),
    (re.compile(r"^(主要)?取引先|^得意先|^仕入先|^.*取引先一覧"), "取引先一覧表"),
    (re.compile(r"^労働安全衛生誓約|^誓約書"), "労働安全衛生誓約書"),
    (re.compile(r"^資格者名簿|^有資格者|^技能者名簿|^資格略字"), "資格略字一覧"),
    # 決算（「決算公告用」「簡易版」「報告書」各種含む）
    (re.compile(r"決算報告書|決算書|^第\d+期|^\d+期.*決算|損益計算書|貸借対照表"), "決算書"),
    # 取引申請書（単独 申請書.pdf / チェックリスト.pdf 等のみ、bundled は除外済）
    (re.compile(r"^申請書(?:\.|_|\s|$)|^提出書類チェック|^チェックリスト|^新規.?継続取引申請書(?:\.|_|\s|$)"), "取引申請書"),
]


def classify_by_filename(fn: str) -> str | None:
    if BUNDLE_PATTERN.search(fn):
        return None
    for rgx, dt in SINGLE_CONTENT_RULES:
        if rgx.search(fn):
            return dt
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT page_id, company_id, file_name, page_no, doc_type_name FROM pages"
    ).fetchall()

    updates = []
    for r in rows:
        expected = classify_by_filename(r["file_name"])
        if expected and expected != r["doc_type_name"]:
            updates.append((r["page_id"], r["company_id"], r["file_name"], r["page_no"], r["doc_type_name"], expected))

    print(f"対象候補: {len(updates)}件")
    print()
    # サンプル表示
    print("=== 修正前後サンプル（先頭20件） ===")
    for pid, cid, fn, pno, cur, exp in updates[:20]:
        print(f"  [{cid}] {fn[:50]:<50} p{pno} : {cur} → {exp}")

    if args.dry_run:
        print("\n(dry-run) 書き込みなし")
        return

    # UPDATE
    for pid, cid, fn, pno, cur, exp in updates:
        conn.execute(
            "UPDATE pages SET doc_type_name=? WHERE page_id=?",
            (exp, pid),
        )
    conn.commit()
    print(f"\n✓ UPDATE 完了: {len(updates)}件")

    # 修正後の doc_type_name 分布
    from collections import Counter
    dt_cnt = Counter(r["doc_type_name"] for r in conn.execute("SELECT doc_type_name FROM pages"))
    print("\n=== 修正後 doc_type_name 分布 ===")
    for k, v in dt_cnt.most_common():
        print(f"  {k!r}: {v}ページ")


if __name__ == "__main__":
    main()
