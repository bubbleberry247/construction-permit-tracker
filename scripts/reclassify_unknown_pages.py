"""
doc_type_name='その他/不明' の page を強化キーワード辞書で再分類。

classify_page を逐次拡張し、PyMuPDF テキスト抽出 + キーワードマッチで分類。
不明確なものは「その他/不明」のまま残す。

手動修正 (web_viewer / user_visual) は絶対保護。

Usage:
  python scripts/reclassify_unknown_pages.py --dry-run
  python scripts/reclassify_unknown_pages.py --execute
  python scripts/reclassify_unknown_pages.py --execute --company-id C0028
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

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
sys.stdout.reconfigure(encoding="utf-8")


PATTERNS = [
    # 強い証拠 (順序重要、上ほど優先)
    ("建設業許可証", [
        r"建設業の?許可(証|通知書)?",
        r"許可番号\s*[:：]?\s*[第（(]?\s*[国都道府県]",
        r"国土交通大臣\s*許可",
        r"[都道府県]知事\s*(?:許可|登録)",
        r"般-\s*\d+",
        r"特-\s*\d+",
        r"許可年月日",
        r"許可の有効期限",
        r"建設業許可ＮＯ\s*[:：]\s*\S{3,}",
    ]),
    ("決算書", [
        r"貸借対照表",
        r"損益計算書",
        r"販売費及び一般管理費",
        r"株主資本等変動計算書",
        r"個別注記表",
        r"キャッシュ[・･]?フロー計算書",
        r"製造\(?工事\)?原価報告書",
        r"営業利益",
        r"純資産の部",
        r"流動資産",
        r"固定資産",
    ]),
    ("工事経歴書", [
        r"工事経歴書",
        r"様式第二号",
        r"注文者.*工事(の)?概要",
        r"請負代金",
        r"完成又は完成予定年月",
        r"配置技術者",
        r"主任技術者",
        r"工期",
        r"請負金額",
    ]),
    ("取引先一覧表", [
        r"取引先一覧",
        r"主な取引先",
        r"得意先一覧",
        r"仕入先一覧",
        r"取引先名称",
        r"取引先様一覧",
    ]),
    ("会社案内", [
        r"会社案内",
        r"会社概要",
        r"事業内容",
        r"沿革",
        r"主要取引先",
        r"組織図",
        r"事業所一覧",
        r"代表挨拶",
    ]),
    ("労働者名簿", [
        r"労働者名簿",
        r"従業員名簿",
        r"雇入年月日",
        r"従事する業務の種類",
        r"氏\s*名.*性\s*別.*生年月日",
    ]),
    ("労働安全衛生誓約書", [
        r"労働安全衛生誓約書",
        r"安全衛生(に関する)?誓約",
        r"災害防止",
        r"労働安全衛生規則",
        r"安全衛生管理",
    ]),
    ("資格略字一覧", [
        r"資格略字一覧",
        r"略字一覧",
        r"^\s*免\s*許\s*$",
        r"技能講習",
        r"電工.*高.*電",
    ]),
    ("取引申請書", [
        r"新規[・･]継続取引申請書",
        r"継続取引申請書",
        r"取引申請書",
        r"御取引条件等説明書",
        r"取引条件[・･]提出書類",
    ]),
]


def classify(text: str) -> str | None:
    """各カテゴリのヒット数をスコア化、最高スコアを返す。"""
    if not text or len(text.strip()) < 10:
        return None
    scores = {}
    for cat, pats in PATTERNS:
        s = 0
        for p in pats:
            if re.search(p, text):
                s += 1
        if s > 0:
            scores[cat] = s
    if not scores:
        return None
    best = max(scores.values())
    for cat, _ in PATTERNS:
        if scores.get(cat) == best:
            return cat
    return None


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--company-id", help="特定 cid のみ")
    args = ap.parse_args()
    is_execute = args.execute and not args.dry_run

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # 手動修正 (web_viewer / user_visual) は対象から除外 = 絶対保護
    q = """SELECT page_id, company_id, file_name, page_no
           FROM pages
           WHERE doc_type_name='その他/不明'
             AND NOT EXISTS (
               SELECT 1 FROM page_doc_type_history h
               WHERE h.page_id = pages.page_id
                 AND h.confirmed_by IN ('web_viewer','user_visual')
             )"""
    params = ()
    if args.company_id:
        q += " AND company_id=?"
        params = (args.company_id,)
    q += " ORDER BY company_id, file_name, page_no"

    targets = list(conn.execute(q, params).fetchall())
    print(f"対象: {len(targets)} ページ (手動修正は除外済)", file=sys.stderr)

    workflow = f"dict_reclassify_auto_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    by_cat = {}
    pdf_cache = {}

    if is_execute:
        conn.execute("BEGIN")
    try:
        for i, r in enumerate(targets, 1):
            pid = r["page_id"]; cid = r["company_id"]; fn = r["file_name"]; pno = r["page_no"]
            key = (cid, fn)
            if key not in pdf_cache:
                p = find_pdf(fn, cid)
                if not p or p.suffix.lower() != ".pdf":
                    pdf_cache[key] = None
                else:
                    try:
                        pdf_cache[key] = fitz.open(p)
                    except Exception:
                        pdf_cache[key] = None

            doc = pdf_cache[key]
            if doc is None:
                continue
            if pno > len(doc):
                continue
            try:
                txt = doc[pno - 1].get_text() or ""
            except Exception:
                continue

            new_type = classify(txt)
            if not new_type or new_type == "その他/不明":
                continue

            by_cat[new_type] = by_cat.get(new_type, 0) + 1

            if is_execute:
                conn.execute(
                    "UPDATE pages SET doc_type_name=?, confidence=0.7 WHERE page_id=?",
                    (new_type, pid)
                )
                conn.execute(
                    """INSERT INTO page_doc_type_history
                       (page_id, company_id, file_name, page_no,
                        old_doc_type_name, new_doc_type_name,
                        reason, workflow_id, decision_id, confirmed_by)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'dict_reclassify_auto')""",
                    (pid, cid, fn, pno, "その他/不明", new_type,
                     "強化辞書再分類", workflow, str(uuid.uuid4()))
                )

            if i % 100 == 0:
                print(f"  進捗 {i}/{len(targets)}", file=sys.stderr)

        if is_execute:
            conn.commit()
            print("COMMIT OK", file=sys.stderr)
    except Exception as e:
        if is_execute:
            conn.rollback()
        print(f"ERROR rollback: {e}", file=sys.stderr)
        raise
    finally:
        for d in pdf_cache.values():
            if d:
                d.close()

    print(f"\n=== {'EXECUTE' if is_execute else 'DRY-RUN'} 結果 ===")
    total_changed = sum(by_cat.values())
    if targets:
        print(f"  {total_changed} / {len(targets)} を再分類 ({total_changed / len(targets) * 100:.1f}%)")
    else:
        print("no targets")
    for k, v in sorted(by_cat.items(), key=lambda x: -x[1]):
        print(f"  → {k}: {v}")


if __name__ == "__main__":
    main()
