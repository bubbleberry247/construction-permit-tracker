"""
Phase R-C-1: 藤田の修正依頼コメント (81 件) を正規表現で自動分類。

入力: 藤田 xlsx の「修正依頼」シート Col 32 (修正依頼自由文)
出力: output/FDE_MANAGED/10_corrections_classified_<TS>.csv

分類カテゴリ:
  PAGE_REREGISTER   - 「P○ に添付あり」「会社概要に記載」 → ページ再登録
  EMAIL_REIMPORT    - 「○月○日メールに添付」 → メール再取り込み
  EXEMPTION         - 「卸業者のため」「提出不要」 → 不要書類
  REJECTED          - 「拒否」「書類不備のまま審査へ」 → 督促/拒否
  STILL_MISSING     - 「なし」「未提出」 → 真に未受領
  OTHER             - それ以外 (要人間判断)

Usage:
  python scripts/extract_corrections_from_comments.py <fujita_xlsx>
"""
from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import openpyxl

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "permit_tracker.db"

DOC_COLS = ["取引申請書", "建設業許可証", "決算書", "工事経歴書",
             "取引先一覧表", "労働安全衛生誓約書", "資格略字一覧", "労働者名簿"]


# 分類パターン (上から順に評価、最初にマッチしたものを採用)
PATTERNS = [
    # PAGE_REREGISTER (拡張: 会社案内・受領バンドル・申請書内・最終ページ・保存あり)
    (re.compile(r"P\s*(\d+)\s*に?添付"), "PAGE_REREGISTER", "page_in_bundle"),
    (re.compile(r"P\s*(\d+)$"), "PAGE_REREGISTER", "page_only"),
    (re.compile(r"会社(?:概要|案内)(?:に|の|内|最終)?(?:記載|添付|保存|ページ)?"), "PAGE_REREGISTER", "in_company_overview"),
    (re.compile(r"申請書\s*P?\s*(\d+)"), "PAGE_REREGISTER", "page_in_application"),
    (re.compile(r"バンドル|ﾊﾟﾝﾄﾞﾙ|パンドル|ﾊﾞﾝﾄﾞﾙ"), "PAGE_REREGISTER", "in_bundle"),
    (re.compile(r"内に保存|内に添付|内にあり"), "PAGE_REREGISTER", "in_other_doc"),
    (re.compile(r"添付あり|記載あり|保存あり"), "PAGE_REREGISTER", "exists_attached"),
    (re.compile(r"誓約書と同じ|同じ資料"), "PAGE_REREGISTER", "same_as_other"),

    # EMAIL_REIMPORT (日付パターンも対応)
    (re.compile(r"(\d+)\s*/\s*(\d+)\s*メール"), "EMAIL_REIMPORT", "email_attachment"),
    (re.compile(r"\d+\s*メール添付"), "EMAIL_REIMPORT", "email_attachment_short"),
    (re.compile(r"メール(?:に)?添付"), "EMAIL_REIMPORT", "email_attachment"),

    # EXEMPTION
    (re.compile(r"卸業者"), "EXEMPTION", "NOT_APPLICABLE"),
    (re.compile(r"提出(?:不要|なし|の必要なし)"), "EXEMPTION", "NOT_APPLICABLE"),
    (re.compile(r"対象外"), "EXEMPTION", "NOT_APPLICABLE"),

    # REJECTED
    (re.compile(r"拒否|書類不備のまま"), "REJECTED", "rejected_by_company"),

    # STILL_MISSING
    (re.compile(r"^なし$|^未提出$"), "STILL_MISSING", "truly_missing"),
    (re.compile(r"^〇$|^○$"), "ALREADY_OK", "marked_ok"),

    # 会社名変更
    (re.compile(r"会社名変更|社名変更"), "COMPANY_RENAME", "name_change"),
]


def classify_comment(comment: str) -> tuple[str, str, str | None]:
    """returns (category, sub_kind, matched_value)"""
    if not isinstance(comment, str) or not comment.strip():
        return ("EMPTY", "", None)
    for pat, cat, sub in PATTERNS:
        m = pat.search(comment)
        if m:
            matched = m.group(0)
            return (cat, sub, matched)
    return ("OTHER", "manual_review_needed", None)


def extract_page_no(comment: str) -> int | None:
    """藤田コメントから P 番号を抽出 (例: "P18" → 18)"""
    if not isinstance(comment, str):
        return None
    m = re.search(r"P\s*(\d+)", comment)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass
    return None


def extract_source_file_hint(comment: str) -> str | None:
    """藤田コメントから参照元ファイルのヒントを抽出
    例: "会社概要に記載" → "会社概要"
        "申請書 P18 添付" → "申請書"
    """
    if not isinstance(comment, str):
        return None
    if re.search(r"会社(?:概要|案内)", comment):
        return "会社概要"
    if re.search(r"申請書", comment):
        return "取引申請書"
    if re.search(r"バンドル|ﾊﾟﾝﾄﾞﾙ", comment):
        return "バンドル"
    return None


def split_comment_by_doc(comment: str) -> list[tuple[str, str]]:
    """「労働者名簿: P18 に添付」のように「書類名: 内容」で分割される複数指摘を分離

    返値: [(doc_type, sub_comment), ...]
    """
    if not isinstance(comment, str):
        return []
    parts = []
    # 「・」「、」「,」「\n」で複数指摘を分割
    # 「/」は日付や複数書類の区切りでも使われるので、「数字/数字」(日付) は保護
    DATE_PLACEHOLDER = "\x01"
    tmp = re.sub(r"(\d+)/(\d+)", lambda m: f"{m.group(1)}{DATE_PLACEHOLDER}{m.group(2)}", comment)
    chunks = re.split(r"[・、，,\n/]", tmp)
    chunks = [c.replace(DATE_PLACEHOLDER, "/") for c in chunks]
    for chunk in chunks:
        chunk = chunk.strip()
        if not chunk:
            continue
        # 「<書類名>: <内容>」「<書類名>：<内容>」形式
        m = re.match(r"^(.+?)[：:]\s*(.+)$", chunk)
        if m:
            doc = m.group(1).strip()
            text = m.group(2).strip()
            # doc_type 候補チェック (DOC_COLS 部分一致)
            doc_normalized = next(
                (dc for dc in DOC_COLS if dc in doc or doc in dc), doc
            )
            parts.append((doc_normalized, text))
        else:
            parts.append(("(全般)", chunk))
    return parts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("fujita_xlsx", type=Path)
    ap.add_argument("--ts", default=datetime.now().strftime("%Y%m%d_%H%M%S"))
    args = ap.parse_args()

    if not args.fujita_xlsx.exists():
        print(f"ERROR: {args.fujita_xlsx} not found", file=sys.stderr)
        sys.exit(1)

    print(f"=== Phase R-C-1: 修正依頼コメント自動分類 ===")

    wb = openpyxl.load_workbook(args.fujita_xlsx, data_only=True)
    ws = wb["修正依頼"]

    # fujita_mapping から company_id 取得
    conn = sqlite3.connect(DB_PATH)
    name_to_cid = {row[0]: row[1] for row in conn.execute(
        "SELECT fujita_name, company_id FROM fujita_mapping WHERE company_id IS NOT NULL"
    )}
    conn.close()
    print(f"  fujita_mapping: {len(name_to_cid)} 件")

    rows = []
    n_with_comment = 0
    for r in range(2, ws.max_row + 1):
        nm = ws.cell(r, 1).value
        comment = ws.cell(r, 32).value
        fuji_status = ws.cell(r, 33).value
        if not nm:
            continue
        if not comment or not str(comment).strip():
            continue
        n_with_comment += 1

        cid = name_to_cid.get(nm) or ""

        # コメントを書類別に分割
        parts = split_comment_by_doc(str(comment))
        for doc, text in parts:
            cat, sub, matched = classify_comment(text)
            page_no = extract_page_no(text) or extract_page_no(str(comment))
            source_hint = extract_source_file_hint(text) or extract_source_file_hint(str(comment))
            rows.append({
                "shusei_row": r,
                "company_id": cid,
                "fujita_name": nm,
                "fuji_status": fuji_status or "",
                "doc_type": doc,                  # 藤田が「ここにある」と言った doc_type
                "sub_comment": text,
                "category": cat,
                "sub_kind": sub,
                "matched_value": matched or "",
                "page_no": page_no or "",          # 藤田指摘の P 番号 (なければ空)
                "source_file_hint": source_hint or "",   # 「会社概要」「申請書」等
                "full_comment": comment,
            })

    print(f"  コメントあり行: {n_with_comment} / {ws.max_row - 1}")
    print(f"  分割後 entries:  {len(rows)}")
    print()
    from collections import Counter
    cat_count = Counter(r["category"] for r in rows)
    print("  カテゴリ別:")
    for k, v in cat_count.most_common():
        print(f"    {k}: {v}")

    # CSV 出力
    out = PROJECT / "output" / "FDE_MANAGED" / f"10_corrections_classified_{args.ts}.csv"
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["shusei_row", "company_id", "fujita_name", "fuji_status",
                    "doc_type", "sub_comment", "category", "sub_kind",
                    "matched_value", "page_no", "source_file_hint", "full_comment"])
        for r in rows:
            w.writerow([r["shusei_row"], r["company_id"], r["fujita_name"],
                        r["fuji_status"], r["doc_type"], r["sub_comment"],
                        r["category"], r["sub_kind"], r["matched_value"],
                        r.get("page_no", ""), r.get("source_file_hint", ""),
                        r["full_comment"]])

    print(f"\n出力: {out.relative_to(PROJECT)}")
    print(f"  PAGE_REREGISTER: {cat_count.get('PAGE_REREGISTER', 0)} 件 → 当方でバンドル PDF を開いて該当ページを再登録")
    print(f"  EMAIL_REIMPORT:  {cat_count.get('EMAIL_REIMPORT', 0)} 件 → メール再取り込み or ページ再登録")
    print(f"  EXEMPTION:       {cat_count.get('EXEMPTION', 0)} 件 → company_doc_exemptions")
    print(f"  REJECTED:        {cat_count.get('REJECTED', 0)} 件 → 督促/拒否フラグ")
    print(f"  STILL_MISSING:   {cat_count.get('STILL_MISSING', 0)} 件 → 真に未受領 (督促)")
    print(f"  OTHER:           {cat_count.get('OTHER', 0)} 件 → 人間レビュー必須")


if __name__ == "__main__":
    main()
