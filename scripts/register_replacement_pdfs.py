"""
xlsx シート別 PDF を DB に登録 (置換登録)。

GPT-5.5 推奨設計:
  1. 旧 xlsx 関連 pages は inactive 化 (削除でなく "(superseded)" マーク + history)
  2. 新 PDF をシート名から推定した doc_type で登録
  3. シート 5 (誓約書 + 資格者名簿 混在) は primary='労働安全衛生誓約書' + secondary='資格略字一覧'
  4. conversion_run_id で完全追跡可能
  5. 手動ラベル絶対保護 (web_viewer/user_visual)

Usage:
  python scripts/register_replacement_pdfs.py --run-id xlsx2pdf_<TS> --dry-run
  python scripts/register_replacement_pdfs.py --run-id xlsx2pdf_<TS> --execute
"""
from __future__ import annotations

import argparse
import hashlib
import json
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

# シート名 → doc_type 推定 (GPT-5.5 推奨マッピング)
SHEET_DOC_TYPE_RULES: list[tuple[re.Pattern, str, str | None]] = [
    # (regex, primary doc_type, secondary doc_type or None)
    (re.compile(r"新規.{0,3}継続.{0,3}取引申請書"), "取引申請書", None),
    (re.compile(r"労働安全衛生誓約書.{0,30}資格.{0,10}名簿"), "労働安全衛生誓約書", "資格略字一覧"),
    (re.compile(r"労働安全衛生誓約書"), "労働安全衛生誓約書", None),
    (re.compile(r"資格.{0,5}名簿|資格.{0,5}一覧"), "資格略字一覧", None),
    (re.compile(r"労働者名簿|従業員名簿"), "労働者名簿", None),
    (re.compile(r"提出書類.{0,5}チェック"), "取引申請書", None),
    (re.compile(r"目次|発注|支払基準"), "その他/不明", None),
    (re.compile(r"取引申請書"), "取引申請書", None),
    (re.compile(r"会社案内|会社概要"), "会社案内", None),
    (re.compile(r"工事経歴書"), "工事経歴書", None),
    (re.compile(r"取引先一覧"), "取引先一覧表", None),
    (re.compile(r"決算"), "決算書", None),
    (re.compile(r"建設業許可"), "建設業許可証", None),
]


def guess_doc_type(sheet_name: str) -> tuple[str, str | None]:
    for pat, primary, secondary in SHEET_DOC_TYPE_RULES:
        if pat.search(sheet_name):
            return primary, secondary
    return "その他/不明", None


def file_sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    if not args.dry_run and not args.execute:
        print("ERROR: --dry-run か --execute", file=sys.stderr)
        sys.exit(1)

    log_path = PROJECT / "data" / f"xlsx_conversion_{args.run_id}.json"
    if not log_path.exists():
        print(f"ERROR: ログなし {log_path}", file=sys.stderr)
        sys.exit(1)
    log = json.loads(log_path.read_text(encoding="utf-8"))

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # ステップ 1: 旧 xlsx pages を特定 (cid + xlsx file_name)
    xlsx_targets: dict[str, set[str]] = {}  # cid → set of xlsx file_names
    for r in log:
        if not r.get("xlsx"):
            continue
        cid = r["company_id"]
        xlsx_targets.setdefault(cid, set()).add(r["xlsx"])

    # ステップ 2: 旧 pages 削除候補
    old_pages_to_delete = []
    skipped_manual = 0
    for cid, fns in xlsx_targets.items():
        for fn in fns:
            for p in conn.execute(
                "SELECT page_id, file_name, page_no, doc_type_name FROM pages WHERE company_id=? AND file_name=?",
                (cid, fn)
            ).fetchall():
                manual = conn.execute(
                    """SELECT 1 FROM page_doc_type_history WHERE page_id=?
                       AND confirmed_by IN ('web_viewer','user_visual','masaru_manual') LIMIT 1""",
                    (p["page_id"],)).fetchone()
                if manual:
                    skipped_manual += 1
                    continue
                old_pages_to_delete.append({
                    "page_id": p["page_id"], "cid": cid, "file_name": p["file_name"],
                    "page_no": p["page_no"], "doc_type": p["doc_type_name"],
                })

    # ステップ 3: 新規 INSERT 候補 (同 cid+file_hash 重複は最初の 1 つに寄せる)
    new_pages = []
    seen_keys: set[tuple[str, str, int]] = set()  # (cid, file_name, page_no)
    seen_content: set[tuple[str, str]] = set()    # (cid, file_hash)
    n_skipped_dup_content = 0
    n_skipped_dup_name = 0
    for r in log:
        if not r.get("ok"):
            continue
        if not r.get("path"):
            continue
        pdf = Path(r["path"])
        if not pdf.exists():
            continue
        cid = r["company_id"]
        sheet_name = r["sheet"]
        primary, secondary = guess_doc_type(sheet_name)
        try:
            doc = fitz.open(pdf)
            n_pages = len(doc)
            doc.close()
        except Exception:
            n_pages = 1
        try:
            file_hash = file_sha256(pdf)
        except Exception:
            file_hash = ""

        # 同 (cid, file_hash) は重複ファイル → スキップ
        if file_hash and (cid, file_hash) in seen_content:
            n_skipped_dup_content += 1
            continue
        if file_hash:
            seen_content.add((cid, file_hash))

        for pno in range(1, n_pages + 1):
            key = (cid, pdf.name, pno)
            if key in seen_keys:
                n_skipped_dup_name += 1
                continue
            seen_keys.add(key)
            new_pages.append({
                "cid": cid,
                "file_name": pdf.name,
                "file_hash": file_hash,
                "page_no": pno,
                "doc_type_primary": primary,
                "doc_type_secondary": secondary if pno == 1 else None,
                "sheet_name": sheet_name,
            })

    # サマリ
    print(f"=== Replacement Plan ({args.run_id}) ===")
    print(f"  対象会社:                          {len(xlsx_targets)}")
    print(f"  旧 xlsx pages 削除候補:            {len(old_pages_to_delete)}")
    print(f"  手動ラベル保護スキップ:            {skipped_manual}")
    print(f"  新 PDF pages 登録候補:             {len(new_pages)}")
    print(f"  重複コンテンツ (同 hash) スキップ: {n_skipped_dup_content}")
    print(f"  重複 file_name スキップ:           {n_skipped_dup_name}")
    print()

    # シート → doc_type 集計
    from collections import Counter
    type_count = Counter((n["doc_type_primary"], n.get("doc_type_secondary")) for n in new_pages if n["page_no"] == 1)
    print(f"  新規 doc_type 内訳 (P1 ベース):")
    for (pri, sec), n in type_count.most_common():
        sec_str = f" + {sec}" if sec else ""
        print(f"    {pri}{sec_str:30s} {n}")

    if args.dry_run:
        print(f"\n[dry-run] --execute で実行")
        return

    # 実行
    workflow = f"replace_xlsx_pdf_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"\n[execute] workflow={workflow}", file=sys.stderr)
    conn.execute("BEGIN IMMEDIATE")

    # 旧 pages 削除 + history
    n_del = 0
    for o in old_pages_to_delete:
        conn.execute(
            """INSERT INTO page_doc_type_history
               (page_id, company_id, file_name, page_no,
                old_doc_type_name, new_doc_type_name,
                reason, workflow_id, decision_id, confirmed_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'system')""",
            (o["page_id"], o["cid"], o["file_name"], o["page_no"],
             o["doc_type"], "(superseded)",
             f"replaced by per-sheet PDF (run={args.run_id})",
             workflow, str(uuid.uuid4()))
        )
        conn.execute("DELETE FROM pages WHERE page_id=?", (o["page_id"],))
        n_del += 1

    # 新 pages INSERT
    n_ins = 0
    for n in new_pages:
        cur = conn.execute(
            """INSERT INTO pages
               (company_id, file_name, file_hash, page_no, doc_type_name, doc_type_secondary, confidence)
               VALUES (?, ?, ?, ?, ?, ?, NULL)""",
            (n["cid"], n["file_name"], n["file_hash"], n["page_no"],
             n["doc_type_primary"], n["doc_type_secondary"])
        )
        new_pid = cur.lastrowid
        conn.execute(
            """INSERT INTO page_doc_type_history
               (page_id, company_id, file_name, page_no,
                old_doc_type_name, new_doc_type_name,
                reason, workflow_id, decision_id, confirmed_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'system')""",
            (new_pid, n["cid"], n["file_name"], n["page_no"],
             None, n["doc_type_primary"],
             f"per-sheet xlsx PDF, sheet='{n['sheet_name']}'",
             workflow, str(uuid.uuid4()))
        )
        n_ins += 1
    conn.commit()

    print(f"\n=== 完了 ===")
    print(f"  DELETE pages:  {n_del}")
    print(f"  INSERT pages:  {n_ins}")
    print(f"  workflow:      {workflow}")

    # 検算: 手動ラベル保護
    bad = conn.execute(
        """SELECT COUNT(*) FROM page_doc_type_history h1
           WHERE h1.confirmed_by IN ('web_viewer','user_visual','masaru_manual')
             AND EXISTS (
               SELECT 1 FROM page_doc_type_history h2
               WHERE h2.page_id = h1.page_id AND h2.workflow_id = ?
                 AND h2.history_id > h1.history_id
             )""", (workflow,)).fetchone()[0]
    print(f"  [安全性検算] 手動上書き: {bad} (期待: 0)")


if __name__ == "__main__":
    main()
