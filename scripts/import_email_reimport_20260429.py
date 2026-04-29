"""
EMAIL_REIMPORT 取込: data/inbox/email_reimport_20260429/<cid_name>/<file>.pdf を originals に移動 + DB INSERT。

ファイル名から doc_type を推論。不明は manual_review CSV へ書き出し。

Usage:
  python scripts/import_email_reimport_20260429.py            # dry-run
  python scripts/import_email_reimport_20260429.py --execute  # 実行
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import re
import shutil
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
INBOX = PROJECT / "data" / "inbox" / "email_reimport_20260429"
ORIGINALS = PROJECT / "data" / "originals"

DOC_FOLDER = {
    "取引申請書": "01_取引申請書",
    "建設業許可証": "02_建設業許可証",
    "決算書": "03_決算書",
    "工事経歴書": "04_工事経歴書",
    "労働安全衛生誓約書": "05_労働安全衛生誓約書",
    "資格略字一覧": "06_資格略字一覧",
    "取引先一覧表": "07_取引先一覧表",
    "労働者名簿": "08_労働者名簿",
    "会社案内": "99_受領バンドル",
}

# キーワード → doc_type 優先順位（前から）
DOC_KEYWORDS = [
    ("労働者名簿", ["労働者名簿", "名簿"]),
    ("労働安全衛生誓約書", ["誓約書", "労働安全衛生", "誓約"]),
    ("資格略字一覧", ["資格略字", "略字一覧", "資格者", "略字"]),
    ("取引先一覧表", ["取引先一覧", "得意先", "取引先", "主要取引先"]),
    ("建設業許可証", ["建設業許可", "許可証", "建設業"]),
    ("工事経歴書", ["工事経歴", "工事実績", "経歴書"]),
    ("決算書", ["決算書", "決算報告", "決算公告", "貸借対照", "損益計算"]),
    ("取引申請書", ["取引申請", "継続取引", "申請書", "御取引条件"]),
    ("会社案内", ["会社案内", "会社概要", "リーフレット"]),
]


def infer_doc_type(filename: str) -> str | None:
    """ファイル名から doc_type を推論。曖昧なら None。"""
    # 純粋な数字・日付プレフィックスを除去（例: 20260424_101820_ など）
    name = re.sub(r"^\d+_\d+_", "", filename)
    name = name.lower()
    for doc, kws in DOC_KEYWORDS:
        for kw in kws:
            if kw.lower() in name:
                return doc
    return None


def parse_company_folder(folder_name: str) -> tuple[str | None, str]:
    """C0029_有限会社大功建築 → (C0029, 有限会社大功建築)"""
    m = re.match(r"^(C\d{4})_(.+)$", folder_name)
    if m:
        return m.group(1), m.group(2)
    m = re.match(r"^未割当_(.+)$", folder_name)
    if m:
        return None, m.group(1)
    return None, folder_name


def find_originals_folder(cid: str, fujita_name: str) -> Path | None:
    """data/originals/<cid>_*/ を検索"""
    if cid:
        for p in ORIGINALS.glob(f"{cid}_*"):
            if p.is_dir() and "_archive_" not in str(p):
                return p
    return None


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    print(f"=== EMAIL_REIMPORT 取込 (mode={'EXECUTE' if args.execute else 'DRY-RUN'}) ===\n")

    if not INBOX.exists():
        print(f"NOT FOUND: {INBOX}")
        return

    plan = []
    manual_review = []
    no_company = []

    for cdir in sorted(INBOX.iterdir()):
        if not cdir.is_dir():
            continue
        cid, fujita_name = parse_company_folder(cdir.name)
        files = [f for f in cdir.iterdir() if f.is_file() and f.suffix.lower() in (".pdf", ".jpg", ".jpeg", ".png")]
        if not files:
            continue

        # originals 側のフォルダ
        if cid:
            orig_dir = find_originals_folder(cid, fujita_name)
            if orig_dir is None:
                # 新規作成パス
                orig_dir = ORIGINALS / cdir.name
                orig_status = "(新規作成)"
            else:
                orig_status = ""
        else:
            no_company.append((cdir.name, [f.name for f in files]))
            continue

        for f in files:
            doc = infer_doc_type(f.name)
            if doc is None:
                manual_review.append({"cid": cid, "fujita_name": fujita_name,
                                      "src": str(f), "filename": f.name})
                continue
            folder_cat = DOC_FOLDER.get(doc, "99_受領バンドル")
            year = "2026"
            dst_dir = orig_dir / folder_cat / year
            dst = dst_dir / f.name
            plan.append({
                "cid": cid, "fujita_name": fujita_name,
                "doc_type": doc, "src": str(f), "dst": str(dst),
                "orig_status": orig_status,
            })

    print(f"取込予定: {len(plan)} 件")
    print(f"manual_review (推論失敗): {len(manual_review)} 件")
    print(f"会社未割当 (cid 不明): {len(no_company)} 件\n")

    # 内訳表示
    from collections import Counter
    by_doc = Counter(p["doc_type"] for p in plan)
    print("=== doc_type 別件数 ===")
    for d, n in by_doc.most_common():
        print(f"  {d}: {n}")
    print()

    # CSV 出力 (確認用)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    plan_csv = PROJECT / "data" / f"email_reimport_plan_{ts}.csv"
    with plan_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["cid", "fujita_name", "doc_type", "src", "dst", "orig_status"])
        w.writeheader()
        w.writerows(plan)
    print(f"plan CSV: {plan_csv}")

    if manual_review:
        review_csv = PROJECT / "data" / f"email_reimport_manual_review_{ts}.csv"
        with review_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["cid", "fujita_name", "src", "filename"])
            w.writeheader()
            w.writerows(manual_review)
        print(f"manual_review CSV: {review_csv}")

    if no_company:
        print("\n=== cid 未割当 (originals に取り込まず) ===")
        for n, fs in no_company:
            print(f"  {n}")
            for fn in fs:
                print(f"    - {fn}")

    if not args.execute:
        print("\n[DRY-RUN] DB は変更されません。--execute で実行。")
        return

    # 実行
    conn = sqlite3.connect(str(DB))
    conn.execute("PRAGMA foreign_keys = OFF")
    inserted_pages = 0
    moved_files = 0
    workflow_id = f"email_reimport_{ts}"

    try:
        conn.execute("BEGIN IMMEDIATE")
        for p in plan:
            src = Path(p["src"])
            dst = Path(p["dst"])
            dst.parent.mkdir(parents=True, exist_ok=True)
            # 衝突回避
            if dst.exists():
                stem, ext = dst.stem, dst.suffix
                for n in range(2, 100):
                    cand = dst.parent / f"{stem}_rec{n:03d}{ext}"
                    if not cand.exists():
                        dst = cand
                        break
            shutil.move(str(src), str(dst))
            moved_files += 1

            # DB INSERT
            fhash = sha256_file(dst)
            conn.execute(
                "INSERT INTO pages (company_id, file_name, file_hash, page_no, "
                "doc_type_name, confidence, rotation, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (company_id, file_name, page_no) DO UPDATE SET "
                "  file_hash = COALESCE(pages.file_hash, excluded.file_hash)",
                (p["cid"], dst.name, fhash, 1, p["doc_type"], 1.0, 0,
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
            new_pid = conn.execute(
                "SELECT page_id FROM pages WHERE company_id=? AND file_name=? AND page_no=1",
                (p["cid"], dst.name)
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO page_doc_type_history "
                "(page_id, company_id, file_name, page_no, old_doc_type_name, "
                " new_doc_type_name, reason, workflow_id, decision_id, confirmed_by) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'email_reimport')",
                (new_pid, p["cid"], dst.name, 1, None, p["doc_type"],
                 f"EMAIL_REIMPORT 取込 {p['fujita_name']}", workflow_id, str(uuid.uuid4()))
            )
            inserted_pages += 1
        conn.commit()
        print(f"\n✓ commit 完了: {moved_files} ファイル move, {inserted_pages} pages INSERT")
    except Exception as e:
        conn.rollback()
        print(f"\n✗ rollback: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
