"""
Stage 4.2: inbox + 展開済ファイルを 145社マスタ照合で分類して CSV 出力。

DB 更新なし、read-only。出力 CSV にユーザーが approve 列を追加して
`scripts/import_classified_inbox.py` に渡す。

confidence:
  HIGH       - ファイル名から完全一致
  MEDIUM     - subsequence マッチ or ZIP 親名継承
  LOW        - sender_email から逆引き
  UNMATCHED  - どれでも紐付かない (archive 退避候補)

Usage:
  python scripts/classify_inbox.py
"""
from __future__ import annotations

import csv
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate_master_db_reconcile import (
    load_db, load_master, normalize, normalize_aggressive, is_subsequence_match,
)

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
INBOX = PROJECT / "data" / "inbox"
EXTRACT_BASE = PROJECT / "data" / "inbox_extracted_20260427"
DB = PROJECT / "data" / "permit_tracker.db"

# 書類カテゴリ判定ルール
DOC_TYPE_RULES = [
    (re.compile(r"御?取引条件|取引申請|新規[・･.]?継続|提出書類|チェックリスト", re.I), "取引申請書"),
    (re.compile(r"建設業許可|建築業許可|許可証|許可通知|許可証明", re.I), "建設業許可証"),
    (re.compile(r"決算|期\.pdf|期決算|前年度分|前々年度|貸借対照表|損益|財務|期分", re.I), "決算書"),
    (re.compile(r"工事経歴|kojikeirekisho|工事実績", re.I), "工事経歴書"),
    (re.compile(r"取引先一覧|主要取引|得意先|取引会社|取引先\.|仕入先|売上先|取引先一覧表", re.I), "取引先一覧表"),
    (re.compile(r"労働安全|安全衛生|誓約", re.I), "労働安全衛生誓約書"),
    (re.compile(r"資格.*一覧|有資格|資格者名簿|略字", re.I), "資格略字一覧"),
    (re.compile(r"労働者名簿|従業員名簿|社員名簿", re.I), "労働者名簿"),
]

# 社名候補から除外する一般用語
COMMON_WORDS = {
    "御取引条件等説明書", "取引申請書", "工事経歴書", "建設業許可証", "建築業許可証",
    "労働者名簿", "労働安全衛生誓約書", "資格略字一覧", "取引先一覧表", "取引先一覧",
    "決算書", "決算報告書", "主要取引先一覧", "主要取引先",
    "提出", "令和7年度", "令和7年", "令和6年", "弊社取引会社様", "建築業許可通知書",
    "建設業許可通知書", "新規", "継続取引", "継続取引申請書類", "新規・継続取引契約書",
    "pdf", "PDF", "Google スプレッドシート", "一覧表", "建設業許可", "建築業許可",
    "許可通知", "建設業許可証明書", "提出書類", "提出書類チェックリスト", "提出書類のご案内",
    "取引条件", "ご案内", "取引条件・提出書類のご案内", "リーフレット",
    "会社案内", "会社概要", "申請書", "誓約書", "前年度分決算書", "労働者名簿一覧",
    "得意先一覧 抜粋", "得意先一覧",
    "コピー御取引条件等説明書", "コピー御取引条件等説明書 (取引条件・提出書類のご案内)2026.2 (002)",
    "御取引条件(提出用",
    "取引申請書書類一式", "新規.継続取引申請書",
}


def guess_doc_type(name: str) -> str:
    for pat, dt in DOC_TYPE_RULES:
        if pat.search(name):
            return dt
    return ""


def extract_company_candidates(filename: str) -> list[str]:
    """ファイル名から会社名候補を抽出"""
    name = Path(filename).stem
    name = re.sub(r"^\d{8}_\d{6}_", "", name)  # timestamp prefix
    name = re.sub(r"^\d+\.\s*", "", name)  # 数字プレフィックス '2.', '3.0'
    name = re.sub(r"\.pdf$", "", name)
    name = re.sub(r"\(\d+\)$", "", name)  # ' (002)'

    candidates = []
    parts = re.split(r"[_＿]", name)
    for p in parts:
        p = p.strip().strip("()（）「」 ")
        if not p or p in COMMON_WORDS:
            continue
        # 法人形態を含むものは強候補
        if any(c in p for c in ["株式会社", "有限会社", "合同会社", "㈱", "㈲", "(株)", "（株）", "(有)", "（有）"]):
            candidates.append(p)
        # 漢字/カナ/英字 3 文字以上
        elif re.match(r"^[A-Za-zァ-ヿ一-龯]{3,}", p) and not re.match(r"^\d+$", p):
            if p not in COMMON_WORDS and not p.isdigit():
                candidates.append(p)
    return candidates


def find_company(name: str, master, db_records):
    """会社名を 145社マスタ + DB と照合"""
    if not name:
        return None, "", "UNMATCHED"
    fnorm = normalize_aggressive(name)
    if not fnorm:
        return None, "", "UNMATCHED"

    # 完全一致 (DB)
    for d in db_records:
        if d["_norm_aggressive"] == fnorm and d["status"] == "ACTIVE":
            return d["company_id"], d["official_name"], "HIGH"

    # subsequence マッチ (DB ACTIVE のみ)
    for d in db_records:
        if d["status"] != "ACTIVE":
            continue
        if is_subsequence_match(fnorm, d["_norm_aggressive"], min_len=4):
            return d["company_id"], d["official_name"], "MEDIUM"

    # マスタ完全一致 (DB に未登録の場合)
    for m in master:
        if m["_norm_aggressive"] == fnorm:
            return None, m["_name"], "MASTER_ONLY"

    return None, "", "UNMATCHED"


def lookup_by_sender(sender_email: str, conn) -> tuple[str | None, str]:
    """sender_email から company_emails / inbound_messages 経由で company_id 逆引き"""
    if not sender_email:
        return None, ""
    cur = conn.cursor()
    # company_emails (現在登録されている)
    r = cur.execute(
        "SELECT ce.company_id, c.official_name FROM company_emails ce "
        "JOIN companies c ON c.company_id=ce.company_id "
        "WHERE LOWER(ce.email)=LOWER(?) AND c.status='ACTIVE' LIMIT 1",
        (sender_email,),
    ).fetchone()
    if r:
        return r[0], r[1]
    # inbound_messages から (過去受信履歴)
    r = cur.execute(
        "SELECT im.company_id, c.official_name FROM inbound_messages im "
        "JOIN companies c ON c.company_id=im.company_id "
        "WHERE (LOWER(im.sender_email)=LOWER(?) OR LOWER(im.original_sender)=LOWER(?)) "
        "AND c.status='ACTIVE' "
        "ORDER BY im.received_at DESC LIMIT 1",
        (sender_email, sender_email),
    ).fetchone()
    if r:
        return r[0], r[1]
    return None, ""


def collect_targets() -> list[dict]:
    """inbox + 展開済の対象ファイルを収集"""
    targets = []

    # inbox 直下 (PDF/Excel)
    for p in sorted(INBOX.iterdir()):
        if p.is_file() and p.suffix.lower() in (".pdf", ".xlsx", ".xls"):
            targets.append({
                "source": "inbox",
                "path": str(p.relative_to(PROJECT)),
                "filename": p.name,
                "parent_zip": "",
            })

    # 展開済ファイル
    if EXTRACT_BASE.exists():
        for sub in sorted(EXTRACT_BASE.iterdir()):
            if not sub.is_dir():
                continue
            parent_zip = sub.name  # 親 ZIP 名 (例: 20260327_134524_御取引条件等説明書_株式会社イシハラ)
            for p in sorted(sub.rglob("*")):
                if p.is_file() and p.suffix.lower() in (".pdf", ".xlsx", ".xls"):
                    targets.append({
                        "source": "extracted",
                        "path": str(p.relative_to(PROJECT)),
                        "filename": p.name,
                        "parent_zip": parent_zip,
                    })

    return targets


def main():
    master = load_master()
    db_records = load_db(read_only=True)
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only = 1")

    targets = collect_targets()
    print(f"分類対象: {len(targets)} files\n")

    # sender_email を未処理 inbox PDF から推測 (ファイル名 prefix の timestamp で inbound_messages を逆引き)
    # 簡易には: inbox の元ファイル名 timestamp prefix → 同時刻に来たメッセージの sender_email
    # ただし複雑すぎる為、本実装では「parent_zip があれば親名から会社名を抽出」を優先

    results = []
    for t in targets:
        # 1. ファイル名から候補抽出
        cands = extract_company_candidates(t["filename"])
        chosen_cid, official, conf = None, "", "UNMATCHED"
        matched_cand = ""
        for c in cands:
            chosen_cid, official, conf = find_company(c, master, db_records)
            if chosen_cid or conf == "MASTER_ONLY":
                matched_cand = c
                break

        # 2. UNMATCHED の場合、parent_zip から会社名抽出 (継承ロジック)
        if not chosen_cid and t["parent_zip"]:
            zip_cands = extract_company_candidates(t["parent_zip"])
            for c in zip_cands:
                cid2, off2, conf2 = find_company(c, master, db_records)
                if cid2:
                    chosen_cid, official, conf = cid2, off2, "MEDIUM"
                    matched_cand = f"{c} (親ZIP)"
                    break
                elif conf2 == "MASTER_ONLY":
                    chosen_cid, official, conf = None, off2, "MASTER_ONLY"
                    matched_cand = f"{c} (親ZIP)"
                    break

        # 3. doc_type 判定 (ファイル名 + 親ZIP名)
        doc_type = guess_doc_type(t["filename"]) or guess_doc_type(t["parent_zip"])

        results.append({
            "source": t["source"],
            "path": t["path"],
            "filename": t["filename"],
            "parent_zip": t["parent_zip"],
            "cands": "|".join(cands),
            "matched_cand": matched_cand,
            "company_id": chosen_cid or "",
            "official_name": official,
            "doc_type": doc_type,
            "confidence": conf,
            "approved": "Y" if conf in ("HIGH", "MEDIUM") and chosen_cid else "",  # default approve for HIGH/MEDIUM
        })

    conn.close()

    # CSV 出力
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = PROJECT / "data" / f"stage4_classification_{ts}.csv"
    fieldnames = ["source", "path", "filename", "parent_zip", "cands", "matched_cand",
                  "company_id", "official_name", "doc_type", "confidence", "approved"]
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(results)
    print(f"CSV: {csv_path.name}\n")

    # サマリ
    conf_cnt = Counter(r["confidence"] for r in results)
    doc_cnt = Counter(r["doc_type"] for r in results if r["doc_type"])
    cid_cnt = Counter(r["company_id"] for r in results if r["company_id"])
    approved_cnt = sum(1 for r in results if r["approved"] == "Y")

    print(f"=== confidence 内訳 ===")
    for c, n in conf_cnt.most_common():
        print(f"  {c}: {n}")

    print(f"\n=== doc_type 判定 ===")
    for d, n in doc_cnt.most_common():
        print(f"  {d}: {n}")

    print(f"\n=== 会社別 (top 15) ===")
    for cid, n in cid_cnt.most_common(15):
        name = next((d["official_name"] for d in db_records if d["company_id"] == cid), "?")
        print(f"  {cid} {name}: {n} files")

    print(f"\n=== approved (default Y for HIGH/MEDIUM): {approved_cnt} / {len(results)} files ===")
    print(f"  UNMATCHED は CSV の approved 列を空のままにしてください")
    print(f"  LOW/MASTER_ONLY は手動で approved=Y を付ける場合は明示的にレビューしてください")


if __name__ == "__main__":
    main()
