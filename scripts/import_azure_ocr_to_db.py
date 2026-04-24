"""
Azure OCR CSV → permit_tracker.db 反映スクリプト。

対象テーブル:
  - pages: 各PDFの全ページを doc_type_name='建設業許可証' で INSERT
  - permits: 許可番号・行政庁・日付等を INSERT (current_flag)
  - permit_trades: 業種別に INSERT

Usage:
    python scripts/import_azure_ocr_to_db.py --csv output/staging_permits_azure_YYYYMMDD_HHMMSS.csv [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB = PROJECT_ROOT / "data" / "permit_tracker.db"
STAGING = PROJECT_ROOT / "data" / "staging"
ORIGINALS = PROJECT_ROOT / "data" / "originals"


def sha256_of(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def find_pdf(filename: str, company_id: str) -> Path | None:
    """staging/, originals/ からPDFを探す"""
    candidates = [
        STAGING / f"{company_id}_*" / filename,
        ORIGINALS / f"{company_id}_*" / filename,
    ]
    for root in [STAGING, ORIGINALS]:
        for cdir in root.glob(f"{company_id}_*"):
            p = cdir / filename
            if p.exists():
                return p
    return None


def page_count(pdf: Path) -> int:
    try:
        import pymupdf as fitz
        doc = fitz.open(pdf)
        n = doc.page_count
        doc.close()
        return n
    except Exception:
        return 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    csv_path = Path(args.csv)
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    # OK/REVIEW_NEEDED 両方取り込む
    target = [r for r in rows if r["parse_status"] in ("OK", "REVIEW_NEEDED")]
    print(f"対象: {len(target)}件 / CSV合計: {len(rows)}件")

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    pages_ins = 0
    pages_skip = 0
    permits_ins = 0
    permits_upd = 0
    trades_ins = 0

    for r in target:
        cid = r["company_id"]
        src = r["source_file"]
        pdf = find_pdf(src, cid)
        if not pdf:
            print(f"  ✗ PDF not found: {cid} {src}")
            continue

        n_pages = page_count(pdf)
        fhash = sha256_of(pdf)

        # ---- pages ----
        for pno in range(1, n_pages + 1):
            existing = conn.execute(
                "SELECT page_id FROM pages WHERE company_id=? AND file_name=? AND page_no=?",
                (cid, src, pno),
            ).fetchone()
            if existing:
                # 既存あれば doc_type_name 更新
                if not args.dry_run:
                    conn.execute(
                        "UPDATE pages SET doc_type_name=?, file_hash=?, confidence=? WHERE page_id=?",
                        ("建設業許可証", fhash, 0.95, existing["page_id"]),
                    )
                pages_skip += 1
            else:
                if not args.dry_run:
                    conn.execute(
                        "INSERT INTO pages (company_id, file_name, file_hash, page_no, doc_type_name, confidence, rotation) "
                        "VALUES (?, ?, ?, ?, ?, ?, 0)",
                        (cid, src, fhash, pno, "建設業許可証", 0.95),
                    )
                pages_ins += 1

        # ---- permits ----
        permit_num = r.get("permit_number_full") or ""
        if permit_num:
            existing = conn.execute(
                "SELECT permit_id FROM permits WHERE company_id=? AND permit_number=?",
                (cid, permit_num),
            ).fetchone()
            if existing:
                if not args.dry_run:
                    conn.execute(
                        "UPDATE permits SET permit_authority=?, permit_category=?, permit_year=?, "
                        "issue_date=?, expiry_date=?, current_flag=1, updated_at=datetime('now','localtime'), "
                        "source='azure_di_ocr' WHERE permit_id=?",
                        (r.get("permit_authority_name"), r.get("permit_category"),
                         r.get("permit_year"), r.get("issue_date") or None,
                         r.get("expiry_date") or None, existing["permit_id"]),
                    )
                permit_id = existing["permit_id"]
                permits_upd += 1
            else:
                if not args.dry_run:
                    cur = conn.execute(
                        "INSERT INTO permits (company_id, permit_number, permit_authority, permit_category, "
                        "permit_year, issue_date, expiry_date, current_flag, source) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'azure_di_ocr')",
                        (cid, permit_num, r.get("permit_authority_name"), r.get("permit_category"),
                         r.get("permit_year"), r.get("issue_date") or None,
                         r.get("expiry_date") or None),
                    )
                    permit_id = cur.lastrowid
                else:
                    permit_id = -1
                permits_ins += 1

            # ---- permit_trades ----
            trades_str = r.get("trade_categories") or ""
            trades = [t.strip() for t in trades_str.split("|") if t.strip()]
            if not args.dry_run and permit_id > 0:
                # 既存 trades を削除して再登録（UPDATE 的に）
                conn.execute("DELETE FROM permit_trades WHERE permit_id=?", (permit_id,))
                for tr in trades:
                    conn.execute(
                        "INSERT INTO permit_trades (permit_id, trade_name) VALUES (?, ?)",
                        (permit_id, tr),
                    )
                    trades_ins += 1
            else:
                trades_ins += len(trades)

    if not args.dry_run:
        conn.commit()
    print(f"\n=== 結果 (dry_run={args.dry_run}) ===")
    print(f"  pages INSERT: {pages_ins}  UPDATE: {pages_skip}")
    print(f"  permits INSERT: {permits_ins}  UPDATE: {permits_upd}")
    print(f"  permit_trades 総INSERT: {trades_ins}")

    conn.close()


if __name__ == "__main__":
    main()
