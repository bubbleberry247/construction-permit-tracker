"""
ページ画像ハッシュベースの重複検出 (read-only レポートのみ)。

各 PDF ページを 150dpi グレースケール画像化 → sha256
→ page_content_hashes テーブルに格納
→ 同 sha256 で異 file_name の重複候補を CSV 出力

物理 dedup は行わない (CSV 確認後、別フェーズで判断)。

Usage:
  python scripts/scan_page_content_hashes.py --build          # 全社 hash 算出
  python scripts/scan_page_content_hashes.py --build --company-id C0065
  python scripts/scan_page_content_hashes.py --report         # 重複候補 CSV
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pymupdf as fitz

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
sys.stdout.reconfigure(encoding="utf-8")

DPI = 150
HASH_METHOD = f"pixmap_grayscale_{DPI}dpi"


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


def page_pixmap_hash(pdf: Path, page_no: int, dpi: int = DPI) -> str | None:
    """指定ページを画像化 → グレースケール → sha256"""
    try:
        doc = fitz.open(pdf)
        if page_no > len(doc):
            doc.close()
            return None
        page = doc[page_no - 1]
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        pix = page.get_pixmap(matrix=mat, colorspace=fitz.csGRAY)
        data = pix.tobytes("png")
        doc.close()
        return hashlib.sha256(data).hexdigest()
    except Exception as e:
        print(f"  hash fail {pdf.name} p{page_no}: {e}", file=sys.stderr)
        return None


def cmd_build(args):
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    q = "SELECT page_id, company_id, file_name, page_no FROM pages"
    params = ()
    if args.company_id:
        q += " WHERE company_id=?"
        params = (args.company_id,)
    q += " ORDER BY company_id, file_name, page_no"

    targets = list(conn.execute(q, params).fetchall())
    print(f"全 page: {len(targets)}", file=sys.stderr)

    # 既に hash 済みは skip
    existing = {(r[0], r[1], r[2]) for r in conn.execute(
        "SELECT cid, file_name, page_no FROM page_content_hashes"
        + (" WHERE cid=?" if args.company_id else ""),
        params
    )}
    print(f"既 hash: {len(existing)} 件、新規対象: {len(targets) - len(existing)} 件", file=sys.stderr)

    pdf_cache = {}
    inserted = 0
    skipped = 0
    failed = 0

    conn.execute("BEGIN")
    try:
        for i, r in enumerate(targets, 1):
            cid, fn, pno = r["company_id"], r["file_name"], r["page_no"]
            if (cid, fn, pno) in existing:
                continue
            if not fn.lower().endswith(".pdf"):
                skipped += 1
                continue
            key = (cid, fn)
            if key not in pdf_cache:
                pdf_cache[key] = find_pdf(fn, cid)
            pdf = pdf_cache[key]
            if not pdf:
                skipped += 1
                continue
            h = page_pixmap_hash(pdf, pno)
            if not h:
                failed += 1
                continue
            conn.execute(
                "INSERT OR IGNORE INTO page_content_hashes (cid, file_name, page_no, sha256, hash_method) "
                "VALUES (?, ?, ?, ?, ?)",
                (cid, fn, pno, h, HASH_METHOD)
            )
            inserted += 1
            if i % 200 == 0:
                print(f"  進捗 {i}/{len(targets)} (insert={inserted})", file=sys.stderr)
        conn.commit()
        print(f"COMMIT OK: insert={inserted}, skip={skipped}, fail={failed}", file=sys.stderr)
    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}", file=sys.stderr)
        raise


def cmd_report(args):
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # 同 sha256 で複数行 = 重複候補
    rows = list(conn.execute("""
        SELECT h.sha256,
               h.cid,
               h.file_name,
               h.page_no,
               p.page_id,
               p.doc_type_name,
               EXISTS (
                 SELECT 1 FROM page_doc_type_history hh
                 WHERE hh.page_id=p.page_id AND hh.confirmed_by IN ('web_viewer','user_visual')
               ) AS has_manual
        FROM page_content_hashes h
        LEFT JOIN pages p ON p.company_id=h.cid AND p.file_name=h.file_name AND p.page_no=h.page_no
        WHERE h.sha256 IN (
          SELECT sha256 FROM page_content_hashes
          GROUP BY sha256
          HAVING COUNT(*) > 1
        )
        ORDER BY h.sha256, h.cid, h.file_name, h.page_no
    """))

    # 同一ハッシュ x 異 file_name のものだけ抽出 (異 file_name = 真の重複)
    by_hash = {}
    for r in rows:
        by_hash.setdefault(r["sha256"], []).append(dict(r))

    suspicious = {}
    for h, lst in by_hash.items():
        files = {r["file_name"] for r in lst}
        if len(files) > 1:  # 異なる file_name の重複
            suspicious[h] = lst

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = PROJECT / "output" / f"page_hash_duplicates_{ts}.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["sha256_short", "cid", "file_name", "page_no", "page_id",
                    "doc_type_name", "has_manual_edit", "group_size"])
        for h, lst in sorted(suspicious.items(), key=lambda x: -len(x[1])):
            for r in lst:
                w.writerow([h[:12], r["cid"], r["file_name"], r["page_no"],
                            r["page_id"], r["doc_type_name"],
                            "Y" if r["has_manual"] else "",
                            len(lst)])
    print(f"\n=== ハッシュ重複レポート ===")
    print(f"  異 file_name 重複グループ: {len(suspicious)}")
    print(f"  影響行数: {sum(len(lst) for lst in suspicious.values())}")
    print(f"  CSV: {out_csv.absolute()}")

    # 上位 5 グループサンプル
    print("\n=== 上位 5 グループ ===")
    for h, lst in sorted(suspicious.items(), key=lambda x: -len(x[1]))[:5]:
        print(f"\n  hash={h[:12]} ({len(lst)} 件)")
        for r in lst[:6]:
            mark = "[手動]" if r["has_manual"] else ""
            print(f"    {r['cid']} {r['file_name'][:40]} p{r['page_no']} type={r['doc_type_name']!s:15s} {mark}")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")
    ap.add_argument("--build", action="store_true", help="全社の page hash を算出")
    ap.add_argument("--report", action="store_true", help="重複候補 CSV 出力")
    ap.add_argument("--company-id")
    args = ap.parse_args()

    if args.build:
        cmd_build(args)
    elif args.report:
        cmd_report(args)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
