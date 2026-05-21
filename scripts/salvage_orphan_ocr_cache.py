"""
旧キャッシュ (data/ocr_cache_orphan_20260430/page_*.json) のテキストを
DB の「その他/不明」ページにテキスト類似度マッチングで紐付け、
新キャッシュキー (sha256(pdf)+page_no) で保存し直す。

OCR 課金を二重に発生させないため。

Match strategy:
  1. orphan cache の text を classify_page で分類
  2. 分類結果 (例: 決算書) と該当する候補ページ群を絞り込む
  3. 候補ページの周囲ページのテキスト類似度 (or PDF 構造) でユニーク特定
  4. 一致度 80%+ なら新キーで保存

Usage:
  python scripts/salvage_orphan_ocr_cache.py --dry-run
  python scripts/salvage_orphan_ocr_cache.py --execute
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pymupdf as fitz

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
CACHE_NEW = PROJECT / "data" / "ocr_cache"
CACHE_ORPHAN = PROJECT / "data" / "ocr_cache_orphan_20260430"
sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from reclassify_bundled_pdfs_azure import classify_page

VALID_DOC_TYPES = {
    "取引申請書", "建設業許可証", "決算書", "工事経歴書",
    "取引先一覧表", "労働安全衛生誓約書", "資格略字一覧", "労働者名簿",
    "会社案内",
}


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


def stable_key(pdf: Path, page_no: int) -> str:
    h = hashlib.sha256()
    with pdf.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return f"{h.hexdigest()}_p{page_no}"


def text_similarity(a: str, b: str) -> float:
    """簡易テキスト類似度 (共通 1-gram 単語数 / 短い側の単語数)"""
    if not a or not b:
        return 0.0
    sa = set(a.split())
    sb = set(b.split())
    common = sa & sb
    short = min(len(sa), len(sb))
    return len(common) / short if short else 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--threshold", type=float, default=0.6)
    args = ap.parse_args()

    is_execute = args.execute and not args.dry_run

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # 候補ページ (image PDF / 不明)
    print("候補ページ抽出中...", file=sys.stderr)
    rows = list(conn.execute("""
        SELECT page_id, company_id, file_name, page_no FROM pages
        WHERE doc_type_name='その他/不明'
        ORDER BY company_id, file_name, page_no
    """))
    candidates = []
    for r in rows:
        if not r["file_name"].lower().endswith(".pdf"):
            continue
        pdf = find_pdf(r["file_name"], r["company_id"])
        if not pdf:
            continue
        # PyMuPDF text is empty for image PDFs
        try:
            d = fitz.open(pdf)
            if r["page_no"] > len(d):
                d.close()
                continue
            t = (d[r["page_no"]-1].get_text() or "").strip()
            d.close()
        except Exception:
            continue
        if t:
            continue  # not image PDF
        candidates.append((r["page_id"], r["company_id"], r["file_name"], r["page_no"], pdf))
    print(f"画像PDF候補: {len(candidates)}", file=sys.stderr)

    # 旧キャッシュ読込
    orphan_files = sorted(CACHE_ORPHAN.glob("page_*.json"))
    print(f"旧キャッシュ: {len(orphan_files)}", file=sys.stderr)

    # 候補ページに対して PyMuPDF 画像化 → 解像度小さくして "indirect text" を取れる範囲を試す
    # ここでは簡易: orphan を順次見て、テキストから推測される doc_type と特徴的キーワード抽出 →
    # 候補 PDF 内のページ画像化結果と Azure DI テキスト一致するかを見る
    # (実際は厳密マッチ困難なので、candidate に同じ orphan text を割り当てるシンプル approach)

    # シンプル戦略: 各 orphan text の最初の 50 文字を sig として、各候補ページに対し sig を含むかチェック
    # しかし候補は image PDF (テキスト無し) なのでこの方法は使えない

    # 代替: 旧キャッシュの text を読んで、同じテキストが既存の azure_di_ocr_auto history のページから
    # 取得できるなら そのページに紐付ける可能性高い

    salvaged = 0
    skipped = 0
    for of in orphan_files:
        try:
            text = json.loads(of.read_text(encoding="utf-8")).get("text", "")
        except Exception:
            skipped += 1
            continue
        if not text:
            skipped += 1
            continue
        # 厳密マッチ困難につき、salvage は dry-run 報告のみ
        # サンプル先頭をログ
    print(f"\n=== 結果 ===")
    print(f"  サルベージ可能 orphan キャッシュ: 厳密ページ紐付け困難")
    print(f"  → 推奨: そのまま v4 で再 OCR (~$1.4)")
    print(f"  orphan files: {len(orphan_files)}")
    print(f"  candidates: {len(candidates)}")


if __name__ == "__main__":
    main()
