"""
多ページ一括PDF（御取引条件等説明書_XXX.pdf 等）を Azure DI prebuilt-layout で
全ページOCR → ルールベースで page.doc_type_name を再分類。

GPT Vision 不使用。OCRテキストの内容キーワードマッチのみで分類する。

Usage:
    op run --env-file config/azure_di.env.op -- python scripts/reclassify_bundled_pdfs_azure.py [--dry-run] [--limit N]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB = PROJECT_ROOT / "data" / "permit_tracker.db"
STAGING = PROJECT_ROOT / "data" / "staging"
ORIGINALS = PROJECT_ROOT / "data" / "originals"
OCR_CACHE = PROJECT_ROOT / "data" / "ocr_cache"
OCR_CACHE.mkdir(exist_ok=True)

BUNDLE_PATTERN = re.compile(r"御取引条件|継続取引|新規・継続|取引申請書類|取引申請書|取引申請|提出書類")

# ---------------------------------------------------------------------------
# ルールベース分類器（内容キーワードマッチ、優先順）
# ---------------------------------------------------------------------------
# 1ページOCRテキストから doc_type を返す
def classify_page(text: str) -> tuple[str, float]:
    """(doc_type_name, confidence) を返す"""
    t = text

    # 建設業許可証: 許可番号パターン or 典型キーワード
    if re.search(r"許可\s*[（(]\s*[般特]\s*[–\-―—ー]+\s*\d+", t):
        return ("建設業許可証", 0.95)
    if re.search(r"(建設業.{0,4}許可番号|建設業法第 ?3 ?条|一般建設業について|特定建設業について)", t):
        return ("建設業許可証", 0.90)

    # 決算書（拡張：「決算報告書」「第X期」「工事原価報告書」等）
    if re.search(r"決\s*算\s*報\s*告\s*書|決\s*算\s*書", t) and re.search(r"第\s*\d+\s*期|令和|平成|至\s*令和|自\s*令和", t):
        return ("決算書", 0.9)
    if re.search(r"工\s*事\s*原\s*価\s*報\s*告\s*書", t):
        return ("決算書", 0.95)
    fin_hits = 0
    for kw in ["貸借対照表", "損益計算書", "製造原価報告書", "株主資本等変動計算書",
               "キャッシュ・フロー計算書", "販売費及び一般管理費", "資産合計", "負債の部",
               "純資産の部", "流動資産", "流動負債", "営業利益", "経常利益", "当期純利益",
               "個別注記表", "材料費", "労務費", "外注費", "経費合計", "期首材料",
               "期末材料", "売上原価", "売上総利益", "棚卸資産", "前期繰越金",
               "株主資本", "資本準備金", "利益剰余金"]:
        if kw in t:
            fin_hits += 1
    if fin_hits >= 2:
        return ("決算書", 0.9)
    if fin_hits == 1 and any(k in t for k in ["第 ", "期 ", "令和", "平成"]) and re.search(r"\d{1,3}(,\d{3})+", t):
        return ("決算書", 0.8)

    # 工事経歴書
    if re.search(r"工\s*事\s*経\s*歴\s*書", t):
        return ("工事経歴書", 0.95)
    work_hits = 0
    for kw in ["工事名", "発注者", "工事場所", "工期", "請負代金", "請負金額", "完成年月", "注文者", "着工年月"]:
        if kw in t:
            work_hits += 1
    if work_hits >= 3:
        return ("工事経歴書", 0.85)

    # 労働者名簿
    if re.search(r"労\s*働\s*者\s*名\s*簿|従\s*業\s*員\s*名\s*簿", t):
        return ("労働者名簿", 0.95)
    labor_hits = 0
    for kw in ["氏 名", "氏名", "生年月日", "住 所", "住所", "雇入年月日", "従事する業務"]:
        if kw in t:
            labor_hits += 1
    if labor_hits >= 3 and "資格" not in t[:100]:
        return ("労働者名簿", 0.80)

    # 労働安全衛生誓約書
    if re.search(r"労\s*働\s*安\s*全|衛\s*生\s*誓\s*約|安\s*全\s*衛\s*生\s*誓\s*約", t):
        return ("労働安全衛生誓約書", 0.95)
    if "誓約書" in t and any(k in t for k in ["労働安全", "安全衛生", "災害", "防止"]):
        return ("労働安全衛生誓約書", 0.85)

    # 資格者名簿・資格略字一覧（拡張）
    if re.search(r"資\s*格\s*者\s*名\s*簿|技\s*能\s*者\s*名\s*簿|有\s*資\s*格\s*者|資\s*格\s*略\s*字\s*一\s*覧|資\s*格\s*略\s*字", t):
        return ("資格略字一覧", 0.95)
    if re.search(r"(電気工事士|電工|主任技術者|危険物取扱|一級建築士|二級建築士|一級建築施工|二級建築施工|一級土木|二級土木|玉掛|クレーン運転)", t) and \
       re.search(r"(【\s*免\s*許\s*】|免\s*許|資格)", t):
        return ("資格略字一覧", 0.85)
    if "資格" in t and re.search(r"(取得年月日|合格年月日|番号)", t) and "名簿" in t:
        return ("資格略字一覧", 0.85)

    # 取引先一覧表
    if re.search(r"(主\s*要\s*)?取\s*引\s*先\s*一\s*覧|得\s*意\s*先\s*一\s*覧|仕\s*入\s*先\s*一\s*覧|売\s*上\s*先|仕\s*入\s*先|主\s*要\s*取\s*引\s*先", t):
        return ("取引先一覧表", 0.9)
    if "取引先" in t and len(re.findall(r"(株式会社|有限会社|\(株\)|㈱|\(有\)|㈲)", t)) >= 3:
        return ("取引先一覧表", 0.8)

    # 会社案内
    if re.search(r"会\s*社\s*案\s*内|会\s*社\s*概\s*要|会\s*社\s*プ\s*ロ\s*フ", t):
        return ("会社案内", 0.9)
    profile_hits = 0
    for kw in ["沿革", "創立", "創業", "設立", "資本金", "事業内容", "役員", "営業所", "組織図", "代表者", "本社", "支店", "所在地"]:
        if kw in t:
            profile_hits += 1
    if profile_hits >= 3:
        return ("会社案内", 0.8)

    # 取引申請書 / 提出書類チェックリスト
    if re.search(r"取\s*引\s*条\s*件\s*等\s*説\s*明\s*書|新\s*規\s*取\s*引|継\s*続\s*取\s*引|取\s*引\s*申\s*請\s*書|提\s*出\s*書\s*類\s*チェック|申\s*請\s*日", t):
        return ("取引申請書", 0.90)
    if "提出書類" in t or "チェックリスト" in t:
        return ("取引申請書", 0.75)
    # カバー/表紙: 御中 + 担当者 + 申請書類系
    if "御中" in t and re.search(r"申\s*請|取\s*引", t):
        return ("取引申請書", 0.70)

    return ("その他/不明", 0.3)


# ---------------------------------------------------------------------------
# Azure DI OCR
# ---------------------------------------------------------------------------
def sha256_of(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def azure_ocr_pages(pdf_path: Path) -> list[str]:
    """PDF全ページをOCRして、ページごとのテキストリストを返す。キャッシュ対応。"""
    h = sha256_of(pdf_path)
    cache_file = OCR_CACHE / f"{h}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text(encoding="utf-8"))

    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
    from azure.core.credentials import AzureKeyCredential

    endpoint = os.environ["AZURE_DI_ENDPOINT"]
    key = os.environ["AZURE_DI_KEY"]
    client = DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))
    with pdf_path.open("rb") as f:
        poller = client.begin_analyze_document(
            "prebuilt-layout", AnalyzeDocumentRequest(bytes_source=f.read())
        )
    result = poller.result()
    pages_text = []
    for page in result.pages or []:
        pages_text.append("\n".join(line.content for line in (page.lines or [])))

    cache_file.write_text(json.dumps(pages_text, ensure_ascii=False), encoding="utf-8")
    return pages_text


# ---------------------------------------------------------------------------
# 対象PDF列挙
# ---------------------------------------------------------------------------
def find_pdf(filename: str, company_id: str) -> Path | None:
    for root in [STAGING, ORIGINALS]:
        for cdir in root.glob(f"{company_id}_*"):
            p = cdir / filename
            if p.exists():
                return p
    return None


def list_bundled_targets(conn) -> list[tuple[str, str, int]]:
    """(company_id, file_name, max_page_no) のリストを返す"""
    out = []
    seen = set()
    for r in conn.execute("SELECT DISTINCT company_id, file_name FROM pages"):
        if BUNDLE_PATTERN.search(r["file_name"]):
            key = (r["company_id"], r["file_name"])
            if key in seen:
                continue
            seen.add(key)
            max_pn = conn.execute(
                "SELECT MAX(page_no) FROM pages WHERE company_id=? AND file_name=?",
                (r["company_id"], r["file_name"]),
            ).fetchone()[0]
            out.append((r["company_id"], r["file_name"], max_pn or 1))
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="処理ファイル数上限（0=無制限）")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    targets = list_bundled_targets(conn)
    if args.limit:
        targets = targets[: args.limit]
    print(f"対象: {len(targets)}ファイル", file=sys.stderr)

    total_pages = 0
    total_updated = 0
    skipped_pdf_not_found = 0

    for i, (cid, fn, max_pn) in enumerate(targets, 1):
        pdf = find_pdf(fn, cid)
        if not pdf:
            print(f"[{i}/{len(targets)}] ✗ PDF not found: {cid} {fn}", file=sys.stderr)
            skipped_pdf_not_found += 1
            continue

        print(f"[{i}/{len(targets)}] {cid} {fn[:60]}  ({max_pn}p)", file=sys.stderr)
        try:
            page_texts = azure_ocr_pages(pdf)
        except Exception as e:
            print(f"    ✗ OCR失敗: {e}", file=sys.stderr)
            continue

        for pno, ptxt in enumerate(page_texts, 1):
            new_type, new_conf = classify_page(ptxt)
            existing = conn.execute(
                "SELECT page_id, doc_type_name FROM pages WHERE company_id=? AND file_name=? AND page_no=?",
                (cid, fn, pno),
            ).fetchone()
            if not existing:
                # 新規INSERT（ZIP展開等でDB未登録の場合）
                if not args.dry_run:
                    conn.execute(
                        "INSERT INTO pages (company_id, file_name, page_no, doc_type_name, confidence, rotation) "
                        "VALUES (?, ?, ?, ?, ?, 0)",
                        (cid, fn, pno, new_type, new_conf),
                    )
                total_updated += 1
            elif existing["doc_type_name"] != new_type:
                if not args.dry_run:
                    conn.execute(
                        "UPDATE pages SET doc_type_name=?, confidence=? WHERE page_id=?",
                        (new_type, new_conf, existing["page_id"]),
                    )
                total_updated += 1
            else:
                # 同じタグなら confidence だけ更新
                if not args.dry_run:
                    conn.execute(
                        "UPDATE pages SET confidence=? WHERE page_id=?",
                        (new_conf, existing["page_id"]),
                    )
            total_pages += 1

        if not args.dry_run:
            conn.commit()

    print(f"\n=== 完了 ===", file=sys.stderr)
    print(f"  処理ページ: {total_pages}", file=sys.stderr)
    print(f"  UPDATE/INSERT: {total_updated}", file=sys.stderr)
    print(f"  PDF not found: {skipped_pdf_not_found}", file=sys.stderr)

    # 更新後分布
    from collections import Counter
    dt_cnt = Counter(r["doc_type_name"] for r in conn.execute("SELECT doc_type_name FROM pages"))
    print("\n=== 更新後 doc_type_name 分布 ===", file=sys.stderr)
    for k, v in dt_cnt.most_common():
        print(f"  {k!r}: {v}ページ", file=sys.stderr)


if __name__ == "__main__":
    main()
