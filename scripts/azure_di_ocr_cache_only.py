"""
Phase 8 Step 1: Azure DI OCR で no-text ページを全件 OCR して cache のみ更新する。
DB は一切触らない。後段の shadow 分類器が cache を読み取って再分類。

GPT-5.5 推奨「OCR は広く、promote は狭く」に基づく。

Usage:
  op run --env-file config/azure_di.env.op -- python scripts/azure_di_ocr_cache_only.py --dry-run
  op run --env-file config/azure_di.env.op -- python scripts/azure_di_ocr_cache_only.py --execute

  # 範囲限定
  op run --env-file config/azure_di.env.op -- python scripts/azure_di_ocr_cache_only.py --execute --company-ids C0079,C0147
  op run --env-file config/azure_di.env.op -- python scripts/azure_di_ocr_cache_only.py --execute --limit 100

  # retry pass (failed list 流用)
  op run --env-file config/azure_di.env.op -- python scripts/azure_di_ocr_cache_only.py --execute --retry-from data/ocr_failed_<TS>.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from datetime import datetime
from pathlib import Path

import pymupdf as fitz

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
OCR_CACHE = PROJECT / "data" / "ocr_cache"
ORIGINALS = PROJECT / "data" / "originals"
OCR_CACHE.mkdir(exist_ok=True)
sys.stdout.reconfigure(encoding="utf-8")

OCR_TIMEOUT_S = 45
OCR_RETRY_TIMEOUT_S = 180
COMMIT_EVERY = 10


def find_pdf(file_name: str, cid: str) -> Path | None:
    if file_name.lower().endswith(".xlsx"):
        alt = file_name[:-5] + ".pdf"
        for c in ORIGINALS.glob(f"{cid}_*"):
            for p in c.rglob(alt):
                if p.is_file():
                    return p
    for c in ORIGINALS.glob(f"{cid}_*"):
        for p in c.rglob(file_name):
            if p.is_file():
                return p
        for p in c.rglob(f"*{file_name}"):
            if p.is_file():
                return p
    return None


_pdf_hash_cache: dict[str, str] = {}


def pdf_sha256(pdf: Path) -> str:
    k = str(pdf.resolve())
    if k in _pdf_hash_cache:
        return _pdf_hash_cache[k]
    h = hashlib.sha256()
    with pdf.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    _pdf_hash_cache[k] = h.hexdigest()
    return _pdf_hash_cache[k]


def page_text_pymupdf(pdf: Path, page_no: int) -> str:
    try:
        doc = fitz.open(pdf)
        if page_no > len(doc):
            doc.close()
            return ""
        t = (doc[page_no - 1].get_text() or "").strip()
        doc.close()
        return t
    except Exception:
        return ""


def extract_single_page_pdf_bytes(pdf: Path, page_no: int) -> bytes:
    src = fitz.open(pdf)
    if page_no > len(src):
        src.close()
        raise ValueError(f"page_no {page_no} > {len(src)}")
    out_doc = fitz.open()
    out_doc.insert_pdf(src, from_page=page_no - 1, to_page=page_no - 1)
    data = out_doc.tobytes()
    out_doc.close()
    src.close()
    return data


_GLOBAL_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ocr")


def _azure_ocr_inner(pdf_bytes: bytes) -> str:
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
    from azure.core.credentials import AzureKeyCredential

    endpoint = os.environ["AZURE_DI_ENDPOINT"]
    key = os.environ["AZURE_DI_KEY"]
    client = DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))
    poller = client.begin_analyze_document(
        "prebuilt-layout", AnalyzeDocumentRequest(bytes_source=pdf_bytes)
    )
    result = poller.result()
    parts = []
    for page in result.pages or []:
        for line in (page.lines or []):
            parts.append(line.content)
    return "\n".join(parts)


def azure_ocr_single(pdf_bytes: bytes, cache_path: Path, timeout_s: int) -> str:
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8")).get("text", "")
    future = _GLOBAL_EXECUTOR.submit(_azure_ocr_inner, pdf_bytes)
    try:
        text = future.result(timeout=timeout_s)
    except FuturesTimeout:
        future.cancel()
        raise TimeoutError(f"Azure DI exceeded {timeout_s}s")
    cache_path.write_text(json.dumps({"text": text}, ensure_ascii=False), encoding="utf-8")
    return text


def collect_targets(conn, args) -> list[dict]:
    """no-text ページを集める。ファイル名ベースで現状ラベル無関係に全部。"""
    where = []
    params: list = []
    if args.company_ids:
        cids = [c.strip() for c in args.company_ids.split(",") if c.strip()]
        if cids:
            where.append(f"p.company_id IN ({','.join('?' for _ in cids)})")
            params.extend(cids)

    q = """SELECT p.page_id, p.company_id, p.file_name, p.page_no, p.doc_type_name
           FROM pages p"""
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY p.company_id, p.file_name, p.page_no"

    targets = []
    pdf_cache: dict[str, Path | None] = {}
    for r in conn.execute(q, params).fetchall():
        ck = f'{r["company_id"]}/{r["file_name"]}'
        if ck in pdf_cache:
            pdf = pdf_cache[ck]
        else:
            pdf = find_pdf(r["file_name"], r["company_id"])
            pdf_cache[ck] = pdf
        if pdf is None:
            continue
        # PyMuPDF text 確認
        ptext = page_text_pymupdf(pdf, r["page_no"])
        if len(ptext) >= 30:
            continue
        # 既存キャッシュ確認 (per-page)
        h = pdf_sha256(pdf)
        per_page = OCR_CACHE / f"page_{h}_p{r['page_no']}.json"
        if per_page.exists():
            cached = json.loads(per_page.read_text(encoding="utf-8")).get("text", "")
            if len(cached) >= 10:
                continue
        # 既存キャッシュ確認 (full-doc list 形式)
        full_cache = OCR_CACHE / f"{h}.json"
        if full_cache.exists():
            try:
                arr = json.loads(full_cache.read_text(encoding="utf-8"))
                if isinstance(arr, list) and 1 <= r["page_no"] <= len(arr):
                    cached_full = arr[r["page_no"] - 1] or ""
                    if len(cached_full) >= 10:
                        continue
            except Exception:
                pass
        targets.append({
            "page_id": r["page_id"],
            "company_id": r["company_id"],
            "file_name": r["file_name"],
            "page_no": r["page_no"],
            "doc_type_name": r["doc_type_name"],
            "pdf": str(pdf),
            "cache_key": h,
        })
    return targets


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--company-ids")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--retry-from", help="failed list JSON ファイル")
    ap.add_argument("--timeout-s", type=int, default=OCR_TIMEOUT_S)
    args = ap.parse_args()

    if not args.dry_run and not args.execute:
        print("ERROR: --dry-run か --execute を指定してください", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # ターゲット取得
    if args.retry_from:
        retry_data = json.loads(Path(args.retry_from).read_text(encoding="utf-8"))
        targets = retry_data
        print(f"[retry] {len(targets)} pages from {args.retry_from}", file=sys.stderr)
    else:
        targets = collect_targets(conn, args)
    if args.limit:
        targets = targets[:args.limit]

    n_target = len(targets)
    print(f"[init] OCR ターゲット: {n_target} ページ", file=sys.stderr)

    # 内訳サマリ
    by_label: dict[str, int] = {}
    for t in targets:
        l = t["doc_type_name"] or ""
        by_label[l] = by_label.get(l, 0) + 1
    print("[init] 現在ラベル別:", file=sys.stderr)
    for l, n in sorted(by_label.items(), key=lambda x: -x[1])[:15]:
        print(f"  {l:25s} {n}", file=sys.stderr)

    cost_est = n_target * 0.0015
    print(f"[init] 推定コスト: ${cost_est:.2f} (S0)", file=sys.stderr)

    if args.dry_run:
        print(f"\n[dry-run] 実行するには --execute を指定してください")
        return

    # Azure 認証確認
    if "AZURE_DI_ENDPOINT" not in os.environ:
        print("ERROR: AZURE_DI_ENDPOINT が未設定。op run でラップして実行してください", file=sys.stderr)
        sys.exit(2)

    n_ok = 0
    n_cached = 0
    n_timeout = 0
    n_fail = 0
    n_short = 0
    failed_list = []
    by_text_len = {"0": 0, "1-49": 0, "50-149": 0, "150+": 0}

    for i, t in enumerate(targets, 1):
        cache_key = t["cache_key"]
        cache_path = OCR_CACHE / f"page_{cache_key}_p{t['page_no']}.json"
        try:
            pdf_bytes = extract_single_page_pdf_bytes(Path(t["pdf"]), t["page_no"])
        except Exception as e:
            n_fail += 1
            failed_list.append({**t, "error": f"extract:{str(e)[:100]}"})
            continue

        try:
            text = azure_ocr_single(pdf_bytes, cache_path, args.timeout_s)
            if cache_path.exists() and len(text) > 0:
                n_ok += 1
            else:
                n_cached += 1  # cached but empty
        except TimeoutError:
            n_timeout += 1
            failed_list.append({**t, "error": f"timeout:{args.timeout_s}s"})
            continue
        except Exception as e:
            n_fail += 1
            failed_list.append({**t, "error": f"api:{str(e)[:100]}"})
            continue

        # text length 分布
        L = len(text or "")
        if L == 0:
            by_text_len["0"] += 1
            n_short += 1
        elif L < 50:
            by_text_len["1-49"] += 1
            n_short += 1
        elif L < 150:
            by_text_len["50-149"] += 1
        else:
            by_text_len["150+"] += 1

        if i % 25 == 0:
            print(f"  進捗 {i}/{n_target}: ok={n_ok}, timeout={n_timeout}, fail={n_fail}, short={n_short}",
                  file=sys.stderr, flush=True)

    print()
    print(f"=== 完了 ===")
    print(f"  OK (新規 OCR):  {n_ok}")
    print(f"  Timeout:        {n_timeout}")
    print(f"  Failed:         {n_fail}")
    print(f"  Short text:     {n_short}")
    print(f"\n=== text 長 分布 ===")
    for k, v in by_text_len.items():
        print(f"  {k:8s} : {v}")

    if failed_list:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fail_path = PROJECT / "data" / f"ocr_failed_{ts}.json"
        fail_path.write_text(json.dumps(failed_list, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n失敗リスト: {fail_path}")
        print(f"retry: op run --env-file config/azure_di.env.op -- python scripts/azure_di_ocr_cache_only.py --execute --retry-from {fail_path} --timeout-s 180")


if __name__ == "__main__":
    main()
