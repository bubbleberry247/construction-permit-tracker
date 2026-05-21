"""
画像 PDF (テキスト 0 字) で「その他/不明」分類のページを Azure DI で OCR し再分類。

設計鉄則 (GPT-5.5 推奨):
  1. 手動修正 (web_viewer / user_visual) は絶対保護
  2. text_len == 0 (画像PDF) のみ対象 = 既存テキスト層は触らない
  3. history append-only (workflow=azure_di_ocr_auto_<TS>)
  4. 改善時のみ UPDATE: old='その他/不明' AND new IN (有効書類)
  5. dry-run 先行
  6. ページ単位 PDF 切り出し → 単頁 OCR (キャッシュ流用)
  7. COMMIT_EVERY ごとに commit (途中失敗時の復旧用)
  8. 共有 ThreadPoolExecutor で hard timeout
  9. キャッシュキー = sha256(元 PDF ファイル) + page_no で決定論的
  10. --company-ids でスコープ縮小可能
  11. タイムアウト/失敗ページを最後に retry (より長い timeout で)

Usage:
  op run --env-file config/azure_di.env.op -- python scripts/azure_di_ocr_unknown_image_pages.py --execute --company-ids C0094,C0095
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from datetime import datetime
from pathlib import Path

import pymupdf as fitz

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
OCR_CACHE = PROJECT / "data" / "ocr_cache"
OCR_CACHE.mkdir(exist_ok=True)
sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from reclassify_bundled_pdfs_azure import classify_page

VALID_DOC_TYPES = {
    "取引申請書", "建設業許可証", "決算書", "工事経歴書",
    "取引先一覧表", "労働安全衛生誓約書", "資格略字一覧", "労働者名簿",
    "会社案内",
}

OCR_TIMEOUT_S = 45        # 1pass の通常 timeout
OCR_RETRY_TIMEOUT_S = 180 # retry 時 (3 分まで待つ)
COMMIT_EVERY = 5

_GLOBAL_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ocr")


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


def page_text_via_pymupdf(pdf: Path, page_no: int) -> str:
    try:
        doc = fitz.open(pdf)
        if page_no > len(doc):
            doc.close()
            return ""
        t = doc[page_no - 1].get_text() or ""
        doc.close()
        return t.strip()
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


_pdf_file_hash_cache: dict[Path, str] = {}


def stable_cache_key(pdf: Path, page_no: int) -> str:
    p = pdf.resolve()
    if p not in _pdf_file_hash_cache:
        h = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        _pdf_file_hash_cache[p] = h.hexdigest()
    return f"{_pdf_file_hash_cache[p]}_p{page_no}"


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


def azure_ocr_single_page(pdf_bytes: bytes, cache_key: str, timeout_s: int = OCR_TIMEOUT_S) -> str:
    cache_file = OCR_CACHE / f"page_{cache_key}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text(encoding="utf-8")).get("text", "")

    future = _GLOBAL_EXECUTOR.submit(_azure_ocr_inner, pdf_bytes)
    try:
        text = future.result(timeout=timeout_s)
    except FuturesTimeout:
        future.cancel()
        raise TimeoutError(f"Azure DI exceeded {timeout_s}s")

    cache_file.write_text(json.dumps({"text": text}, ensure_ascii=False), encoding="utf-8")
    return text


def process_page(pid, cid, fn, pno, pdf, conn, workflow, by_cat, is_execute,
                 timeout_s: int) -> tuple[str, str]:
    """1ページを処理し、status を返す。
    status: 'ok' / 'timeout' / 'fail' / 'no_match' / 'cache_hit_skip' / 'extract_fail'
    """
    try:
        pdf_bytes = extract_single_page_pdf_bytes(pdf, pno)
    except Exception:
        return ('extract_fail', '')

    cache_key = stable_cache_key(pdf, pno)
    cache_file = OCR_CACHE / f"page_{cache_key}.json"
    had_cache = cache_file.exists()

    try:
        if is_execute:
            text = azure_ocr_single_page(pdf_bytes, cache_key, timeout_s)
        else:
            text = (
                json.loads(cache_file.read_text(encoding="utf-8")).get("text", "")
                if had_cache else ""
            )
    except TimeoutError:
        return ('timeout', '')
    except Exception as e:
        return ('fail', str(e)[:120])

    if not text:
        return ('no_match', '')

    new_type, new_conf = classify_page(text)
    if new_type == "その他/不明" or new_type not in VALID_DOC_TYPES:
        return ('no_match', '')

    by_cat[new_type] = by_cat.get(new_type, 0) + 1

    if is_execute:
        conn.execute(
            "UPDATE pages SET doc_type_name=?, confidence=? WHERE page_id=?",
            (new_type, new_conf, pid)
        )
        conn.execute(
            """INSERT INTO page_doc_type_history
               (page_id, company_id, file_name, page_no,
                old_doc_type_name, new_doc_type_name,
                reason, workflow_id, decision_id, confirmed_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'azure_di_ocr_auto')""",
            (pid, cid, fn, pno, "その他/不明", new_type,
             f"Azure DI 単頁OCR conf={new_conf:.2f} timeout={timeout_s}s",
             workflow, str(uuid.uuid4()))
        )
    return ('ok', new_type)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="0=無制限")
    ap.add_argument("--company-id")
    ap.add_argument("--company-ids", help="カンマ区切りの cid リスト")
    ap.add_argument("--no-retry", action="store_true", help="タイムアウト/失敗の retry をスキップ")
    args = ap.parse_args()
    is_execute = args.execute and not args.dry_run

    if is_execute and not (os.environ.get("AZURE_DI_ENDPOINT") and os.environ.get("AZURE_DI_KEY")):
        print("ERROR: AZURE_DI_ENDPOINT / AZURE_DI_KEY 未設定", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    q = """SELECT page_id, company_id, file_name, page_no
           FROM pages
           WHERE doc_type_name='その他/不明'
             AND NOT EXISTS (
               SELECT 1 FROM page_doc_type_history h
               WHERE h.page_id = pages.page_id
                 AND h.confirmed_by IN ('web_viewer','user_visual')
             )"""
    params: tuple = ()
    if args.company_id:
        q += " AND company_id=?"
        params = (args.company_id,)
    elif args.company_ids:
        cids = [c.strip() for c in args.company_ids.split(",") if c.strip()]
        if cids:
            placeholders = ",".join("?" for _ in cids)
            q += f" AND company_id IN ({placeholders})"
            params = tuple(cids)
    q += " ORDER BY company_id, file_name, page_no"

    candidates = list(conn.execute(q, params).fetchall())
    print(f"DB 候補: {len(candidates)} ページ", file=sys.stderr, flush=True)

    image_pages = []
    for r in candidates:
        if not r["file_name"].lower().endswith(".pdf"):
            continue
        pdf = find_pdf(r["file_name"], r["company_id"])
        if not pdf:
            continue
        txt = page_text_via_pymupdf(pdf, r["page_no"])
        if len(txt) == 0:
            image_pages.append((r["page_id"], r["company_id"], r["file_name"], r["page_no"], pdf))

    print(f"画像PDF対象: {len(image_pages)} ページ", file=sys.stderr, flush=True)

    if args.limit > 0:
        image_pages = image_pages[: args.limit]

    workflow = f"azure_di_ocr_auto_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    by_cat = {}
    pending_writes = 0
    timeout_pages: list = []   # 1pass で timeout したページ
    fail_pages: list = []      # 1pass で fail したページ

    counters = {"ok": 0, "no_match": 0, "extract_fail": 0, "timeout": 0, "fail": 0}

    def begin_tx():
        if is_execute:
            conn.execute("BEGIN")

    def commit_tx():
        nonlocal pending_writes
        if is_execute and pending_writes > 0:
            conn.commit()
            pending_writes = 0

    print(f"=== Pass 1: timeout={OCR_TIMEOUT_S}s ===", file=sys.stderr, flush=True)
    begin_tx()
    try:
        for i, page_data in enumerate(image_pages, 1):
            pid, cid, fn, pno, pdf = page_data
            status, info = process_page(pid, cid, fn, pno, pdf, conn, workflow, by_cat,
                                         is_execute, OCR_TIMEOUT_S)
            counters[status] = counters.get(status, 0) + 1
            if status == 'ok':
                pending_writes += 1
            elif status == 'timeout':
                timeout_pages.append(page_data)
                print(f"  [{i}] OCR timeout {cid} {fn[:35]} p{pno}",
                      file=sys.stderr, flush=True)
            elif status == 'fail':
                fail_pages.append(page_data)
                print(f"  [{i}] OCR fail {cid} {fn[:35]} p{pno}: {info}",
                      file=sys.stderr, flush=True)
            elif status == 'extract_fail':
                print(f"  [{i}] extract fail {cid} {fn[:35]} p{pno}",
                      file=sys.stderr, flush=True)

            if pending_writes >= COMMIT_EVERY:
                commit_tx()
                begin_tx()
                print(f"  [commit] 進捗 {i}/{len(image_pages)} (改善 {counters['ok']} / TO {counters['timeout']} / fail {counters['fail']})",
                      file=sys.stderr, flush=True)
        commit_tx()

        # === Pass 2: retry timeout ===
        retry_targets = timeout_pages + fail_pages
        if retry_targets and not args.no_retry:
            print(f"\n=== Pass 2: retry timeout={OCR_RETRY_TIMEOUT_S}s ({len(retry_targets)} ページ) ===",
                  file=sys.stderr, flush=True)
            retry_ok = 0
            retry_to = 0
            retry_fail = 0
            begin_tx()
            for j, page_data in enumerate(retry_targets, 1):
                pid, cid, fn, pno, pdf = page_data
                status, info = process_page(pid, cid, fn, pno, pdf, conn, workflow, by_cat,
                                             is_execute, OCR_RETRY_TIMEOUT_S)
                if status == 'ok':
                    retry_ok += 1
                    pending_writes += 1
                    counters['ok'] += 1
                    counters['timeout' if page_data in timeout_pages else 'fail'] -= 1
                elif status == 'timeout':
                    retry_to += 1
                    print(f"  [retry {j}] STILL TIMEOUT {cid} {fn[:35]} p{pno}",
                          file=sys.stderr, flush=True)
                elif status == 'fail':
                    retry_fail += 1
                    print(f"  [retry {j}] STILL FAIL {cid} {fn[:35]} p{pno}: {info}",
                          file=sys.stderr, flush=True)
                if pending_writes >= COMMIT_EVERY:
                    commit_tx()
                    begin_tx()
                    print(f"  [retry commit] {j}/{len(retry_targets)} (recovered {retry_ok})",
                          file=sys.stderr, flush=True)
            commit_tx()
            print(f"\n[retry pass result] recovered={retry_ok} / still_timeout={retry_to} / still_fail={retry_fail}",
                  file=sys.stderr, flush=True)

        if is_execute:
            print("FINAL COMMIT OK", file=sys.stderr, flush=True)
    except KeyboardInterrupt:
        commit_tx()
        print("\n[interrupted] 部分 commit 済み", file=sys.stderr, flush=True)
    except Exception as e:
        if is_execute:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"ERROR: {e}", file=sys.stderr, flush=True)
        raise
    finally:
        _GLOBAL_EXECUTOR.shutdown(wait=False, cancel_futures=True)

    print(f"\n=== {'EXECUTE' if is_execute else 'DRY-RUN'} 結果 ===")
    total_changed = counters['ok']
    if image_pages:
        print(f"  改善: {total_changed} / {len(image_pages)} ({total_changed / len(image_pages) * 100:.1f}%)")
    print(f"  分類不能: {counters['no_match']}")
    print(f"  OCR 失敗 (例外): {counters['fail']}")
    print(f"  OCR タイムアウト (Pass 1+2 両方): {counters['timeout']}")
    print(f"  extract 失敗: {counters['extract_fail']}")
    for k, v in sorted(by_cat.items(), key=lambda x: -x[1]):
        print(f"  → {k}: {v}")


if __name__ == "__main__":
    main()
