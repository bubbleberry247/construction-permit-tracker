"""
GPT-5.5 vision で confidence=NULL の要確認ページを再 OCR して分類する。

対象:
  pages.confidence IS NULL かつ
  会社が手動修正履歴を持たない（field_reviews.confirmed_by IN MANUAL_SET）
  → 約 205 ページ

更新:
  - pages.doc_type_id, pages.doc_type_name, pages.rotation, pages.confidence
  - ocr_runs INSERT（model_name='gpt-5.5', status, raw_response）

安全策:
  - dry-run 既定（--execute で実行）
  - 事前 SQLite .backup（permit_tracker.db.bak_pre_reocr_gpt55_<ts>）
  - バッチ単位トランザクション、失敗は ROLLBACK + ログ継続
  - バジェット強制停止（--budget-usd、既定 $5.00）

Usage:
  python scripts/reocr_null_pages_gpt55.py                      # dry-run
  python scripts/reocr_null_pages_gpt55.py --execute            # 実行
  python scripts/reocr_null_pages_gpt55.py --limit 5 --execute  # 5 ページのみ
  python scripts/reocr_null_pages_gpt55.py --execute --model gpt-5.4   # 別モデル
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import sqlite3
import sys
import time
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

import fitz  # PyMuPDF
import httpx
from openai import OpenAI

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "permit_tracker.db"
ORIGINALS_DIR = PROJECT / "data" / "originals"
API_KEY_PATH = Path(r"C:\ProgramData\RK10\credentials\openai_api_key.txt")

DEFAULT_MODEL = "gpt-5.5"
DPI = 200
BATCH_SIZE = 5
MAX_RETRIES = 3
API_TIMEOUT = 120
DEFAULT_BUDGET_USD = 5.00

# 料金推定（GPT-5.5 vision detail=low）
PRICE_INPUT_PER_K = 0.00015
PRICE_OUTPUT_PER_K = 0.0006
TOKENS_PER_IMAGE_LOW = 170      # detail=low の画像 1 枚

# review.py と同じ MANUAL_CONFIRM_BY
MANUAL_CONFIRM_BY = (
    "viewer_correction", "manual_review_v2", "manual_review",
    "web_viewer", "user_manual", "manual_visual",
)
_MANUAL_SET_SQL = ",".join(f"'{x}'" for x in MANUAL_CONFIRM_BY)

# 既存 reclassify_pages.py と互換のプロンプト
SYSTEM_PROMPT = """建設業の下請け業者が提出した書類です。各ページの書類種別と回転方向を判定してください。

【書類種別（提出順序の目安）】
1. 取引申請書 — 「新規・継続取引申請書」「御取引条件等説明書」の全ページ。テンプレート記入ページだけでなく、表紙・提出書類の案内・チェックリスト・提出要領の説明ページも全て含む。ファイル名に「御取引条件等説明書」「取引申請」「取引条件」を含むPDFのページは原則このカテゴリ。
2. 建設業許可証 — 「建設業の許可について（通知）」。許可番号・許可年月日・有効期間・許可業種が記載された公文書。知事/大臣の公印あり。変更届出書も含む。
3. 決算書 — 貸借対照表、損益計算書、販売費及び一般管理費、株主資本等変動計算書、個別注記表、製造原価報告書。数字の表が中心。
4. 会社案内 — 会社概要、沿革、事業内容、組織図。写真やカラーが多い。パンフレット的。
5. 工事経歴書 — 工事名・発注者・請負代金・工期の一覧表。横向きの表が多い。実務経験証明書も含む。
6. 取引先一覧表 — 主要取引先の社名リスト。
7. 労働安全衛生誓約書 — 「保護帽の着用」「足場・脚立の適正使用」等の安全項目リスト。誓約文+番号付き項目。
8. 資格略字一覧 — 「電気工事士（電工）」「クレーン運転士（ク）」等の資格チェックリスト。チェックボックス多数。
9. 労働者名簿 — 従業員の氏名・生年月日・資格・健康診断の表。横向きが多い。
0. その他 — 上記に該当しないもの（保険証券、納入実績等）

【回転判定】
各ページの文字が正しく読める向きを判定:
- 0: 正しい向き
- 90: 右に90度回転が必要
- 180: 上下逆
- 270: 左に90度回転が必要

JSON配列で回答（jsonキーは"pages"）:
{"pages": [{"page": 1, "type": 1, "type_name": "取引申請書", "rotation": 0, "confidence": 0.9}, ...]}
- confidence: 自信度（0.0〜1.0）。書類種別が明確なら 0.85+、迷ったら 0.50〜0.70、不明なら 0.30 以下。"""

TYPE_NAMES = {
    0: "その他/不明", 1: "取引申請書", 2: "建設業許可証", 3: "決算書",
    4: "会社案内", 5: "工事経歴書", 6: "取引先一覧表",
    7: "労働安全衛生誓約書", 8: "資格略字一覧", 9: "労働者名簿",
}


def get_api_key() -> str:
    return API_KEY_PATH.read_text(encoding="utf-8").strip()


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.row_factory = sqlite3.Row
    return conn


def backup_db() -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak = DB_PATH.parent / f"permit_tracker.db.bak_pre_reocr_gpt55_{ts}"
    src = sqlite3.connect(str(DB_PATH))
    dst = sqlite3.connect(str(bak))
    with dst:
        src.backup(dst)
    dst.close()
    src.close()
    return bak


def fetch_target_pages(
    conn: sqlite3.Connection,
    limit: int | None = None,
    page_ids: list[int] | None = None,
) -> list[dict]:
    """対象ページを取得。page_ids 指定時はそれだけ、無指定時は要確認キュー対象。"""
    if page_ids:
        placeholders = ",".join("?" * len(page_ids))
        sql = (
            f"SELECT p.page_id, p.company_id, p.file_name, p.page_no, p.doc_type_name "
            f"FROM pages p WHERE p.page_id IN ({placeholders}) "
            f"ORDER BY p.company_id, p.file_name, p.page_no"
        )
        return [dict(r) for r in conn.execute(sql, page_ids)]

    sql = f"""
        SELECT p.page_id, p.company_id, p.file_name, p.page_no, p.doc_type_name
        FROM pages p
        WHERE p.confidence IS NULL
          AND p.company_id NOT IN (
            SELECT DISTINCT company_id FROM field_reviews
            WHERE confirmed_by IN ({_MANUAL_SET_SQL})
          )
        ORDER BY p.company_id, p.file_name, p.page_no
    """
    if limit:
        sql += f" LIMIT {limit}"
    return [dict(r) for r in conn.execute(sql)]


def find_pdf(company_id: str, file_name: str) -> Path | None:
    """data/originals/C0XXX_*/file_name のフルパスを返す。"""
    if not ORIGINALS_DIR.exists():
        return None
    for d in ORIGINALS_DIR.iterdir():
        if not d.is_dir() or not d.name.startswith(f"{company_id}_"):
            continue
        candidate = d / file_name
        if candidate.exists():
            return candidate
    return None


def render_pages(pdf_path: Path, page_nos: list[int]) -> dict[int, bytes]:
    """指定 page_no（1-indexed）の PNG bytes を返す"""
    doc = fitz.open(str(pdf_path))
    zoom = DPI / 72.0
    mat = fitz.Matrix(zoom, zoom)
    out: dict[int, bytes] = {}
    try:
        for pno in page_nos:
            if 1 <= pno <= len(doc):
                page = doc[pno - 1]
                pix = page.get_pixmap(matrix=mat)
                out[pno] = pix.tobytes("png")
    finally:
        doc.close()
    return out


def classify_batch(
    client: OpenAI,
    model: str,
    batch_pages: list[tuple[int, bytes]],
    max_tokens: int = 2000,
) -> tuple[list[dict], int, int]:
    """バッチ分類。戻り値: (pages_list, input_tokens, output_tokens)"""
    content: list[dict] = []
    for pno, img_bytes in batch_pages:
        b64 = base64.b64encode(img_bytes).decode("ascii")
        content.append({"type": "text", "text": f"ページ {pno}:"})
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64}", "detail": "low"},
        })

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": content},
                ],
                max_completion_tokens=max_tokens,
                response_format={"type": "json_object"},
                # GPT-5.5 は temperature 固定（1）のみサポート、指定不要
                timeout=API_TIMEOUT,
            )
            raw = resp.choices[0].message.content
            in_tok = resp.usage.prompt_tokens if resp.usage else 0
            out_tok = resp.usage.completion_tokens if resp.usage else 0
            if not raw or not raw.strip():
                # 空レスポンス: デバッグ情報付きで失敗扱い
                finish = resp.choices[0].finish_reason if resp.choices else "?"
                print(f"    [WARN] 空レスポンス (finish_reason={finish}, in={in_tok} out={out_tok})")
                raise ValueError(f"empty response (finish_reason={finish})")
            parsed = json.loads(raw)
            pages_list = parsed.get("pages", parsed)
            if not isinstance(pages_list, list):
                pages_list = [pages_list]
            return pages_list, in_tok, out_tok
        except Exception as e:
            last_err = e
            print(f"    [WARN] attempt {attempt}/{MAX_RETRIES}: {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(min(2 ** attempt, 10))

    raise RuntimeError(f"classify_batch 全リトライ失敗: {last_err}")


class BudgetGuard:
    def __init__(self, max_usd: float):
        self.max = max_usd
        self.spent = 0.0
        self.calls = 0

    def add(self, in_tokens: int, out_tokens: int) -> float:
        cost = in_tokens / 1000 * PRICE_INPUT_PER_K + out_tokens / 1000 * PRICE_OUTPUT_PER_K
        self.spent += cost
        self.calls += 1
        return cost

    def check(self):
        if self.spent > self.max:
            raise RuntimeError(f"Budget exceeded: ${self.spent:.4f} / ${self.max:.2f} ({self.calls} calls)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="実行（既定 dry-run）")
    ap.add_argument("--limit", type=int, default=None, help="先頭 N ページのみ処理")
    ap.add_argument("--page-ids", default="", help="特定 page_id だけ処理（カンマ区切り）")
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="1 リクエスト当たりページ数")
    ap.add_argument("--max-tokens", type=int, default=2000, help="max_completion_tokens")
    ap.add_argument("--no-write", action="store_true",
                    help="pages テーブルを更新しない（ocr_runs と結果 CSV のみ記録）")
    ap.add_argument("--budget-usd", type=float, default=DEFAULT_BUDGET_USD, help="上限コスト USD")
    ap.add_argument("--model", default=DEFAULT_MODEL, help="OpenAI モデル ID")
    args = ap.parse_args()

    conn = get_db()
    page_ids = None
    if args.page_ids:
        page_ids = [int(x) for x in args.page_ids.split(",") if x.strip()]
    targets = fetch_target_pages(conn, limit=args.limit, page_ids=page_ids)
    print(f"対象ページ: {len(targets)} 件")

    # ファイル別に集約
    by_file: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for t in targets:
        by_file[(t["company_id"], t["file_name"])].append(t)
    print(f"対象ファイル数: {len(by_file)}")

    # PDF 解決
    resolved: list[tuple[Path, list[dict]]] = []
    missing_files = []
    for (cid, fname), pages_list in by_file.items():
        pdf_path = find_pdf(cid, fname)
        if pdf_path is None:
            missing_files.append((cid, fname))
            continue
        resolved.append((pdf_path, pages_list))

    if missing_files:
        print(f"\n[WARN] PDF が見つからないファイル: {len(missing_files)}")
        for cid, fname in missing_files[:10]:
            print(f"  {cid}/{fname}")
        if len(missing_files) > 10:
            print(f"  ... 他 {len(missing_files) - 10} 件")

    total_pages = sum(len(p) for _, p in resolved)
    n_batches = sum((len(p) + BATCH_SIZE - 1) // BATCH_SIZE for _, p in resolved)
    est_in_tokens = n_batches * (TOKENS_PER_IMAGE_LOW * BATCH_SIZE + 600)
    est_out_tokens = n_batches * 500
    est_cost = est_in_tokens / 1000 * PRICE_INPUT_PER_K + est_out_tokens / 1000 * PRICE_OUTPUT_PER_K

    print(f"\n=== 推定 ===")
    print(f"  処理可能ページ: {total_pages}")
    print(f"  バッチ数: {n_batches} (5 ページずつ)")
    print(f"  推定 input tokens: {est_in_tokens}")
    print(f"  推定 output tokens: {est_out_tokens}")
    print(f"  推定コスト: ${est_cost:.4f}")
    print(f"  バジェット上限: ${args.budget_usd}")
    print(f"  モデル: {args.model}")

    if not args.execute:
        print("\n=== dry-run サンプル（先頭 10 件） ===")
        for t in targets[:10]:
            print(f"  {t['company_id']} / {t['file_name']} p{t['page_no']}  (現在: {t['doc_type_name']!r})")
        print("\n(dry-run) 実書込しません。--execute で実行してください。")
        return

    # 実行モード
    print("\n--- 事前バックアップ ---")
    bak = backup_db()
    print(f"  バックアップ: {bak.name}")

    api_key = get_api_key()
    http_client = httpx.Client(timeout=httpx.Timeout(API_TIMEOUT, connect=30.0))
    client = OpenAI(api_key=api_key, http_client=http_client)
    budget = BudgetGuard(args.budget_usd)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_csv = PROJECT / "data" / f"reocr_results_{ts}.csv"
    failed_csv = PROJECT / "data" / f"reocr_failed_{ts}.csv"

    results: list[dict] = []
    failures: list[dict] = []
    pages_updated = 0

    print(f"\n--- 処理開始 ---")
    for fi, (pdf_path, pages_list) in enumerate(resolved, 1):
        cid = pages_list[0]["company_id"]
        fname = pdf_path.name
        page_nos = [p["page_no"] for p in pages_list]
        page_to_id = {p["page_no"]: p["page_id"] for p in pages_list}
        page_to_old = {p["page_no"]: p["doc_type_name"] for p in pages_list}

        print(f"[{fi}/{len(resolved)}] {cid} / {fname} ({len(pages_list)} ページ)")

        try:
            page_imgs = render_pages(pdf_path, page_nos)
        except Exception as e:
            print(f"  [ERROR] PDF 画像化失敗: {e}")
            for p in pages_list:
                failures.append({**p, "error": f"render: {e}"})
            continue

        # バッチ分割
        page_no_list = sorted(page_imgs.keys())
        for batch_start in range(0, len(page_no_list), args.batch_size):
            batch_pnos = page_no_list[batch_start:batch_start + args.batch_size]
            batch_pages = [(pno, page_imgs[pno]) for pno in batch_pnos]

            try:
                pages_resp, in_tok, out_tok = classify_batch(client, args.model, batch_pages, max_tokens=args.max_tokens)
                cost = budget.add(in_tok, out_tok)
                budget.check()
                print(f"  p{batch_pnos[0]}-{batch_pnos[-1]}: tokens in={in_tok} out={out_tok} cost=${cost:.4f} 累計=${budget.spent:.4f}")
            except Exception as e:
                print(f"  [ERROR] classify_batch: {e}")
                for pno in batch_pnos:
                    failures.append({
                        "page_id": page_to_id[pno], "company_id": cid,
                        "file_name": fname, "page_no": pno,
                        "error": f"classify: {e}",
                    })
                if "Budget exceeded" in str(e):
                    print("\n*** バジェット超過、処理中断 ***")
                    break
                continue

            # DB 更新（トランザクション）
            try:
                conn.execute("BEGIN IMMEDIATE")
                for pr in pages_resp:
                    pno = pr.get("page", 0)
                    if pno not in page_to_id:
                        continue
                    page_id = page_to_id[pno]
                    type_id = pr.get("type", 0)
                    type_name = pr.get("type_name") or TYPE_NAMES.get(type_id, "その他/不明")
                    rotation = int(pr.get("rotation", 0))
                    conf = float(pr.get("confidence", 0.5))
                    conf = max(0.0, min(1.0, conf))

                    if not args.no_write:
                        conn.execute(
                            "UPDATE pages SET doc_type_id=?, doc_type_name=?, rotation=?, confidence=? WHERE page_id=?",
                            (type_id, type_name, rotation, conf, page_id),
                        )
                    run_type = "reocr_audit_only" if args.no_write else "reocr_null_confidence"
                    conn.execute(
                        "INSERT INTO ocr_runs (page_id, run_type, model_name, started_at, finished_at, status, raw_response, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (page_id, run_type, args.model,
                         datetime.now().isoformat(), datetime.now().isoformat(),
                         "OK", json.dumps(pr, ensure_ascii=False), datetime.now().isoformat()),
                    )
                    results.append({
                        "page_id": page_id, "company_id": cid, "file_name": fname,
                        "page_no": pno, "before_doc_type": page_to_old.get(pno, ""),
                        "after_doc_type": type_name, "confidence": conf,
                        "rotation": rotation, "model": args.model,
                    })
                    pages_updated += 1
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"  [ERROR] DB 更新失敗: {e}")
                traceback.print_exc()
                for pno in batch_pnos:
                    failures.append({
                        "page_id": page_to_id[pno], "company_id": cid,
                        "file_name": fname, "page_no": pno, "error": f"db: {e}",
                    })

        if budget.spent > args.budget_usd:
            break

    # 結果出力
    if results:
        with result_csv.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            w.writeheader()
            w.writerows(results)
    if failures:
        with failed_csv.open("w", encoding="utf-8-sig", newline="") as f:
            keys = sorted({k for r in failures for k in r.keys()})
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(failures)

    http_client.close()
    conn.close()

    print(f"\n=== 完了 ===")
    print(f"  処理ページ: {pages_updated}")
    print(f"  失敗: {len(failures)}")
    print(f"  API 呼び出し: {budget.calls}")
    print(f"  累計コスト: ${budget.spent:.4f}")
    if results:
        print(f"  結果 CSV: {result_csv}")
    if failures:
        print(f"  失敗 CSV: {failed_csv}")
    print(f"  バックアップ: {bak}")


if __name__ == "__main__":
    main()
