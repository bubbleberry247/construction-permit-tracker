"""
GPT-5.4 Vision による全ページ再分類スクリプト
- originals/ 内の全64社PDFを対象
- 全ページを画像化 → 5ページずつバッチで分類
- SQLite pagesテーブルを更新 + CSV出力
- レジューム対応: field_reviewsにgpt54_reclassify記録があるPDFはスキップ
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

import base64
import csv
import io
import json
import os
import sqlite3
import time
import traceback
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF
import httpx
from openai import OpenAI

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------
ORIGINALS_DIR = Path(r"C:\ProgramData\Generative AI\Github\construction-permit-tracker\data\originals")
DB_PATH = Path(r"C:\ProgramData\Generative AI\Github\construction-permit-tracker\data\permit_tracker.db")
API_KEY_PATH = Path(r"C:\ProgramData\RK10\credentials\openai_api_key.txt")
CSV_OUTPUT = Path(r"C:\tmp\page_reclassification.csv")

MODEL = "gpt-5.4"
DPI = 200
BATCH_SIZE = 5
MAX_PAGES = 100
MAX_RETRIES = 3
API_TIMEOUT = 120  # seconds

# ---------------------------------------------------------------------------
# プロンプト
# ---------------------------------------------------------------------------
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
- 0: 正しい向き（回転不要）
- 90: 右に90度回転が必要（文字が左に倒れている）
- 180: 上下逆
- 270: 左に90度回転が必要（文字が右に倒れている）

JSON配列で回答（jsonキーは"pages"）:
{"pages": [{"page": 1, "type": 1, "type_name": "取引申請書", "rotation": 0}, ...]}"""


# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------

def get_api_key() -> str:
    return API_KEY_PATH.read_text(encoding="utf-8").strip()


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.row_factory = sqlite3.Row
    return conn


def get_processed_pdfs(conn: sqlite3.Connection) -> set[str]:
    """既にgpt54_reclassifyで処理済みのcompany_id:file_nameセットを返す"""
    rows = conn.execute(
        "SELECT DISTINCT company_id, field_name FROM field_reviews WHERE confirmed_by = 'gpt54_reclassify'"
    ).fetchall()
    done: set[str] = set()
    for r in rows:
        # field_name = "page_classify:{pdf_name}:p{page_no}"
        parts = r["field_name"].split(":")
        if len(parts) >= 2:
            pdf_name = parts[1]
            done.add(f"{r['company_id']}:{pdf_name}")
    return done


def pdf_to_images(pdf_path: Path) -> list[bytes]:
    """PDFの各ページをPNG画像(bytes)のリストとして返す"""
    doc = fitz.open(str(pdf_path))
    images: list[bytes] = []
    zoom = DPI / 72.0
    mat = fitz.Matrix(zoom, zoom)
    for page in doc:
        pix = page.get_pixmap(matrix=mat)
        images.append(pix.tobytes("png"))
    doc.close()
    return images


def images_to_base64(images: list[bytes]) -> list[str]:
    return [base64.b64encode(img).decode("ascii") for img in images]


def classify_batch(
    client: OpenAI,
    b64_images: list[str],
    page_offset: int,
) -> list[dict[str, Any]]:
    """5ページ以内のバッチを分類。リトライ付き。"""
    content: list[dict] = []
    for i, b64 in enumerate(b64_images):
        content.append({
            "type": "text",
            "text": f"ページ {page_offset + i + 1}:"
        })
        content.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/png;base64,{b64}",
                "detail": "low",
            },
        })

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": content},
                ],
                max_completion_tokens=2000,
                response_format={"type": "json_object"},
                temperature=0,
                timeout=API_TIMEOUT,
            )
            raw = resp.choices[0].message.content
            parsed = json.loads(raw)
            pages_list = parsed.get("pages", parsed)
            if isinstance(pages_list, list):
                return pages_list
            # dict直接の場合
            return [pages_list]
        except Exception as e:
            print(f"\n    [WARN] attempt {attempt}/{MAX_RETRIES}: {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES:
                wait = min(2 ** attempt, 10)
                print(f"    {wait}秒待機中...")
                time.sleep(wait)
            else:
                print(f"    [ERROR] 全リトライ失敗")
                # フォールバック: 各ページを「その他」で返す
                return [
                    {"page": page_offset + i + 1, "type": 0, "type_name": "その他", "rotation": 0}
                    for i in range(len(b64_images))
                ]


TYPE_NAMES = {
    0: "その他",
    1: "取引申請書",
    2: "建設業許可証",
    3: "決算書",
    4: "会社案内",
    5: "工事経歴書",
    6: "取引先一覧表",
    7: "労働安全衛生誓約書",
    8: "資格略字一覧",
    9: "労働者名簿",
}


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main() -> None:
    api_key = get_api_key()
    http_client = httpx.Client(timeout=httpx.Timeout(API_TIMEOUT, connect=30.0))
    client = OpenAI(api_key=api_key, http_client=http_client)
    conn = get_db()

    # レジューム: 処理済みPDFを取得
    processed = get_processed_pdfs(conn)
    print(f"処理済みPDF: {len(processed)}件（スキップ対象）")

    # originals/ のフォルダ一覧
    company_dirs = sorted([
        d for d in ORIGINALS_DIR.iterdir()
        if d.is_dir()
    ])
    total_companies = len(company_dirs)
    print(f"=== 全 {total_companies} 社のPDF再分類を開始 ===\n")

    # 既存CSV結果をロード（レジューム用）
    all_results: list[dict] = []
    if CSV_OUTPUT.exists():
        with open(CSV_OUTPUT, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_results.append(row)
        print(f"既存CSV結果: {len(all_results)}件ロード済み")

    total_pages_processed = 0
    total_pdfs_processed = 0
    skipped_pdfs = 0
    resumed_pdfs = 0

    for ci, comp_dir in enumerate(company_dirs, 1):
        company_id = comp_dir.name.split("_")[0]
        company_name = comp_dir.name[len(company_id) + 1:]
        print(f"[{ci}/{total_companies}] {company_id} {company_name}")

        # PDFファイルのみ
        pdf_files = sorted([
            f for f in comp_dir.iterdir()
            if f.suffix.lower() == ".pdf"
        ])

        if not pdf_files:
            print(f"  PDFなし、スキップ")
            continue

        for pdf_path in pdf_files:
            pdf_name = pdf_path.name

            # レジューム: 処理済みならスキップ
            key = f"{company_id}:{pdf_name}"
            if key in processed:
                print(f"  PDF: {pdf_name} [処理済み、スキップ]")
                resumed_pdfs += 1
                continue

            print(f"  PDF: {pdf_name}")

            # ページ数チェック
            try:
                doc = fitz.open(str(pdf_path))
                num_pages = len(doc)
                doc.close()
            except Exception as e:
                print(f"    [ERROR] PDF読み込み失敗: {e}")
                continue

            if num_pages > MAX_PAGES:
                print(f"    {num_pages}ページ > {MAX_PAGES}、スキップ")
                skipped_pdfs += 1
                continue

            # 画像化
            try:
                images = pdf_to_images(pdf_path)
            except Exception as e:
                print(f"    [ERROR] 画像変換失敗: {e}")
                continue

            b64_images = images_to_base64(images)
            print(f"    {num_pages}ページを画像化完了")

            # バッチ分類
            page_results: list[dict] = []
            for batch_start in range(0, num_pages, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, num_pages)
                batch = b64_images[batch_start:batch_end]
                print(f"    分類中: p{batch_start+1}-{batch_end}...", end="", flush=True)

                results = classify_batch(client, batch, batch_start)
                page_results.extend(results)
                print(f" OK ({len(results)}件)")

            # DB更新
            for pr in page_results:
                page_no = pr.get("page", 0)
                doc_type_id = pr.get("type", 0)
                doc_type_name = pr.get("type_name", TYPE_NAMES.get(doc_type_id, "その他"))
                rotation = pr.get("rotation", 0)

                # 既存レコードを検索（company_id + file_name + page_no）
                # pagesテーブルにはcompany_idがあるものとないものがある
                # まずcompany_id付きで検索、なければfile_nameで検索
                existing = conn.execute(
                    "SELECT page_id FROM pages WHERE company_id = ? AND file_name = ? AND page_no = ?",
                    (company_id, pdf_name, page_no),
                ).fetchone()

                if not existing:
                    # file_nameだけで検索（company_idがNULLのレコード）
                    # originals配下のファイル名はall_documents名と異なる可能性
                    # → company_id + page_noで検索
                    existing = conn.execute(
                        "SELECT page_id FROM pages WHERE company_id = ? AND page_no = ? AND file_name LIKE ?",
                        (company_id, page_no, f"%{pdf_name}%"),
                    ).fetchone()

                if existing:
                    conn.execute(
                        "UPDATE pages SET doc_type_id = ?, doc_type_name = ?, rotation = ? WHERE page_id = ?",
                        (doc_type_id, doc_type_name, rotation, existing["page_id"]),
                    )
                else:
                    conn.execute(
                        "INSERT INTO pages (company_id, file_name, page_no, doc_type_id, doc_type_name, rotation) VALUES (?, ?, ?, ?, ?, ?)",
                        (company_id, pdf_name, page_no, doc_type_id, doc_type_name, rotation),
                    )

                # field_reviews に記録
                conn.execute(
                    "INSERT INTO field_reviews (company_id, field_name, confirmed_value, confirmed_by) VALUES (?, ?, ?, ?)",
                    (
                        company_id,
                        f"page_classify:{pdf_name}:p{page_no}",
                        json.dumps({"type": doc_type_id, "type_name": doc_type_name, "rotation": rotation}, ensure_ascii=False),
                        "gpt54_reclassify",
                    ),
                )

                all_results.append({
                    "company_id": company_id,
                    "company_name": company_name,
                    "file_name": pdf_name,
                    "page_no": page_no,
                    "doc_type_id": doc_type_id,
                    "doc_type_name": doc_type_name,
                    "rotation": rotation,
                })

            conn.commit()
            total_pdfs_processed += 1
            total_pages_processed += len(page_results)
            print(f"    DB更新完了: {len(page_results)}ページ")

    # CSV出力
    CSV_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(CSV_OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "company_id", "company_name", "file_name", "page_no",
            "doc_type_id", "doc_type_name", "rotation",
        ])
        writer.writeheader()
        writer.writerows(all_results)

    conn.close()
    http_client.close()

    print(f"\n=== 完了 ===")
    print(f"処理社数: {total_companies}")
    print(f"新規処理PDF数: {total_pdfs_processed}")
    print(f"レジューム(スキップ)PDF数: {resumed_pdfs}")
    print(f"ページ超過スキップPDF数: {skipped_pdfs}")
    print(f"処理ページ数: {total_pages_processed}")
    print(f"CSV出力: {CSV_OUTPUT} ({len(all_results)}件)")


if __name__ == "__main__":
    main()
