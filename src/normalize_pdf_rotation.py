"""
PDF全ページ方向判定・回転補正スクリプト

GPT-4o Vision で各ページの向きを判定し、正しい読み方向に回転して保存する。
対象: all_documents/ 内の5ページ以上のPDF（会社ごとに最大サイズの1ファイルのみ）
"""

import sys
import os
import json
import csv
import time
import base64
import io
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

import fitz  # PyMuPDF
from openai import OpenAI

# --- 定数 ---
DATA_DIR = Path(r"C:\ProgramData\Generative AI\Github\construction-permit-tracker\data\all_documents")
OUTPUT_DIR = Path(r"C:\ProgramData\Generative AI\Github\construction-permit-tracker\data\all_documents_normalized")
LOG_PATH = Path(r"C:\tmp\rotation_log.csv")
API_KEY_FILE = Path(r"C:\ProgramData\RK10\credentials\openai_api_key.txt")
BATCH_SIZE = 5
MAX_PAGES = 30
MIN_PAGES = 5
DPI = 100
MAX_RETRIES = 3


def load_api_key() -> str:
    """APIキーをファイルから読み込む"""
    return API_KEY_FILE.read_text(encoding="utf-8").strip()


def get_company_name(filename: str) -> str:
    """ファイル名から会社名を抽出（最初の_まで）"""
    if "_" in filename:
        return filename.split("_")[0]
    return filename.rsplit(".", 1)[0]


def find_target_pdfs() -> list[dict]:
    """
    5ページ以上のPDFを検索し、会社ごとに最大サイズのものだけ返す。
    """
    pdf_info: list[dict] = []

    for f in DATA_DIR.iterdir():
        if f.suffix.lower() != ".pdf":
            continue
        try:
            doc = fitz.open(str(f))
            page_count = doc.page_count
            doc.close()
        except Exception as e:
            print(f"  [SKIP] {f.name}: fitz.open失敗 {e}")
            continue

        if page_count >= MIN_PAGES:
            pdf_info.append({
                "path": f,
                "filename": f.name,
                "company": get_company_name(f.name),
                "pages": page_count,
                "size": f.stat().st_size,
            })

    # 会社ごとに最大サイズのファイルだけ残す
    by_company: dict[str, list[dict]] = defaultdict(list)
    for info in pdf_info:
        by_company[info["company"]].append(info)

    targets: list[dict] = []
    for company, files in sorted(by_company.items()):
        best = max(files, key=lambda x: x["size"])
        if len(files) > 1:
            print(f"  [{company}] 重複{len(files)}件 -> 最大サイズ選択: {best['filename']} ({best['size']:,}bytes, {best['pages']}p)")
        targets.append(best)

    return targets


def render_page_to_base64(doc: fitz.Document, page_no: int) -> str:
    """ページをPNG画像にレンダリングし、base64エンコードして返す"""
    page = doc[page_no]
    mat = fitz.Matrix(DPI / 72, DPI / 72)
    pix = page.get_pixmap(matrix=mat)
    img_bytes = pix.tobytes("png")
    return base64.b64encode(img_bytes).decode("utf-8")


def judge_rotation_batch(
    client: OpenAI,
    images_b64: list[str],
    page_numbers: list[int],
) -> list[dict]:
    """
    GPT-4o Vision でバッチ内の各ページの回転を判定する。
    戻り値: [{"page": int, "rotation_needed": int}, ...]
    """
    content: list[dict] = [
        {
            "type": "text",
            "text": (
                "これらのページ画像の向きを判定してください。\n"
                "各ページについて、正しく読める向きにするために何度回転が必要かを判定してください。\n\n"
                f"ページ番号: {page_numbers}\n\n"
                "回答形式（JSON配列のみ、他のテキストは不要）:\n"
                "[\n"
                '  {"page": 1, "rotation_needed": 0},\n'
                '  {"page": 2, "rotation_needed": 90},\n'
                "  ...\n"
                "]\n\n"
                "rotation_needed の値:\n"
                "- 0: 正しい向き（回転不要）\n"
                "- 90: 右に90度回転が必要（現在左に倒れている）\n"
                "- 180: 180度回転が必要（上下逆）\n"
                "- 270: 左に90度回転が必要（現在右に倒れている）\n"
            ),
        }
    ]

    for i, img_b64 in enumerate(images_b64):
        content.append({
            "type": "text",
            "text": f"--- ページ {page_numbers[i]} ---",
        })
        content.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/png;base64,{img_b64}",
                "detail": "low",
            },
        })

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": content,
                    }
                ],
                max_tokens=1000,
                temperature=0,
            )
            text = response.choices[0].message.content.strip()
            # JSON部分を抽出
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
                text = text.strip()
            result = json.loads(text)
            return result
        except Exception as e:
            print(f"    [RETRY {attempt}/{MAX_RETRIES}] API error: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
            else:
                # 全リトライ失敗 -> rotation_needed=-1（エラー）を返す
                return [{"page": p, "rotation_needed": -1} for p in page_numbers]

    return []


def process_pdf(
    client: OpenAI,
    pdf_info: dict,
    log_rows: list[list],
    idx: int,
    total: int,
) -> bool:
    """
    1つのPDFを処理: 方向判定 -> 回転補正 -> 保存
    """
    pdf_path: Path = pdf_info["path"]
    company = pdf_info["company"]
    page_count = pdf_info["pages"]
    effective_pages = min(page_count, MAX_PAGES)

    print(f"\n[{idx}/{total}] {company} - {pdf_path.name} ({page_count}p, 処理: {effective_pages}p)")

    try:
        doc = fitz.open(str(pdf_path))
    except Exception as e:
        print(f"  [ERROR] fitz.open失敗: {e}")
        return False

    # 全ページの回転判定結果を集める
    all_rotations: dict[int, int] = {}

    # バッチ処理
    page_indices = list(range(effective_pages))
    for batch_start in range(0, len(page_indices), BATCH_SIZE):
        batch = page_indices[batch_start:batch_start + BATCH_SIZE]
        page_numbers = [p + 1 for p in batch]  # 1-indexed for display

        print(f"  バッチ: ページ {page_numbers[0]}-{page_numbers[-1]} ...", end=" ", flush=True)

        # 画像化
        images_b64: list[str] = []
        for p in batch:
            img_b64 = render_page_to_base64(doc, p)
            images_b64.append(img_b64)

        # Vision判定
        results = judge_rotation_batch(client, images_b64, page_numbers)

        for r in results:
            page_num = r["page"]
            rot = r["rotation_needed"]
            all_rotations[page_num] = rot

        # 結果表示
        rot_summary = ", ".join(f"p{r['page']}:{r['rotation_needed']}°" for r in results)
        print(f"-> {rot_summary}")

        # レート制限対策
        time.sleep(1)

    # 回転適用
    needs_rotation = any(v not in (0, -1) for v in all_rotations.values())

    for page_no_1 in range(1, page_count + 1):
        page = doc[page_no_1 - 1]
        original_rotation = page.rotation
        rect = page.rect
        width = rect.width
        height = rect.height
        rotation_needed = all_rotations.get(page_no_1, 0)

        if rotation_needed > 0:
            new_rotation = (original_rotation + rotation_needed) % 360
            page.set_rotation(new_rotation)
            final_rotation = new_rotation
        else:
            final_rotation = original_rotation

        log_rows.append([
            company,
            pdf_path.name,
            page_no_1,
            original_rotation,
            f"{width:.0f}",
            f"{height:.0f}",
            rotation_needed,
            final_rotation,
        ])

    # 保存
    output_path = OUTPUT_DIR / pdf_path.name
    try:
        doc.save(str(output_path))
        if needs_rotation:
            print(f"  -> 回転補正して保存: {output_path.name}")
        else:
            print(f"  -> 回転不要、そのまま保存: {output_path.name}")
    except Exception as e:
        print(f"  [ERROR] 保存失敗: {e}")
        doc.close()
        return False

    doc.close()
    return True


def main() -> None:
    print("=" * 60)
    print("PDF方向判定・回転補正スクリプト")
    print("=" * 60)

    # APIクライアント初期化
    api_key = load_api_key()
    client = OpenAI(api_key=api_key)

    # 出力ディレクトリ作成
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: 対象PDF一覧
    print("\n--- Step 1: 対象PDF一覧 ---")
    targets = find_target_pdfs()
    print(f"\n対象: {len(targets)}社")
    for t in targets:
        print(f"  {t['company']}: {t['filename']} ({t['pages']}p, {t['size']:,}bytes)")

    # Step 2-3: 各PDFを処理
    print("\n--- Step 2-3: 方向判定 & 回転補正 ---")
    log_rows: list[list] = []
    success = 0
    fail = 0

    for idx, pdf_info in enumerate(targets, 1):
        ok = process_pdf(client, pdf_info, log_rows, idx, len(targets))
        if ok:
            success += 1
        else:
            fail += 1

    # Step 4: ログ出力
    print("\n--- Step 4: ログ出力 ---")
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "company", "filename", "page_no",
            "original_rotation", "width", "height",
            "rotation_needed", "final_rotation",
        ])
        writer.writerows(log_rows)

    print(f"ログ保存: {LOG_PATH}")

    # サマリ
    print("\n" + "=" * 60)
    print(f"完了: 成功={success}, 失敗={fail}, 合計={len(targets)}社")
    rotation_count = sum(1 for row in log_rows if int(row[6]) > 0)
    error_count = sum(1 for row in log_rows if int(row[6]) == -1)
    print(f"回転したページ: {rotation_count}, エラーページ: {error_count}")
    print("=" * 60)


if __name__ == "__main__":
    main()
