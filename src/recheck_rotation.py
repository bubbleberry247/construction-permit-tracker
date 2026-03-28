"""
問題ページの個別再検証・修正スクリプト

手動検証データと不一致のあるページを1ページずつ再判定し、修正する。
"""
import sys
import os
import json
import csv
import time
import base64
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

import fitz
from openai import OpenAI

API_KEY_FILE = Path(r"C:\ProgramData\RK10\credentials\openai_api_key.txt")
DATA_DIR = Path(r"C:\ProgramData\Generative AI\Github\construction-permit-tracker\data\all_documents")
OUTPUT_DIR = Path(r"C:\ProgramData\Generative AI\Github\construction-permit-tracker\data\all_documents_normalized")
LOG_PATH = Path(r"C:\tmp\rotation_log.csv")
DPI = 150  # 再検証は高解像度で

# 問題ページの定義: (filename, pages_to_check, expected)
# expected: "0" = 全ページ回転不要, "check" = 個別判定結果を確認
PROBLEM_CASES = [
    {
        "company": "イシハラ",
        "filename": "イシハラ_御取引条件等説明書_株式会社イシハラ.pdf",
        "pages": [10, 20, 29],  # false positive疑い
        "expected": "all_zero",  # 手動で全ページ0度と確認済み
    },
    {
        "company": "ニュー商事株式会社",
        "filename": "ニュー商事株式会社_御取引条件等説明書_ニュー商事株式会社.pdf",
        "pages": [14, 15, 19],  # 90度判定だが270度が正しいはず
        "expected": "check_270",  # 手動で270度と確認済み
    },
    {
        "company": "三信建材工業株式会社",
        "filename": "三信建材工業株式会社_御取引条件等説明書_三信建材工業㈱豊橋支店.pdf",
        "pages": [13, 21],  # false positive疑い
        "expected": "all_zero",  # 手動で全ページ0度と確認済み
    },
]


def load_api_key() -> str:
    return API_KEY_FILE.read_text(encoding="utf-8").strip()


def render_page_to_base64(doc: fitz.Document, page_no: int, dpi: int = DPI) -> str:
    page = doc[page_no]
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat)
    return base64.b64encode(pix.tobytes("png")).decode("utf-8")


def judge_single_page(client: OpenAI, img_b64: str, page_num: int) -> int:
    """1ページずつ丁寧に判定"""
    content = [
        {
            "type": "text",
            "text": (
                f"このページ画像（ページ{page_num}）の向きを判定してください。\n"
                "この画像は建設業の申請書類（工事経歴書、名簿、決算書、許可証など）です。\n\n"
                "テキストの向きだけでなく、表のレイアウト、罫線、印鑑の位置なども考慮してください。\n"
                "横向き（landscape）の表がそのまま正しい向きで表示されている場合は回転不要です。\n\n"
                "正しく読める向きにするために何度回転が必要ですか？\n\n"
                "回答は数字のみ（0, 90, 180, 270のいずれか）:\n"
                "- 0: 正しい向き（回転不要）\n"
                "- 90: 右に90度回転が必要（テキストが左に倒れている）\n"
                "- 180: 180度回転が必要（上下逆）\n"
                "- 270: 左に90度回転が必要（テキストが右に倒れている）\n"
            ),
        },
        {
            "type": "image_url",
            "image_url": {
                "url": f"data:image/png;base64,{img_b64}",
                "detail": "high",
            },
        },
    ]

    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": content}],
                max_tokens=50,
                temperature=0,
            )
            text = response.choices[0].message.content.strip()
            # 数字だけ抽出
            for val in [0, 90, 180, 270]:
                if str(val) in text:
                    return val
            print(f"    [WARN] 予期しない応答: {text}")
            return 0
        except Exception as e:
            print(f"    [RETRY {attempt+1}/3] {e}")
            time.sleep(2 ** (attempt + 1))

    return -1


def main() -> None:
    print("=" * 60)
    print("問題ページ再検証スクリプト")
    print("=" * 60)

    api_key = load_api_key()
    client = OpenAI(api_key=api_key)

    # CSVログ読み込み
    with open(LOG_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        log_rows = list(reader)

    # ログをキー(company, filename, page_no)でインデックス化
    log_index: dict[tuple[str, str, int], dict] = {}
    for row in log_rows:
        key = (row["company"], row["filename"], int(row["page_no"]))
        log_index[key] = row

    corrections: list[dict] = []

    for case in PROBLEM_CASES:
        company = case["company"]
        filename = case["filename"]
        pages = case["pages"]
        expected = case["expected"]

        print(f"\n--- {company}: {filename} ---")
        print(f"  問題ページ: {pages}, 期待: {expected}")

        pdf_path = DATA_DIR / filename
        if not pdf_path.exists():
            print(f"  [ERROR] ファイルが見つかりません: {pdf_path}")
            continue

        doc = fitz.open(str(pdf_path))

        for page_num in pages:
            print(f"  ページ {page_num}: ", end="", flush=True)

            img_b64 = render_page_to_base64(doc, page_num - 1)
            new_rotation = judge_single_page(client, img_b64, page_num)

            old_rotation = int(log_index[(company, filename, page_num)]["rotation_needed"])

            if expected == "all_zero":
                # 手動検証で回転不要と確認済み → 強制0度
                final = 0
                status = "FORCE 0" if old_rotation != 0 else "OK"
            elif expected == "check_270":
                # 手動で270度と確認済み → 新判定が270ならそのまま、それ以外は270に矯正
                if new_rotation == 270:
                    final = 270
                    status = "CORRECTED to 270"
                elif new_rotation == 90:
                    # GPT-4oが90と判定→手動確認で270が正解なので矯正
                    final = 270
                    status = "OVERRIDE 90->270 (manual)"
                else:
                    final = 270
                    status = f"OVERRIDE {new_rotation}->270 (manual)"
            else:
                final = new_rotation
                status = "RECHECK"

            print(f"旧={old_rotation}度, 再判定={new_rotation}度, 最終={final}度 [{status}]")

            corrections.append({
                "company": company,
                "filename": filename,
                "page_num": page_num,
                "old_rotation": old_rotation,
                "new_judgment": new_rotation,
                "final_rotation": final,
            })

            # ログ更新
            key = (company, filename, page_num)
            if key in log_index:
                log_index[key]["rotation_needed"] = str(final)
                orig_rot = int(log_index[key]["original_rotation"])
                log_index[key]["final_rotation"] = str((orig_rot + final) % 360)

            time.sleep(1)

        doc.close()

    # 修正が必要なPDFを再保存
    print("\n--- 修正PDFの再保存 ---")
    files_to_fix: dict[str, list[dict]] = {}
    for c in corrections:
        if c["old_rotation"] != c["final_rotation"]:
            fn = c["filename"]
            if fn not in files_to_fix:
                files_to_fix[fn] = []
            files_to_fix[fn].append(c)

    for filename, page_corrections in files_to_fix.items():
        pdf_path = DATA_DIR / filename
        print(f"\n  修正: {filename}")

        doc = fitz.open(str(pdf_path))
        for pc in page_corrections:
            page_num = pc["page_num"]
            final = pc["final_rotation"]
            page = doc[page_num - 1]
            original_rotation = page.rotation
            new_rot = (original_rotation + final) % 360
            page.set_rotation(new_rot)
            print(f"    p{page_num}: set_rotation({new_rot})")

        # 変更がないページはそのまま
        output_path = OUTPUT_DIR / filename
        doc.save(str(output_path))
        doc.close()
        print(f"    保存完了: {output_path.name}")

    # CSVログ更新
    print("\n--- ログ更新 ---")
    header = ["company", "filename", "page_no", "original_rotation", "width", "height", "rotation_needed", "final_rotation"]
    updated_rows = []
    for row in log_rows:
        key = (row["company"], row["filename"], int(row["page_no"]))
        if key in log_index:
            updated_rows.append(log_index[key])
        else:
            updated_rows.append(row)

    with open(LOG_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(updated_rows)

    print(f"ログ更新完了: {LOG_PATH}")

    print("\n" + "=" * 60)
    print(f"修正完了: {len(corrections)}ページ再検証, {len(files_to_fix)}ファイル再保存")
    print("=" * 60)


if __name__ == "__main__":
    main()
