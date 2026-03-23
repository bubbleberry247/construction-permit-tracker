#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
OCR Benchmark: 建設業請求書PDF 30枚を3つのOCRモデルで処理し精度を比較する

Usage:
    python tests/ocr_benchmark.py
    python tests/ocr_benchmark.py --models glm-ocr,qwen3-vl:30b-a3b
    python tests/ocr_benchmark.py --skip-models ocr-best
"""

import argparse
import base64
import csv
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Windows cp932 エンコーディング対策
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# PDF → 画像変換
# ---------------------------------------------------------------------------
try:
    import fitz  # PyMuPDF

    def pdf_to_images(pdf_path: str) -> list[bytes]:
        doc = fitz.open(pdf_path)
        images = []
        for page in doc:
            pix = page.get_pixmap(dpi=200)
            images.append(pix.tobytes("png"))
        doc.close()
        return images

except ImportError:
    try:
        from pdf2image import convert_from_path
        import io

        def pdf_to_images(pdf_path: str) -> list[bytes]:
            pages = convert_from_path(pdf_path, dpi=200)
            images = []
            for page in pages:
                buf = io.BytesIO()
                page.save(buf, format="PNG")
                images.append(buf.getvalue())
            return images

    except ImportError:
        print("[FATAL] PyMuPDF (fitz) も pdf2image もインストールされていません。")
        print("  pip install pymupdf  または  pip install pdf2image")
        sys.exit(1)

try:
    import requests
except ImportError:
    print("[FATAL] requests がインストールされていません。  pip install requests")
    sys.exit(1)

# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------
BASE = (
    "C:/ProgramData/RK10/Robots/"
    "12・13受信メールのPDFを保存・一括印刷/samples/"
    "PDF教師データ/2026.1月分(2026.2末支払い)"
)

FILES = [
    "3259_セイワ/(有)竹下建築_20260115_11,000,000_セイワ㈱工場新築工事_請求書.pdf",
    "3285_中島町三丁目/(有)前畑工務店_20260125_4050000_(仮称)中村区中島三丁目計画新築工事.pdf",
    "3285_中島町三丁目/クレーンタル野田_20260125_769,500【（仮称）中村区中島町三丁目計画新築工事】.pdf",
    "3285_中島町三丁目/大建左官工業_20260125_5,000,000_中島町3丁目計画.pdf",
    "3285_中島町三丁目/株式会社インテルグロー_20260125_3249000_【（仮称）中村区中島三丁目計画新築工事】.pdf",
    "3294_モビテック造成/カイノス㈱_20260125_1703230【モビテックシンテクニカルセンター新築工事】.pdf",
    "3294_モビテック造成/株式会社ZERQ_2026.01.25_630000_㈱モビテック シン・テクニカルセンター新築工事　請求書.pdf",
    "3304_冨田様共同住宅/㈱山西_20260125_550000_【冨田様共同住宅新築工事】.pdf",
    "3304_冨田様共同住宅/御請求書(冨田信行様邸共同住宅新築工事)_甲陽建設工業_2026.1.25_280,000(税抜).pdf",
    "3304_冨田様共同住宅/✖北恵㈱＿２０２６年1月26日＿539,000円_富田様邸共同住宅新築工事.pdf",
    "3329_モビテック/サイタ工業株式会社_20260125_2,394,000_株式会社モビテック　シン・テクニカルセンター新築工事.pdf",
    "3329_モビテック/吉田電気工事㈱_20260125_7,761,600【株式会社モビテックシン・テクニカルセンター新築工事】.pdf",
    "3329_モビテック/太田商事㈱_260125_13,176,000_株式会社モビテックシン・テクニカルセンター新築工事.pdf",
    "3329_モビテック/✖㈱ヨシダ建装_20260125_1,530,000_株式会社モビテック　ｼﾝ･ﾃｸﾆｶﾙｾﾝﾀｰ新築工事.pdf",
    "3402_相羽製作所解体/真栄産業_20260125_5200_相羽製作所解体工事.pdf",
    "3472_家族葬の結家西尾/マルフミ榊原工業㈱_20260127_420,000_【（仮称）家族葬の結家西尾市一色町味浜改装工事】.pdf",
    "3472_家族葬の結家西尾/近藤電工社_20260125_6,156,000_【仮称】家族葬の結家西尾市一色町味浜改装工事.pdf",
    "3472_家族葬の結家西尾/株式会社オカケン-20260125-253000-家族葬の結家西尾市一色町味浜改修工事.pdf",
    "3473_ローソン/前田道路㈱_20260131_13,500,000【ローソン三河安城本町店】.pdf",
    "3473_ローソン/髙田工業所_20260125_160000_ローソン三河安城本町店設備架台工事.pdf",
    "3509_めぐ/大功建築_20260125_990000_【めぐウィメンズクリニック新築工事】.pdf",
    "3509_めぐ/株式会社オカケン-20260125-5220000-めぐウィメンズクリニック新築工事.pdf",
    "一般経費/㈱NJS 2026_0125_127700_二本木資材置場.pdf",
    "営繕/㈱拓土_20260125_268,000【陽だまりの森クリニック 外壁修繕工事】.pdf",
    "営繕/株式会社橋建_20260125_7,260,000_愛三工業 安城工場 消防倉庫前排水整備工事.pdf",
    "営繕/愛知ベース工業㈱_20260125_2,184,000_愛同工業様株式会社吉田工場新築工事.pdf",
    "営繕/㈲池本シート商会_2026.1.17_710000_ヤマコー緑区 遮熱シート新設工事.pdf",
    "営繕/株式会社あかり_20260121_1,500,000_愛三工業㈱豊田工場 食堂・厨房改修に伴う電気工事.pdf",
    "現場経費/明和産業㈱_20260202_1,022,993【ローソン三河安城本町店】.pdf",
    "現場経費/三友電子㈱_20260109_30,000【浅田レディースクリニック名古屋駅前】.pdf",
]

ALL_MODELS = ["glm-ocr", "qwen3-vl:30b-a3b", "ocr-best"]

OLLAMA_PROMPT_V1 = (
    "この請求書から以下を抽出してJSON形式で返してください: "
    "vendor_name(取引先名), issue_date(発行日YYYYMMDD), "
    "amount(税込合計金額、数値のみ), "
    "invoice_number(適格請求書番号T+13桁、なければ空文字)"
)

OLLAMA_PROMPT = (
    "あなたは建設業の経理担当です。この請求書画像から以下の情報を抽出し、JSON形式で返してください。\n\n"
    "## 抽出ルール\n"
    "- vendor_name: **請求書の差出人（発行元・請求元）の会社名**。「御中」の宛先側ではない。"
    "請求書の右上や下部にある発行者情報から取得すること。\n"
    "- issue_date: 発行日をYYYYMMDD形式の8桁数字で。年は2026年のはず。\n"
    "- amount: **税込合計金額**を数値のみで（カンマなし）。"
    "差引請求額があればそれを優先。なければ税込合計。\n"
    "- invoice_number: 適格請求書番号（T+13桁数字）。なければ空文字。\n\n"
    '## 出力形式（厳密に従うこと）\n'
    '{"vendor_name": "会社名", "issue_date": "YYYYMMDD", "amount": 数値, "invoice_number": "T..."}\n'
)

# ---------------------------------------------------------------------------
# Ground Truth パース
# ---------------------------------------------------------------------------

# 全角→半角 数字・英字
_ZEN2HAN_TABLE = str.maketrans(
    "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ",
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
)
# 全角アンダースコア→半角
_ZEN2HAN_TABLE[ord("＿")] = ord("_")


def _normalize_filename(name: str) -> str:
    """ファイル名の全角数字・記号を半角化し、プレフィックスを除去"""
    name = name.translate(_ZEN2HAN_TABLE)
    # 先頭の ✖ を除去
    name = re.sub(r"^[✖✕×]+\s*", "", name)
    # 先頭の【...】プレフィックスを除去
    name = re.sub(r"^【[^】]*】\s*", "", name)
    return name


def _normalize_date(raw: str) -> str:
    """日付文字列を YYYYMMDD に正規化"""
    raw = raw.strip()
    # 「年」「月」「日」 を除去
    raw = raw.replace("年", ".").replace("月", ".").replace("日", "")
    # YYYY.MM.DD or YYYY.M.DD
    m = re.match(r"(\d{4})[./](\d{1,2})[./](\d{1,2})", raw)
    if m:
        return f"{int(m.group(1)):04d}{int(m.group(2)):02d}{int(m.group(3)):02d}"
    # YYYYMMDD
    m = re.match(r"(\d{8})$", raw)
    if m:
        return raw
    # YYMMDD (6桁)
    m = re.match(r"(\d{6})$", raw)
    if m:
        yy = int(raw[:2])
        year = 2000 + yy if yy < 100 else yy
        return f"{year:04d}{raw[2:]}"
    # YYYY_MMDD (アンダースコア区切り)
    m = re.match(r"(\d{4})_(\d{4})", raw)
    if m:
        return m.group(1) + m.group(2)
    return raw


def _normalize_amount(raw: str) -> int:
    """金額文字列を数値化"""
    raw = raw.strip()
    raw = raw.replace(",", "").replace("，", "")
    raw = raw.replace("円", "")
    # (税抜) や 【...】 の末尾注釈を除去
    raw = re.sub(r"[\(（].*?[\)）]", "", raw)
    raw = re.sub(r"【.*?】", "", raw)
    raw = raw.strip()
    m = re.search(r"(\d+)", raw)
    if m:
        return int(m.group(1))
    return 0


def parse_ground_truth(relative_path: str) -> dict:
    """ファイル名から正解データをパース"""
    filename = Path(relative_path).name
    # 拡張子除去
    stem = filename.rsplit(".", 1)[0] if "." in filename else filename

    stem = _normalize_filename(stem)

    # --- 特殊パターン: 「御請求書(...)_取引先_日付_金額」 ---
    # 例: 御請求書(冨田信行様邸共同住宅新築工事)_甲陽建設工業_2026.1.25_280,000(税抜)
    m_special = re.match(r"御請求書\([^)]*\)_(.+?)_([\d.]+)_(.+)", stem)
    if m_special:
        vendor = m_special.group(1).strip()
        date_raw = m_special.group(2)
        amount_raw = m_special.group(3).split("_")[0]
        return {
            "vendor_name": vendor,
            "issue_date": _normalize_date(date_raw),
            "amount": _normalize_amount(amount_raw),
        }

    # --- 特殊パターン: 「㈱NJS 2026_0125_127700_...」 (スペース区切り+アンダースコア) ---
    m_njs = re.match(r"(.+?)\s+(\d{4})_(\d{4})_(\d[\d,]*)_(.*)", stem)
    if m_njs:
        vendor = m_njs.group(1).strip()
        date_raw = m_njs.group(2) + m_njs.group(3)
        amount_raw = m_njs.group(4)
        return {
            "vendor_name": vendor,
            "issue_date": _normalize_date(date_raw),
            "amount": _normalize_amount(amount_raw),
        }

    # --- ハイフン区切りパターン: 「株式会社オカケン-20260125-253000-...」 ---
    if "-" in stem and "_" not in stem.split("-")[0]:
        parts = stem.split("-")
        if len(parts) >= 3:
            vendor = parts[0].strip()
            date_raw = parts[1].strip()
            amount_raw = parts[2].strip()
            return {
                "vendor_name": vendor,
                "issue_date": _normalize_date(date_raw),
                "amount": _normalize_amount(amount_raw),
            }

    # --- 全角アンダースコア混在パターン: 「北恵㈱_2026年1月26日_539,000円_...」---
    # （_normalize_filename で全角＿は半角_に変換済み）

    # --- 標準パターン: アンダースコア区切り ---
    parts = stem.split("_")
    if len(parts) >= 3:
        vendor = parts[0].strip()
        date_raw = parts[1].strip()
        amount_raw = parts[2].strip()
        return {
            "vendor_name": vendor,
            "issue_date": _normalize_date(date_raw),
            "amount": _normalize_amount(amount_raw),
        }

    # パース失敗
    return {"vendor_name": "", "issue_date": "", "amount": 0}


# ---------------------------------------------------------------------------
# OCR 実行関数
# ---------------------------------------------------------------------------


def ollama_ocr(model_name: str, image_bytes: bytes, prompt: str) -> dict:
    """Ollama Vision API でOCR"""
    b64 = base64.b64encode(image_bytes).decode()
    resp = requests.post(
        "http://localhost:11434/api/chat",
        json={
            "model": model_name,
            "messages": [{"role": "user", "content": prompt, "images": [b64]}],
            "stream": False,
            "options": {"temperature": 0},
        },
        timeout=180,
    )
    resp.raise_for_status()
    content = resp.json()["message"]["content"]
    # JSON部分を抽出（複数手法でパース試行）
    match = re.search(r"\{[^{}]*\}", content, re.DOTALL)
    if match:
        raw_json = match.group()
        # 1) まず標準JSONとしてパース
        try:
            return json.loads(raw_json)
        except json.JSONDecodeError:
            pass
        # 2) キー未クォートJSON対応: key: "value" → "key": "value"
        fixed = re.sub(r'(\w+)\s*:', r'"\1":', raw_json)
        # 関数呼び出し風の値も修正: "key"("value") → "key": "value"
        fixed = re.sub(r'"(\w+)"\("([^"]*)"\)', r'"\1": "\2"', fixed)
        try:
            return json.loads(fixed)
        except json.JSONDecodeError:
            pass
    # 3) 正規表現で個別フィールド抽出（最終フォールバック）
    result = {"vendor_name": "", "issue_date": "", "amount": 0, "invoice_number": ""}
    vm = re.search(r'vendor_name["\s:()]*["\']?([^"\',}\n]+)', content)
    if vm:
        result["vendor_name"] = vm.group(1).strip().strip('"\'')
    dm = re.search(r'issue_date["\s:()]*["\']?([^"\',}\n]+)', content)
    if dm:
        result["issue_date"] = dm.group(1).strip().strip('"\'')
    am = re.search(r'amount["\s:()]*["\']?([^"\',}\n]+)', content)
    if am:
        result["amount"] = am.group(1).strip().strip('"\'')
    im = re.search(r'invoice_number["\s:()]*["\']?([^"\',}\n]+)', content)
    if im:
        result["invoice_number"] = im.group(1).strip().strip('"\'')
    if any(v for k, v in result.items() if k != "amount"):
        return result
    return {
        "vendor_name": "",
        "issue_date": "",
        "amount": 0,
        "invoice_number": "",
        "raw": content,
    }


def run_ollama_model(model_name: str, pdf_path: str) -> dict:
    """Ollamaモデルで1ファイル処理（先頭ページのみ）"""
    images = pdf_to_images(pdf_path)
    if not images:
        return {"vendor_name": "", "issue_date": "", "amount": 0, "invoice_number": ""}
    # 先頭ページで処理
    result = ollama_ocr(model_name, images[0], OLLAMA_PROMPT)
    return result


def run_ocr_best(pdf_path: str) -> dict:
    """YomiToku ocr-best で1ファイル処理"""
    env = {**os.environ, "RK10_ROBOT_DIR": r"C:\ProgramData\RK10\Tools\ocr-best"}
    result = subprocess.run(
        [
            "python",
            r"C:\ProgramData\RK10\Tools\ocr-best\cli.py",
            "--pdf",
            pdf_path,
        ],
        capture_output=True,
        env=env,
        timeout=120,
    )
    # バイナリモードでデコード（cp932 → utf-8 フォールバック）
    stdout_str = ""
    stderr_str = ""
    for enc in ("utf-8", "cp932", "latin-1"):
        try:
            stdout_str = result.stdout.decode(enc)
            break
        except (UnicodeDecodeError, AttributeError):
            continue
    for enc in ("utf-8", "cp932", "latin-1"):
        try:
            stderr_str = result.stderr.decode(enc)
            break
        except (UnicodeDecodeError, AttributeError):
            continue
    if result.returncode != 0:
        print(f"    [WARN] ocr-best stderr: {stderr_str[:200]}")
        return {"vendor_name": "", "issue_date": "", "amount": 0, "invoice_number": ""}
    try:
        data = json.loads(stdout_str)
        return data
    except (json.JSONDecodeError, TypeError):
        print(f"    [WARN] ocr-best JSON parse failed: {stdout_str[:200]}")
        return {
            "vendor_name": "",
            "issue_date": "",
            "amount": 0,
            "invoice_number": "",
            "raw": stdout_str,
        }


def run_model(model_name: str, pdf_path: str) -> dict:
    """モデル名に応じて適切な関数を呼び出す"""
    if model_name == "ocr-best":
        return run_ocr_best(pdf_path)
    else:
        return run_ollama_model(model_name, pdf_path)


# ---------------------------------------------------------------------------
# 評価
# ---------------------------------------------------------------------------


def _clean_vendor(name: str) -> str:
    """比較用に取引先名をクリーンアップ"""
    s = str(name).strip()
    # 法人格の表記揺れを統一
    for old, new in [
        ("(有)", "有限会社"), ("（有）", "有限会社"),
        ("(株)", "株式会社"), ("（株）", "株式会社"),
        ("㈱", "株式会社"), ("㈲", "有限会社"),
    ]:
        s = s.replace(old, new)
    return s


def match_vendor(gt: str, ocr: str) -> bool:
    """取引先名の部分一致判定"""
    if not gt or not ocr:
        return False
    gt_clean = _clean_vendor(gt)
    ocr_clean = _clean_vendor(str(ocr))
    # 双方向部分一致
    return gt_clean in ocr_clean or ocr_clean in gt_clean


def match_date(gt: str, ocr: str) -> bool:
    """日付の完全一致判定"""
    if not gt or not ocr:
        return False
    return _normalize_date(str(gt)) == _normalize_date(str(ocr))


def match_amount(gt: int, ocr) -> bool:
    """金額の完全一致判定"""
    try:
        ocr_val = int(str(ocr).replace(",", "").replace("円", "").strip())
    except (ValueError, TypeError):
        return False
    return gt == ocr_val


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="OCR Benchmark for Construction Invoices")
    parser.add_argument(
        "--models",
        type=str,
        default=",".join(ALL_MODELS),
        help="使用するモデル (カンマ区切り)",
    )
    parser.add_argument(
        "--skip-models",
        type=str,
        default="",
        help="スキップするモデル (カンマ区切り)",
    )
    args = parser.parse_args()

    models = [m.strip() for m in args.models.split(",") if m.strip()]
    skip = {m.strip() for m in args.skip_models.split(",") if m.strip()}
    models = [m for m in models if m not in skip]

    if not models:
        print("[ERROR] 実行するモデルがありません")
        sys.exit(1)

    print("=" * 70)
    print("OCR Benchmark: 建設業請求書 精度比較テスト")
    print(f"対象ファイル: {len(FILES)} 件")
    print(f"使用モデル: {', '.join(models)}")
    print("=" * 70)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tests_dir = Path(__file__).parent
    total_files = len(FILES)

    # Ground Truth の事前パース
    ground_truths = {}
    print("\n--- Ground Truth パース ---")
    for rel in FILES:
        gt = parse_ground_truth(rel)
        ground_truths[rel] = gt
        fname = Path(rel).name
        print(f"  {fname}")
        print(f"    vendor={gt['vendor_name']}  date={gt['issue_date']}  amount={gt['amount']}")

    # 各モデルの結果を格納
    all_results: dict[str, dict[str, dict]] = {m: {} for m in models}

    # 各モデルで全ファイル処理
    for model in models:
        print(f"\n{'=' * 70}")
        print(f"Model: {model}")
        print("=" * 70)

        for idx, rel in enumerate(FILES, 1):
            pdf_path = os.path.join(BASE, rel)
            fname = Path(rel).name

            if not os.path.exists(pdf_path):
                print(f"[{idx}/{total_files}] SKIP (not found): {fname}")
                all_results[model][rel] = {
                    "vendor_name": "",
                    "issue_date": "",
                    "amount": 0,
                    "invoice_number": "",
                    "error": "file not found",
                }
                continue

            print(f"[{idx}/{total_files}] Processing: {fname} ({model})")
            t0 = time.time()
            try:
                result = run_model(model, pdf_path)
                elapsed = time.time() - t0
                print(f"    -> {elapsed:.1f}s  vendor={result.get('vendor_name', '')}  "
                      f"date={result.get('issue_date', '')}  amount={result.get('amount', '')}")
                all_results[model][rel] = result
            except Exception as e:
                elapsed = time.time() - t0
                print(f"    -> ERROR ({elapsed:.1f}s): {e}")
                all_results[model][rel] = {
                    "vendor_name": "",
                    "issue_date": "",
                    "amount": 0,
                    "invoice_number": "",
                    "error": str(e),
                }

        # モデル別結果をJSON保存
        model_safe = model.replace(":", "_").replace("/", "_")
        json_path = tests_dir / f"results_{model_safe}_{timestamp}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(all_results[model], f, ensure_ascii=False, indent=2)
        print(f"\n結果保存: {json_path}")

    # ---------------------------------------------------------------------------
    # 比較サマリー CSV
    # ---------------------------------------------------------------------------
    csv_path = tests_dir / f"ocr_benchmark_results_{timestamp}.csv"

    header = ["file", "gt_vendor", "gt_date", "gt_amount"]
    for m in models:
        ms = m.replace(":", "_").replace("/", "_")
        header.extend([
            f"{ms}_vendor", f"{ms}_date", f"{ms}_amount",
            f"{ms}_vendor_match", f"{ms}_date_match", f"{ms}_amount_match",
        ])

    rows = []
    # 集計用
    stats: dict[str, dict[str, int]] = {
        m: {"vendor_ok": 0, "date_ok": 0, "amount_ok": 0, "total": 0}
        for m in models
    }

    for rel in FILES:
        gt = ground_truths[rel]
        row = [
            Path(rel).name,
            gt["vendor_name"],
            gt["issue_date"],
            gt["amount"],
        ]
        for m in models:
            r = all_results[m].get(rel, {})
            v_match = match_vendor(gt["vendor_name"], r.get("vendor_name", ""))
            d_match = match_date(gt["issue_date"], str(r.get("issue_date", "")))
            a_match = match_amount(gt["amount"], r.get("amount", 0))

            row.extend([
                r.get("vendor_name", ""),
                r.get("issue_date", ""),
                r.get("amount", ""),
                v_match,
                d_match,
                a_match,
            ])

            stats[m]["total"] += 1
            if v_match:
                stats[m]["vendor_ok"] += 1
            if d_match:
                stats[m]["date_ok"] += 1
            if a_match:
                stats[m]["amount_ok"] += 1

        rows.append(row)

    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"\nCSV保存: {csv_path}")

    # ---------------------------------------------------------------------------
    # サマリー統計
    # ---------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("サマリー統計")
    print("=" * 70)
    print(f"{'モデル':<25} {'取引先名':>10} {'発行日':>10} {'金額':>10} {'総合':>10}")
    print("-" * 70)

    for m in models:
        s = stats[m]
        total = s["total"] or 1
        v_pct = s["vendor_ok"] / total * 100
        d_pct = s["date_ok"] / total * 100
        a_pct = s["amount_ok"] / total * 100
        overall = (s["vendor_ok"] + s["date_ok"] + s["amount_ok"]) / (total * 3) * 100
        print(
            f"{m:<25} "
            f"{s['vendor_ok']:>3}/{s['total']} ({v_pct:5.1f}%) "
            f"{s['date_ok']:>3}/{s['total']} ({d_pct:5.1f}%) "
            f"{s['amount_ok']:>3}/{s['total']} ({a_pct:5.1f}%) "
            f"({overall:5.1f}%)"
        )

    print("=" * 70)
    print("完了")


if __name__ == "__main__":
    main()
