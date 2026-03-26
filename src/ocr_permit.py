"""
ocr_permit.py
建設業許可証PDF処理スクリプト（東海インプル建設㈱）

処理フロー:
  data/inbox/ の PDF を読み込み → OCR + GPT-4o 抽出 → staging CSV 出力
  → data/processed/{success|review_needed|error|skip}/ に移動

Usage:
    python src/ocr_permit.py               # inbox/ 全件処理
    python src/ocr_permit.py --file path   # 単一ファイル処理
    python src/ocr_permit.py --dry-run     # 処理対象の表示のみ
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
import shutil
import sys
import time
import unicodedata
import base64
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

# ---------------------------------------------------------------------------
# Path setup — srcディレクトリをsys.pathに追加
# ---------------------------------------------------------------------------
_SRC_DIR = Path(__file__).parent
_PROJECT_ROOT = _SRC_DIR.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from utils.wareki_convert import wareki_to_date  # noqa: E402
from utils.permit_parser import parse_permit_number  # noqa: E402
from utils.trade_master import normalize_trade_list  # noqa: E402

# ---------------------------------------------------------------------------
# Error category constants
# ---------------------------------------------------------------------------
TIER1_DUPLICATE = "TIER1_DUPLICATE"
TIER1_OCR_TIMEOUT = "TIER1_OCR_TIMEOUT"
TIER1_SHEETS_TIMEOUT = "TIER1_SHEETS_TIMEOUT"
TIER1_VALIDATION_ERROR = "TIER1_VALIDATION_ERROR"
TIER2_ENCRYPT_PDF = "TIER2_ENCRYPT_PDF"
TIER2_OCR_LOW_QUALITY = "TIER2_OCR_LOW_QUALITY"
TIER2_LLM_PARSE = "TIER2_LLM_PARSE"
TIER2_NO_MANDATORY_FIELD = "TIER2_NO_MANDATORY_FIELD"
TIER2_AUTH_ERROR = "TIER2_AUTH_ERROR"
TIER3_PARTIAL_EXTRACT = "TIER3_PARTIAL_EXTRACT"
TIER3_UNKNOWN_COMPANY = "TIER3_UNKNOWN_COMPANY"
TIER3_AMBIGUOUS_COMPANY = "TIER3_AMBIGUOUS_COMPANY"
TIER3_DATE_PARSE = "TIER3_DATE_PARSE"
TIER3_TRADE_MISMATCH = "TIER3_TRADE_MISMATCH"
TIER3_AUTH_NAME_UNKNOWN = "TIER3_AUTH_NAME_UNKNOWN"

# ---------------------------------------------------------------------------
# Staging CSV columns (order matters)
# ---------------------------------------------------------------------------
STAGING_CSV_COLUMNS = [
    "source_file", "source_file_hash", "processed_at", "company_name_raw",
    "company_name_normalized", "company_id", "permit_authority_name",
    "permit_authority_name_normalized", "permit_authority_type", "permit_category",
    "permit_year", "contractor_number", "permit_number_full", "trade_categories",
    "issue_date", "expiry_date", "renewal_deadline_date", "parse_status",
    "error_category", "error_reason", "confidence_score", "retry_count",
]

# ---------------------------------------------------------------------------
# Mandatory fields for permit
# ---------------------------------------------------------------------------
MANDATORY_FIELDS = ["permit_number_full", "expiry_date", "contractor_number", "trade_categories"]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GPT-4o prompt
# ---------------------------------------------------------------------------
_EXTRACT_PROMPT = """あなたは建設業許可証のOCR後処理エキスパートです。
以下のOCRテキストから情報を抽出し、必ずJSON形式で回答してください。

重要: テキストに複数ページの内容が含まれています。いずれかのページに建設業許可証または建設業許可証明書の内容が含まれていれば、document_typeは"建設業許可証"とし、その許可証の情報を抽出してください。

抽出するフィールド:
- document_type: ["建設業許可証", "決算書", "会社案内", "工事経歴書", "取引先一覧表", "新規継続取引申請書", "労働安全衛生誓約書", "その他"] のいずれか。複数種類のページがある場合は建設業許可証を優先
- company_name_raw: 「許可を受けた者」の欄に記載された法人名または個人事業主名。宛先・担当者名ではない。株式会社・有限会社等の法人格を含む
- permit_number_full: 許可番号（例: 愛知県知事 許可（特-6）第57805号）
- permit_authority_name: 許可行政庁名（例: 愛知県知事）
- permit_category: "一般" または "特定"
- permit_year: 許可番号の括弧内にある数字（例: 般-3 なら 3、特-7 なら 7）。西暦年ではない。許可の世代番号である
- contractor_number: 業者番号（「第XXXXX号」の数字部分。例: "57805"）
- trade_categories: 建設業種リスト（例: ["土木工事業", "建築工事業"]）
- issue_date: 許可年月日（YYYY-MM-DD形式。和暦は西暦に変換: 令和4年=2022年, 令和7年=2025年）
- expiry_date: 有効期限（YYYY-MM-DD形式。和暦は西暦に変換）
- note: その他特記事項

document_typeが"建設業許可証"以外の場合でも、company_name_rawは必ず抽出してください（書類の提出元・発行元の会社名）。許可証固有のフィールドはnullとしてください。
不明なフィールドはnullとしてください。回答はJSONのみ。コードブロック不要。

OCRテキスト:
{ocr_text}"""

_EXTRACT_VISION_PROMPT = """あなたは建設業許可証のOCR処理エキスパートです。
この画像（複数ページの場合あり）から情報を抽出し、必ずJSON形式で回答してください。

重要: 複数ページがある場合、全ページを確認してください。いずれかのページに建設業許可証が含まれていれば、document_typeは"建設業許可証"とし、許可証のページから情報を抽出してください。

抽出するフィールド:
- document_type: ["建設業許可証", "決算書", "会社案内", "工事経歴書", "取引先一覧表", "新規継続取引申請書", "労働安全衛生誓約書", "その他"] のいずれか。複数種類のページがある場合は建設業許可証を優先
- company_name_raw: 「許可を受けた者」の欄に記載された法人名または個人事業主名。宛先・担当者名ではない。株式会社・有限会社等の法人格を含む
- permit_number_full: 許可番号の全文（例: 愛知県知事 許可（特-6）第57805号）
- permit_authority_name: 許可行政庁名（例: 愛知県知事、国土交通大臣）
- permit_category: "一般" または "特定"
- permit_year: 許可番号の括弧内にある数字（例: 般-3 なら 3、特-7 なら 7）。西暦年ではない。令和の年号番号でもない。許可の世代番号である
- contractor_number: 業者番号（「第XXXXX号」の数字部分。例: "57805"）
- trade_categories: 許可を受けた建設業の業種リスト（例: ["土木工事業", "建築工事業"]）
- issue_date: 許可年月日（YYYY-MM-DD形式。和暦は西暦に変換: 令和4年=2022年, 令和5年=2023年, 令和6年=2024年, 令和7年=2025年, 令和8年=2026年）
- expiry_date: 有効期間の満了日（YYYY-MM-DD形式。和暦は西暦に変換）
- note: その他特記事項

document_typeが"建設業許可証"以外の場合でも、company_name_rawは必ず抽出してください（書類の提出元・発行元の会社名）。許可証固有のフィールドはnullとしてください。
不明なフィールドはnullとしてください。回答はJSONのみ。コードブロック不要。"""


# ---------------------------------------------------------------------------
# 1. Config
# ---------------------------------------------------------------------------
def load_config() -> dict[str, Any]:
    """config.json をスクリプトの親ディレクトリ（プロジェクトルート）から読み込む。"""
    config_path = _PROJECT_ROOT / "config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"config.json が見つかりません: {config_path}")
    with config_path.open(encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# 2. File hash
# ---------------------------------------------------------------------------
def compute_file_hash(path: Path) -> str:
    """ファイルの SHA256 ハッシュを返す。"""
    sha256 = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


# ---------------------------------------------------------------------------
# 3. Retry with Exponential Backoff
# ---------------------------------------------------------------------------
def call_with_retry(
    fn: Callable[[], Any],
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> tuple[Any, int]:
    """
    Exponential Backoff でリトライする。
    Returns: (result, retry_count)
    Raises: 最後の例外をそのまま raise。
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            return fn(), attempt
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    "リトライ %d/%d (%.1fs待機): %s",
                    attempt + 1, max_retries, delay, exc,
                )
                time.sleep(delay)
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 4. PDF text extraction
# ---------------------------------------------------------------------------
def extract_text_from_pdf(pdf_path: Path) -> list[str]:
    """
    PDF からページ毎のテキストリストを返す。
    暗号化PDFの場合は ValueError を raise。
    pdfplumber → pypdf2 の順にフォールバック。
    """
    try:
        import pdfplumber  # noqa: PLC0415
        with pdfplumber.open(str(pdf_path)) as pdf:
            pages: list[str] = []
            for page in pdf.pages:
                text = page.extract_text() or ""
                pages.append(text)
            return pages
    except Exception as exc:
        if "encrypted" in str(exc).lower() or "password" in str(exc).lower():
            raise ValueError(f"PDF が暗号化されています: {pdf_path}") from exc
        logger.warning("pdfplumber 失敗、pypdf2 にフォールバック: %s", exc)

    try:
        import PyPDF2  # noqa: PLC0415
        pages = []
        with open(pdf_path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            if reader.is_encrypted:
                raise ValueError(f"PDF が暗号化されています: {pdf_path}")
            for page in reader.pages:
                pages.append(page.extract_text() or "")
        return pages
    except ValueError:
        raise
    except Exception as exc:
        raise RuntimeError(f"PDF テキスト抽出失敗: {pdf_path} — {exc}") from exc


def _pdf_to_images(
    pdf_path: Path, max_pages: int = 3, dpi: int = 200,
    page_indices: list[int] | None = None,
) -> list[bytes]:
    """PDF ページを PNG バイト列に変換する（PyMuPDF）。
    page_indices 指定時はそのページのみを変換する。
    """
    import fitz  # noqa: PLC0415
    doc = fitz.open(str(pdf_path))
    images: list[bytes] = []
    if page_indices is not None:
        for i in page_indices:
            if i < len(doc):
                pix = doc[i].get_pixmap(dpi=dpi)
                images.append(pix.tobytes("png"))
    else:
        for i, page in enumerate(doc):
            if i >= max_pages:
                break
            pix = page.get_pixmap(dpi=dpi)
            images.append(pix.tobytes("png"))
    doc.close()
    return images


# ---------------------------------------------------------------------------
# 5. GPT-4o extraction
# ---------------------------------------------------------------------------
def call_gpt4o_extract(ocr_text: str, model: str, api_key: str) -> dict[str, Any]:
    """
    GPT-4o に OCR テキストを渡してフィールド抽出。
    JSON パース失敗時は ValueError を raise。
    """
    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError as exc:
        raise RuntimeError("openai パッケージが未インストールです: pip install openai") from exc

    client = OpenAI(api_key=api_key)
    prompt = _EXTRACT_PROMPT.format(ocr_text=ocr_text[:4000])  # トークン節約

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={"type": "json_object"},
    )

    raw = response.choices[0].message.content or ""
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"GPT-4o レスポンスの JSON パース失敗: {exc}\nraw={raw[:200]}") from exc


def call_gpt4o_vision_extract(
    page_images: list[bytes], model: str, api_key: str
) -> dict[str, Any]:
    """
    GPT-4o Vision にページ画像を渡してフィールド抽出。
    JSON パース失敗時は ValueError を raise。
    """
    try:
        from openai import OpenAI  # noqa: PLC0415
    except ImportError as exc:
        raise RuntimeError("openai パッケージが未インストールです: pip install openai") from exc

    client = OpenAI(api_key=api_key)

    content: list[dict[str, Any]] = []
    for img_bytes in page_images:
        b64 = base64.b64encode(img_bytes).decode()
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64}"},
        })
    content.append({"type": "text", "text": _EXTRACT_VISION_PROMPT})

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": content}],
        temperature=0,
        max_tokens=1024,
        response_format={"type": "json_object"},
    )

    raw = response.choices[0].message.content or ""
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"GPT-4o Vision レスポンスの JSON パース失敗: {exc}\nraw={raw[:200]}") from exc


# ---------------------------------------------------------------------------
# 6. Company matching
# ---------------------------------------------------------------------------
def _normalize_company_name(name: str) -> str:
    """会社名の表記ゆれ正規化（import_company_masterと同一ロジック）。"""
    if not name:
        return ""
    import re
    normalized = unicodedata.normalize("NFKC", name)
    normalized = normalized.replace("\u3000", " ")
    normalized = normalized.replace("㈱", "株式会社")
    normalized = normalized.replace("㈲", "有限会社")
    normalized = re.sub(r"\s*[（(]株[）)]\s*", "株式会社", normalized)
    normalized = re.sub(r"\s*[（(]有[）)]\s*", "有限会社", normalized)
    return normalized.strip()


def match_company(
    company_name_normalized: str,
    companies_csv_path: Path,
) -> tuple[str, str]:
    """
    正規化済み会社名を Companies CSV と照合。
    Returns: (company_id, match_type)
        match_type: "exact" | "ambiguous" | "not_found"
    """
    if not companies_csv_path.exists():
        return "", "not_found"

    exact_matches: list[str] = []
    partial_matches: list[str] = []

    with companies_csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            csv_name = row.get("company_name_normalized", "").strip()
            csv_id = row.get("company_id", "").strip()
            if csv_name == company_name_normalized:
                exact_matches.append(csv_id)
            elif company_name_normalized in csv_name or csv_name in company_name_normalized:
                partial_matches.append(csv_id)

    if len(exact_matches) == 1:
        return exact_matches[0], "exact"
    if len(exact_matches) > 1:
        return exact_matches[0], "ambiguous"  # 複数完全一致
    # partial matches → always ambiguous (not exact)
    if partial_matches:
        return partial_matches[0], "ambiguous"
    return "", "not_found"


# ---------------------------------------------------------------------------
# Permit authority name normalization
# ---------------------------------------------------------------------------
# 都道府県 → 正式表記 マッピング（全47）
_PREFECTURE_SUFFIX: dict[str, str] = {
    "北海道": "北海道",
    "青森": "青森県", "岩手": "岩手県", "宮城": "宮城県", "秋田": "秋田県",
    "山形": "山形県", "福島": "福島県", "茨城": "茨城県", "栃木": "栃木県",
    "群馬": "群馬県", "埼玉": "埼玉県", "千葉": "千葉県", "東京": "東京都",
    "神奈川": "神奈川県", "新潟": "新潟県", "富山": "富山県", "石川": "石川県",
    "福井": "福井県", "山梨": "山梨県", "長野": "長野県", "岐阜": "岐阜県",
    "静岡": "静岡県", "愛知": "愛知県", "三重": "三重県", "滋賀": "滋賀県",
    "京都": "京都府", "大阪": "大阪府", "兵庫": "兵庫県", "奈良": "奈良県",
    "和歌山": "和歌山県", "鳥取": "鳥取県", "島根": "島根県", "岡山": "岡山県",
    "広島": "広島県", "山口": "山口県", "徳島": "徳島県", "香川": "香川県",
    "愛媛": "愛媛県", "高知": "高知県", "福岡": "福岡県", "佐賀": "佐賀県",
    "長崎": "長崎県", "熊本": "熊本県", "大分": "大分県", "宮崎": "宮崎県",
    "鹿児島": "鹿児島県", "沖縄": "沖縄県",
}


def normalize_authority_name(raw: str) -> str:
    """許可行政庁名の表記ゆれを正規化する（全47都道府県対応）。"""
    if not raw:
        return ""
    # 全角スペース・前後空白除去 + NFKC正規化
    name = unicodedata.normalize("NFKC", raw).replace("\u3000", "").strip()

    # 国土交通大臣はそのまま
    if "大臣" in name:
        return "国土交通大臣"

    # 「XX知事」パターン: XX が都道府県名に対応するか確認
    m = re.match(r'^(.+?)知事$', name)
    if m:
        prefix = m.group(1).strip()
        if prefix in _PREFECTURE_SUFFIX:
            # 短縮形（例: "愛知"）→ 正式表記（例: "愛知県知事"）
            full_pref = _PREFECTURE_SUFFIX[prefix]
            return f"{full_pref}知事"
        # prefix が "XX県/都/道/府" の形の場合（例: "愛知県"）→ そのまま返す
        for full in _PREFECTURE_SUFFIX.values():
            if prefix == full:  # すでに正式表記
                return f"{full}知事"

    return name  # 上記パターン外はそのまま（大臣許可・特殊ケース）


# 正規化済み行政庁名として受け入れられる形式かチェック
_VALID_AUTHORITY_NAMES: frozenset[str] = frozenset(
    [f"{v}知事" for v in _PREFECTURE_SUFFIX.values()] + ["国土交通大臣"]
)


def is_valid_authority_name(name: str) -> bool:
    """normalize_authority_name の出力が既知の正式表記かどうかを返す。"""
    return name in _VALID_AUTHORITY_NAMES


# ---------------------------------------------------------------------------
# Confidence estimation
# ---------------------------------------------------------------------------
def estimate_confidence(pages: list[str]) -> float:
    """
    テキスト量からOCR信頼度を推定する（簡易ヒューリスティック）。
    100文字/ページ未満 → 低品質とみなす。
    """
    if not pages:
        return 0.0
    avg_chars = sum(len(p) for p in pages) / len(pages)
    if avg_chars < 100:
        return 0.2
    if avg_chars < 300:
        return 0.45
    return 0.8


# ---------------------------------------------------------------------------
# Date parsing helper
# ---------------------------------------------------------------------------
def parse_date_field(raw: str | None) -> tuple[date | None, bool]:
    """
    日付フィールドを date に変換。
    Returns: (date_obj, parse_ok)
    """
    if not raw:
        return None, False
    # ISO 形式（YYYY-MM-DD）
    try:
        return date.fromisoformat(raw), True
    except (ValueError, TypeError):
        pass
    # 和暦
    result = wareki_to_date(raw)
    if result is not None:
        return result, True
    return None, False


# ---------------------------------------------------------------------------
# API key loader
# ---------------------------------------------------------------------------
def load_api_key(key_file: str) -> str:
    """APIキーをファイルから読み込む。"""
    path = Path(key_file)
    if not path.exists():
        raise FileNotFoundError(f"APIキーファイルが見つかりません: {path}")
    return path.read_text(encoding="utf-8").strip()


# ---------------------------------------------------------------------------
# 7a. Validate file and check duplicate hash
# ---------------------------------------------------------------------------
def _validate_and_hash(
    pdf_path: Path, known_hashes: set[str], result: dict[str, Any]
) -> bool:
    """バリデーション + 重複チェック。問題があれば result を更新して False を返す。"""
    if pdf_path.suffix.lower() != ".pdf":
        result["error_category"] = TIER1_VALIDATION_ERROR
        result["error_reason"] = "拡張子が .pdf ではありません"
        return False

    file_size = pdf_path.stat().st_size
    if file_size == 0:
        result["error_category"] = TIER1_VALIDATION_ERROR
        result["error_reason"] = "ファイルサイズが 0 bytes"
        return False

    if file_size > 50 * 1024 * 1024:
        result["error_category"] = TIER1_VALIDATION_ERROR
        result["error_reason"] = f"ファイルサイズ超過: {file_size // 1024 // 1024}MB > 50MB"
        return False

    file_hash = compute_file_hash(pdf_path)
    result["source_file_hash"] = file_hash
    if file_hash in known_hashes:
        result["parse_status"] = "SKIP"
        result["error_category"] = TIER1_DUPLICATE
        result["error_reason"] = "同一ハッシュが処理済みです"
        return False
    known_hashes.add(file_hash)
    return True


# ---------------------------------------------------------------------------
# 7b. Extract OCR text and check confidence
# ---------------------------------------------------------------------------
def _extract_and_check_confidence(
    pdf_path: Path, config: dict[str, Any], result: dict[str, Any]
) -> tuple[str | list[bytes], bool]:
    """テキスト抽出 + 信頼度チェック。低品質時はVision用画像を返す。"""
    try:
        pages = extract_text_from_pdf(pdf_path)
    except ValueError as exc:
        result["error_category"] = TIER2_ENCRYPT_PDF
        result["error_reason"] = str(exc)
        return "", False
    except Exception as exc:
        result["error_category"] = TIER1_OCR_TIMEOUT
        result["error_reason"] = str(exc)
        return "", False

    confidence = estimate_confidence(pages)
    result["confidence_score"] = f"{confidence:.2f}"
    low_thresh = config.get("OCR_CONFIDENCE_THRESHOLD_LOW", 0.30)
    review_thresh = config.get("OCR_CONFIDENCE_THRESHOLD_REVIEW", 0.60)

    if confidence < low_thresh:
        # テキスト抽出不足 → Vision fallback（画像を返す）
        # 最初3ページ + 最後3ページを送信（許可証が末尾にある場合に対応）
        logger.info("[%s] テキスト抽出不足 (confidence=%.2f) → Vision fallback", pdf_path.name, confidence)
        try:
            import fitz as _fitz  # noqa: PLC0415
            _doc = _fitz.open(str(pdf_path))
            total_pages = len(_doc)
            _doc.close()
            if total_pages <= 6:
                target = list(range(total_pages))
            else:
                head = list(range(3))
                tail = list(range(total_pages - 3, total_pages))
                target = head + [i for i in tail if i not in head]
            logger.info("[%s] Vision: %dページ中 %s を送信", pdf_path.name, total_pages, target)
            images = _pdf_to_images(pdf_path, dpi=300, page_indices=target)
            if not images:
                result["error_category"] = TIER2_OCR_LOW_QUALITY
                result["error_reason"] = "Vision fallback: ページ画像の生成に失敗"
                return "", False
            return images, True  # Vision mode: list[bytes] を返す
        except Exception as exc:
            result["error_category"] = TIER2_OCR_LOW_QUALITY
            result["error_reason"] = f"Vision fallback 画像変換失敗: {exc}"
            return "", False

    # 空白ページ（画像スキャン）が混在するか検出
    # テキストが少ないページ（画像スキャンの可能性）を検出
    empty_pages = [i for i, p in enumerate(pages) if len(p.strip()) < 100]
    if empty_pages and len(empty_pages) >= 2:
        # テキストページと空白（画像）ページが混在 → 空白ページをVisionで処理
        logger.info(
            "[%s] 空白ページ %d件検出 → 空白ページをVision処理",
            pdf_path.name, len(empty_pages),
        )
        try:
            # 空白ページを直接指定して画像化（最大3枚）
            target_pages = empty_pages[:3]
            empty_images = _pdf_to_images(pdf_path, dpi=300, page_indices=target_pages)
            if empty_images:
                return empty_images, True
        except Exception as exc:
            logger.warning("[%s] 空白ページVision変換失敗: %s", pdf_path.name, exc)

    if confidence < review_thresh:
        logger.warning("[%s] OCR信頼度低 (%.2f) — 処理は継続します", pdf_path.name, confidence)

    ocr_text = "\n\n--- PAGE BREAK ---\n\n".join(pages)
    return ocr_text, True


# ---------------------------------------------------------------------------
# 7c. Call GPT-4o with retry
# ---------------------------------------------------------------------------
def _call_llm(
    ocr_data: str | list[bytes], config: dict[str, Any], result: dict[str, Any]
) -> tuple[dict[str, Any], bool]:
    """GPT-4o 呼び出し。ocr_data が list[bytes] の場合は Vision モードで呼び出す。"""
    api_key = load_api_key(config["OPENAI_API_KEY_FILE"])
    model = config.get("OPENAI_MODEL", "gpt-4o")
    max_retries = config.get("RETRY_MAX", 3)
    base_delay = config.get("RETRY_BASE_DELAY_SEC", 1.0)

    is_vision = isinstance(ocr_data, list)

    try:
        if is_vision:
            extracted, retry_count = call_with_retry(
                lambda: call_gpt4o_vision_extract(ocr_data, model, api_key),
                max_retries=max_retries,
                base_delay=base_delay,
            )
            logger.info("  Vision モードで抽出完了")
        else:
            extracted, retry_count = call_with_retry(
                lambda: call_gpt4o_extract(ocr_data, model, api_key),
                max_retries=max_retries,
                base_delay=base_delay,
            )
        result["retry_count"] = retry_count
        return extracted, True
    except Exception as exc:
        result["error_category"] = TIER2_LLM_PARSE
        result["error_reason"] = f"GPT-4o 呼び出し失敗: {exc}"
        return {}, False


# ---------------------------------------------------------------------------
# 7d. Map extracted JSON to result dict
# ---------------------------------------------------------------------------
def _map_extracted_fields(
    extracted: dict[str, Any], result: dict[str, Any], pdf_name: str
) -> None:
    """GPT-4o 抽出結果を result に展開する（副作用のみ）。"""
    company_name_raw = extracted.get("company_name_raw") or ""
    result["company_name_raw"] = company_name_raw
    result["company_name_normalized"] = _normalize_company_name(company_name_raw)
    result["permit_number_full"] = extracted.get("permit_number_full") or ""
    result["permit_authority_name"] = extracted.get("permit_authority_name") or ""
    result["permit_authority_name_normalized"] = normalize_authority_name(
        result["permit_authority_name"]
    )
    result["permit_category"] = extracted.get("permit_category") or ""
    result["permit_year"] = extracted.get("permit_year") or ""
    result["contractor_number"] = extracted.get("contractor_number") or ""

    # 許可番号パース（抽出値を補完）
    permit_parse = parse_permit_number(result["permit_number_full"])
    if permit_parse.parse_success:
        result["permit_authority_name"] = (
            result["permit_authority_name"] or permit_parse.permit_authority_name
        )
        result["permit_authority_name_normalized"] = normalize_authority_name(
            result["permit_authority_name"]
        )
        result["permit_authority_type"] = permit_parse.permit_authority_type
        result["permit_category"] = result["permit_category"] or permit_parse.permit_category
        result["permit_year"] = result["permit_year"] or permit_parse.permit_year
        result["contractor_number"] = (
            result["contractor_number"] or permit_parse.contractor_number
        )
    else:
        logger.warning("[%s] 許可番号パース失敗: %s", pdf_name, permit_parse.parse_warnings)

    # 行政庁名の正規化結果が既知形式でなければ TIER3 に落とす
    # （OCR誤字等でupsertキーが揺れ、重複登録を起こすリスクを予防）
    auth_normalized = result.get("permit_authority_name_normalized", "")
    if auth_normalized and not is_valid_authority_name(auth_normalized):
        result["error_category"] = TIER3_AUTH_NAME_UNKNOWN
        result["error_reason"] = (
            f"行政庁名を既知の47都道府県知事/国土交通大臣に正規化できませんでした: "
            f"'{result.get('permit_authority_name')}' → '{auth_normalized}'"
        )
        logger.warning("[%s] %s: %s", pdf_name, TIER3_AUTH_NAME_UNKNOWN, result["error_reason"])


def _validate_extracted_fields(
    extracted: dict[str, Any], result: dict[str, Any], pdf_name: str
) -> list[str]:
    """抽出結果のバリデーションと自動修正。修正した場合は警告リストに追加。"""
    warnings: list[str] = []

    # 1. permit_year がカレンダー年（> 20）の場合、許可番号から再抽出
    permit_year = extracted.get("permit_year")
    if permit_year is not None:
        try:
            py = int(permit_year)
            if py > 20:
                # 許可番号から再抽出を試みる
                pn = result.get("permit_number_full", "")
                m = re.search(r'[（(][\u822c\u7279\w]*[ー\-](\d{1,2})[）)]', pn)
                if m:
                    corrected = int(m.group(1))
                    warnings.append(
                        f"permit_year自動修正: {py} → {corrected}（許可番号から再抽出）"
                    )
                    result["permit_year"] = str(corrected)
                    extracted["permit_year"] = corrected
                    logger.info("[%s] permit_year修正: %d → %d", pdf_name, py, corrected)
                else:
                    warnings.append(f"permit_year異常値: {py}（許可番号からの再抽出失敗）")
        except (ValueError, TypeError):
            pass

    # 2. company_name_raw に「様」「殿」が含まれる場合は個人名の可能性
    company = result.get("company_name_raw", "")
    if company and ("様" in company or "殿" in company):
        warnings.append(f"company_name_rawが個人名の可能性: '{company}'")
        logger.warning("[%s] 会社名が個人名の可能性: %s", pdf_name, company)

    # 3. contractor_number の桁数チェック（通常3-6桁）
    cn = result.get("contractor_number", "")
    if cn:
        try:
            cn_int = int(cn)
            if cn_int > 200000:
                warnings.append(f"contractor_number異常値: {cn}（通常3-6桁）")
                logger.warning("[%s] contractor_number異常: %s", pdf_name, cn)
        except (ValueError, TypeError):
            pass

    return warnings


# ---------------------------------------------------------------------------
# 7e. Normalize trades and dates, collect warnings
# ---------------------------------------------------------------------------
def _normalize_trades_and_dates(
    extracted: dict[str, Any], result: dict[str, Any]
) -> list[str]:
    """業種正規化 + 日付パースを行い、警告リストを返す。"""
    warnings: list[str] = []

    # 業種
    raw_trades: list[str] = extracted.get("trade_categories") or []
    if isinstance(raw_trades, str):
        raw_trades = [t.strip() for t in raw_trades.split(",") if t.strip()]
    normalized_trades, failed_trades = normalize_trade_list(raw_trades)
    result["trade_categories"] = "|".join(normalized_trades)
    if failed_trades:
        warnings.append(f"業種名正規化失敗: {failed_trades}")
        result["error_category"] = TIER3_TRADE_MISMATCH

    # 日付
    issue_date, _ = parse_date_field(extracted.get("issue_date"))
    expiry_date, expiry_ok = parse_date_field(extracted.get("expiry_date"))
    result["issue_date"] = (
        issue_date.isoformat() if issue_date else (extracted.get("issue_date") or "")
    )
    result["expiry_date"] = (
        expiry_date.isoformat() if expiry_date else (extracted.get("expiry_date") or "")
    )
    if not expiry_ok:
        warnings.append(f"有効期限の日付パース失敗: {extracted.get('expiry_date')}")
        if not result.get("error_category"):
            result["error_category"] = TIER3_DATE_PARSE
    if expiry_date:
        result["renewal_deadline_date"] = (expiry_date - timedelta(days=30)).isoformat()

    return warnings


# ---------------------------------------------------------------------------
# 7f. Company match and determine parse_status
# ---------------------------------------------------------------------------
def _resolve_company_and_status(
    result: dict[str, Any], data_root: Path, warnings: list[str]
) -> None:
    """会社マッチングと最終ステータス確定（副作用のみ）。"""
    csv_candidates = sorted(
        (data_root / "output").glob("companies_import_*.csv"), reverse=True
    )
    companies_csv = csv_candidates[0] if csv_candidates else data_root / "output" / "companies_import_latest.csv"
    company_id, match_type = match_company(result["company_name_normalized"], companies_csv)
    result["company_id"] = company_id

    if match_type == "not_found":
        result["error_category"] = TIER3_UNKNOWN_COMPANY
        warnings.append(f"会社マスタに未登録: {result['company_name_normalized']}")
        result["parse_status"] = "REVIEW_NEEDED"
    elif match_type == "ambiguous":
        if not result.get("error_category"):
            result["error_category"] = TIER3_AMBIGUOUS_COMPANY
        warnings.append(f"会社名が曖昧一致: {result['company_name_normalized']}")

    if not result.get("parse_status") or result["parse_status"] == "ERROR":
        if not result.get("error_category"):
            result["parse_status"] = "OK"
        elif result["error_category"].startswith("TIER3"):
            result["parse_status"] = "REVIEW_NEEDED"

    result["error_reason"] = " / ".join(warnings) if warnings else ""


# ---------------------------------------------------------------------------
# 7. Process single PDF (orchestrator)
# ---------------------------------------------------------------------------
def process_pdf(
    pdf_path: Path,
    config: dict[str, Any],
    known_hashes: set[str],
) -> dict[str, Any]:
    """1つのPDFを処理してステージングレコード辞書を返す。エラー時も必ず辞書を返す。"""
    data_root = Path(config["DATA_ROOT"])
    result: dict[str, Any] = {col: "" for col in STAGING_CSV_COLUMNS}
    result["source_file"] = pdf_path.name
    result["processed_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    result["parse_status"] = "ERROR"
    result["retry_count"] = 0

    if not _validate_and_hash(pdf_path, known_hashes, result):
        return result

    ocr_data, ok = _extract_and_check_confidence(pdf_path, config, result)
    if not ok:
        return result

    extracted, ok = _call_llm(ocr_data, config, result)
    if not ok:
        return result

    doc_type = extracted.get("document_type", "")
    # 非許可証でも会社名を記録（書類チェックリスト用）
    skip_company = extracted.get("company_name_raw") or ""
    if skip_company:
        result["company_name_raw"] = skip_company
        result["company_name_normalized"] = _normalize_company_name(skip_company)
    if doc_type != "建設業許可証":
        result["parse_status"] = "SKIP"
        result["error_reason"] = f"document_type={doc_type}（許可証以外）"
        return result

    _map_extracted_fields(extracted, result, pdf_path.name)

    validation_warnings = _validate_extracted_fields(extracted, result, pdf_path.name)

    missing = [f for f in MANDATORY_FIELDS if not extracted.get(f)]
    if missing:
        result["error_category"] = TIER2_NO_MANDATORY_FIELD
        result["error_reason"] = f"必須フィールド不足: {missing}"
        return result

    warnings = _normalize_trades_and_dates(extracted, result)
    warnings = validation_warnings + warnings
    _resolve_company_and_status(result, data_root, warnings)
    return result


# ---------------------------------------------------------------------------
# Folder routing
# ---------------------------------------------------------------------------
def destination_folder(data_root: Path, parse_status: str) -> Path:
    """parse_status に応じた移動先サブフォルダを返す。"""
    mapping = {
        "OK": "success",
        "REVIEW_NEEDED": "review_needed",
        "ERROR": "error",
        "SKIP": "skip",
    }
    subfolder = mapping.get(parse_status, "error")
    dest = data_root / "data" / "processed" / subfolder
    dest.mkdir(parents=True, exist_ok=True)
    return dest


def move_pdf(pdf_path: Path, dest_dir: Path) -> None:
    """PDFをdest_dirに移動。同名ファイルが存在する場合はタイムスタンプを付与。"""
    dest = dest_dir / pdf_path.name
    if dest.exists():
        stem = pdf_path.stem
        suffix = pdf_path.suffix
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        dest = dest_dir / f"{stem}_{ts}{suffix}"
    shutil.move(str(pdf_path), str(dest))
    logger.info("移動: %s → %s", pdf_path.name, dest)


# ---------------------------------------------------------------------------
# Staging CSV write
# ---------------------------------------------------------------------------
def write_staging_csv(records: list[dict[str, Any]], output_dir: Path) -> Path:
    """処理結果を staging CSV に書き出す。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"staging_permits_{ts}.csv"

    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=STAGING_CSV_COLUMNS,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(records)

    logger.info("Staging CSV 出力: file:///%s", str(csv_path).replace("\\", "/"))
    return csv_path


# ---------------------------------------------------------------------------
# 8. main helpers
# ---------------------------------------------------------------------------
def _process_one(
    pdf_path: Path, config: dict[str, Any], known_hashes: set[str], data_root: Path
) -> dict[str, Any]:
    """1 ファイルを処理して result を返す。移動まで担当。"""
    logger.info("処理開始: %s", pdf_path.name)
    try:
        record = process_pdf(pdf_path, config, known_hashes)
    except Exception as exc:
        logger.error("予期しないエラー [%s]: %s", pdf_path.name, exc)
        record = {col: "" for col in STAGING_CSV_COLUMNS}
        record["source_file"] = pdf_path.name
        record["processed_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        record["parse_status"] = "ERROR"
        record["error_category"] = TIER1_VALIDATION_ERROR
        record["error_reason"] = str(exc)

    logger.info(
        "  → parse_status=%s  error=%s",
        record.get("parse_status"),
        record.get("error_category") or "なし",
    )
    parse_status = record.get("parse_status", "ERROR")
    dest = destination_folder(data_root, parse_status)
    if pdf_path.exists():
        move_pdf(pdf_path, dest)
    return record


def _collect_files(inbox_dir: Path, single_file: str | None) -> list[Path]:
    """処理対象ファイルリストを返す。"""
    if single_file:
        return [Path(single_file)]
    return sorted(inbox_dir.glob("*.pdf"))


# ---------------------------------------------------------------------------
# 8. main()
# ---------------------------------------------------------------------------
def main() -> None:
    args = _parse_args()
    config = load_config()
    data_root = Path(config["DATA_ROOT"])
    inbox_dir = data_root / "data" / "inbox"
    output_dir = data_root / "output"

    if args.dry_run:
        _run_dry(inbox_dir, args.file)
        return

    pdf_files = _collect_files(inbox_dir, args.file)
    if not pdf_files:
        logger.info("処理対象PDFなし。終了します。")
        return

    logger.info("処理対象: %d 件", len(pdf_files))
    known_hashes: set[str] = set()
    records = [_process_one(p, config, known_hashes, data_root) for p in pdf_files]

    if records:
        write_staging_csv(records, output_dir)
    _print_summary(records)


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------
def _run_dry(inbox_dir: Path, single_file: str | None) -> None:
    """対象ファイルを表示するのみで処理しない。"""
    if single_file:
        files = [Path(single_file)]
    else:
        files = sorted(inbox_dir.glob("*.pdf"))

    print(f"\n=== DRY RUN: {len(files)} 件のPDFが処理対象 ===")
    for f in files:
        size_kb = f.stat().st_size // 1024 if f.exists() else -1
        print(f"  {f.name}  ({size_kb} KB)")
    print("実際の処理は行いません。\n")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
def _print_summary(records: list[dict[str, Any]]) -> None:
    counts: dict[str, int] = {}
    for rec in records:
        status = rec.get("parse_status", "ERROR")
        counts[status] = counts.get(status, 0) + 1

    print("\n=== 処理結果サマリー ===")
    print(f"  合計:           {len(records)} 件")
    for status, count in sorted(counts.items()):
        print(f"  {status:<15}: {count} 件")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="建設業許可証PDF処理スクリプト（東海インプル建設㈱）"
    )
    parser.add_argument(
        "--file",
        metavar="PATH",
        help="単一PDFファイルのパスを指定（省略時はinbox/全件処理）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="処理対象の表示のみ。実際の処理・移動は行わない",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
