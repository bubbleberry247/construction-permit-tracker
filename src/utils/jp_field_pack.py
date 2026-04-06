"""
jp_field_pack.py
OCR後処理の日本語フィールド正規化統合パック。

既存の utils モジュール（wareki_convert, permit_parser）をラップし、
会社名・住所・電話番号・金額・郵便番号などの正規化を統合インターフェースで提供する。
既存モジュールは変更せず、このモジュールがファサードとなる。
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

from utils.wareki_convert import wareki_to_iso
from utils.permit_parser import parse_permit_number


# ---------------------------------------------------------------------------
# NormalizedField: 正規化結果を保持するデータクラス
# ---------------------------------------------------------------------------
@dataclass
class NormalizedField:
    raw: str                           # 原文（監査用）
    normalized: str                    # 正規化値（機械可読）
    field_type: str                    # "company_name", "address", etc.
    changed: bool                      # raw != normalized
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# 低レベルユーティリティ
# ---------------------------------------------------------------------------

# 全角英数字→半角変換テーブル（unicodedata.normalize('NFKC') でも可能だが、
# カタカナを半角化したくないため手動テーブルを使用）
_ZEN_TO_HAN_TABLE = str.maketrans(
    'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ'
    'ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ'
    '０１２３４５６７８９'
    '！＂＃＄％＆＇（）＊＋，－．／：；＜＝＞？＠'
    '［＼］＾＿｀｛｜｝～　',
    'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    'abcdefghijklmnopqrstuvwxyz'
    '0123456789'
    '!"#$%&\'()*+,-./:;<=>?@'
    '[\\]^_`{|}~ ',
)

# 半角カタカナ→全角カタカナマッピング
_HAN_KANA_MAP: dict[str, str] = {
    'ｦ': 'ヲ', 'ｧ': 'ァ', 'ｨ': 'ィ', 'ｩ': 'ゥ', 'ｪ': 'ェ',
    'ｫ': 'ォ', 'ｬ': 'ャ', 'ｭ': 'ュ', 'ｮ': 'ョ', 'ｯ': 'ッ',
    'ｰ': 'ー', 'ｱ': 'ア', 'ｲ': 'イ', 'ｳ': 'ウ', 'ｴ': 'エ',
    'ｵ': 'オ', 'ｶ': 'カ', 'ｷ': 'キ', 'ｸ': 'ク', 'ｹ': 'ケ',
    'ｺ': 'コ', 'ｻ': 'サ', 'ｼ': 'シ', 'ｽ': 'ス', 'ｾ': 'セ',
    'ｿ': 'ソ', 'ﾀ': 'タ', 'ﾁ': 'チ', 'ﾂ': 'ツ', 'ﾃ': 'テ',
    'ﾄ': 'ト', 'ﾅ': 'ナ', 'ﾆ': 'ニ', 'ﾇ': 'ヌ', 'ﾈ': 'ネ',
    'ﾉ': 'ノ', 'ﾊ': 'ハ', 'ﾋ': 'ヒ', 'ﾌ': 'フ', 'ﾍ': 'ヘ',
    'ﾎ': 'ホ', 'ﾏ': 'マ', 'ﾐ': 'ミ', 'ﾑ': 'ム', 'ﾒ': 'メ',
    'ﾓ': 'モ', 'ﾔ': 'ヤ', 'ﾕ': 'ユ', 'ﾖ': 'ヨ', 'ﾗ': 'ラ',
    'ﾘ': 'リ', 'ﾙ': 'ル', 'ﾚ': 'レ', 'ﾛ': 'ロ', 'ﾜ': 'ワ',
    'ﾝ': 'ン',
    # 濁点・半濁点
    'ﾞ': '゛', 'ﾟ': '゜',
}

# 濁音・半濁音の結合ペア（半角カナ + 濁点/半濁点 → 全角濁音/半濁音）
_DAKUTEN_MAP: dict[str, str] = {
    'ｶﾞ': 'ガ', 'ｷﾞ': 'ギ', 'ｸﾞ': 'グ', 'ｹﾞ': 'ゲ', 'ｺﾞ': 'ゴ',
    'ｻﾞ': 'ザ', 'ｼﾞ': 'ジ', 'ｽﾞ': 'ズ', 'ｾﾞ': 'ゼ', 'ｿﾞ': 'ゾ',
    'ﾀﾞ': 'ダ', 'ﾁﾞ': 'ヂ', 'ﾂﾞ': 'ヅ', 'ﾃﾞ': 'デ', 'ﾄﾞ': 'ド',
    'ﾊﾞ': 'バ', 'ﾋﾞ': 'ビ', 'ﾌﾞ': 'ブ', 'ﾍﾞ': 'ベ', 'ﾎﾞ': 'ボ',
    'ﾊﾟ': 'パ', 'ﾋﾟ': 'ピ', 'ﾌﾟ': 'プ', 'ﾍﾟ': 'ペ', 'ﾎﾟ': 'ポ',
    'ｳﾞ': 'ヴ',
}

# 法人格の正規化マッピング
_CORP_TYPE_MAP: dict[str, str] = {
    '㈱': '株式会社',
    '㈲': '有限会社',
    '(株)': '株式会社',
    '(有)': '有限会社',
    '（株）': '株式会社',
    '（有）': '有限会社',
}

# 住所の丁目・番地パターン
_ADDRESS_CHOME_PATTERN = re.compile(
    r'(\d+)\s*丁目\s*(\d+)\s*番(?:地)?\s*(\d+)\s*号?'
)
_ADDRESS_BANCHI_PATTERN = re.compile(
    r'(\d+)\s*番地?\s*(\d+)\s*号?'
)


def zen_to_han(text: str) -> str:
    """全角英数字・記号を半角に変換する。全角カタカナは変換しない。"""
    if not text:
        return text
    return text.translate(_ZEN_TO_HAN_TABLE)


def han_to_zen_kana(text: str) -> str:
    """半角カタカナを全角カタカナに変換する。濁音・半濁音も結合処理する。"""
    if not text:
        return text
    # 濁音・半濁音ペアを先に処理（2文字 → 1文字）
    for han_pair, zen_char in _DAKUTEN_MAP.items():
        text = text.replace(han_pair, zen_char)
    # 残りの半角カナを変換
    result: list[str] = []
    for ch in text:
        result.append(_HAN_KANA_MAP.get(ch, ch))
    return ''.join(result)


def normalize_whitespace(text: str) -> str:
    """全角スペースを半角に変換し、連続スペースを除去、前後トリムする。"""
    if not text:
        return text
    # 全角スペース → 半角
    text = text.replace('\u3000', ' ')
    # 連続スペース → 単一スペース
    text = re.sub(r' {2,}', ' ', text)
    return text.strip()


# ---------------------------------------------------------------------------
# フィールド単位の正規化関数
# ---------------------------------------------------------------------------

def _safe_str(raw: object) -> str:
    """None/非文字列を空文字に変換する。"""
    if raw is None:
        return ''
    if not isinstance(raw, str):
        return str(raw)
    return raw


def normalize_company_name(raw: str) -> NormalizedField:
    """会社名正規化。法人格統一、全角半角、前後空白除去。

    ただし表記ゆれ自動同一判定はしない（CLAUDE.mdルール）。
    """
    raw_str = _safe_str(raw)
    if not raw_str.strip():
        return NormalizedField(
            raw=raw_str, normalized='', field_type='company_name',
            changed=bool(raw_str), warnings=['空の会社名'],
        )

    result = normalize_whitespace(zen_to_han(raw_str))
    result = han_to_zen_kana(result)

    # 法人格の正規化
    for abbr, full in _CORP_TYPE_MAP.items():
        result = result.replace(abbr, full)

    warnings: list[str] = []
    if result != raw_str:
        # 法人格変換があった場合に警告
        for abbr in _CORP_TYPE_MAP:
            if abbr in raw_str:
                warnings.append(f'法人格略称 "{abbr}" を正式名称に展開しました')

    return NormalizedField(
        raw=raw_str, normalized=result, field_type='company_name',
        changed=result != raw_str, warnings=warnings,
    )


def normalize_address(raw: str) -> NormalizedField:
    """住所正規化。丁目・番地のハイフン統一、全角→半角数字。"""
    raw_str = _safe_str(raw)
    if not raw_str.strip():
        return NormalizedField(
            raw=raw_str, normalized='', field_type='address',
            changed=bool(raw_str), warnings=['空の住所'],
        )

    result = normalize_whitespace(zen_to_han(raw_str))
    result = han_to_zen_kana(result)

    # 丁目番地号 → ハイフン形式
    result = _ADDRESS_CHOME_PATTERN.sub(r'\1-\2-\3', result)
    result = _ADDRESS_BANCHI_PATTERN.sub(r'\1-\2', result)

    warnings: list[str] = []
    return NormalizedField(
        raw=raw_str, normalized=result, field_type='address',
        changed=result != raw_str, warnings=warnings,
    )


def normalize_phone(raw: str) -> NormalizedField:
    """電話番号正規化。全角→半角、ハイフン統一。"""
    raw_str = _safe_str(raw)
    if not raw_str.strip():
        return NormalizedField(
            raw=raw_str, normalized='', field_type='phone',
            changed=bool(raw_str), warnings=['空の電話番号'],
        )

    result = zen_to_han(raw_str).strip()

    # 各種ハイフン類を統一（全角ハイフン、長音符、マイナス記号など）
    result = re.sub('[\u2010-\u2015\u2212\uFF0D\u30FC\u2500]', '-', result)
    # スペース除去
    result = result.replace(' ', '')

    warnings: list[str] = []
    return NormalizedField(
        raw=raw_str, normalized=result, field_type='phone',
        changed=result != raw_str, warnings=warnings,
    )


def normalize_date(raw: str) -> NormalizedField:
    """日付正規化。和暦→西暦変換、YYYY-MM-DD形式に統一。

    wareki_convert.py をラップ。
    """
    raw_str = _safe_str(raw)
    if not raw_str.strip():
        return NormalizedField(
            raw=raw_str, normalized='', field_type='date',
            changed=bool(raw_str), warnings=['空の日付'],
        )

    text = zen_to_han(raw_str).strip()

    # まず wareki_to_iso を試行
    iso = wareki_to_iso(text)
    if iso is not None:
        return NormalizedField(
            raw=raw_str, normalized=iso, field_type='date',
            changed=iso != raw_str, warnings=[],
        )

    # ISO 8601 形式チェック（YYYY-MM-DD）
    iso_match = re.match(r'^(\d{4})-(\d{1,2})-(\d{1,2})$', text)
    if iso_match:
        y, m, d = iso_match.groups()
        normalized = f'{int(y):04d}-{int(m):02d}-{int(d):02d}'
        return NormalizedField(
            raw=raw_str, normalized=normalized, field_type='date',
            changed=normalized != raw_str, warnings=[],
        )

    # YYYY/MM/DD 形式
    slash_match = re.match(r'^(\d{4})/(\d{1,2})/(\d{1,2})$', text)
    if slash_match:
        y, m, d = slash_match.groups()
        normalized = f'{int(y):04d}-{int(m):02d}-{int(d):02d}'
        return NormalizedField(
            raw=raw_str, normalized=normalized, field_type='date',
            changed=True, warnings=[],
        )

    return NormalizedField(
        raw=raw_str, normalized=raw_str, field_type='date',
        changed=False, warnings=['日付形式を認識できませんでした'],
    )


def normalize_permit_number(raw: str) -> NormalizedField:
    """許可番号正規化。permit_parser.py をラップ。"""
    raw_str = _safe_str(raw)
    if not raw_str.strip():
        return NormalizedField(
            raw=raw_str, normalized='', field_type='permit_number',
            changed=bool(raw_str), warnings=['空の許可番号'],
        )

    result = parse_permit_number(raw_str)

    if result.parse_success:
        # 正規化された許可番号文字列を構築
        normalized = (
            f'{result.permit_authority_name} 許可'
            f'({result.permit_category}-{result.permit_year})'
            f'第{result.contractor_number}号'
        )
        return NormalizedField(
            raw=raw_str, normalized=normalized,
            field_type='permit_number',
            changed=normalized != raw_str,
            warnings=result.parse_warnings,
        )

    return NormalizedField(
        raw=raw_str, normalized=raw_str, field_type='permit_number',
        changed=False, warnings=result.parse_warnings,
    )


def normalize_currency(raw: str) -> NormalizedField:
    """金額正規化。カンマ除去、全角→半角、「円」除去。整数文字列を返す。"""
    raw_str = _safe_str(raw)
    if not raw_str.strip():
        return NormalizedField(
            raw=raw_str, normalized='', field_type='currency',
            changed=bool(raw_str), warnings=['空の金額'],
        )

    result = zen_to_han(raw_str).strip()
    # カンマ除去
    result = result.replace(',', '')
    # 「円」「\\」「¥」「￥」除去
    result = result.replace('円', '').replace('\\', '').replace('¥', '').replace('\uFFE5', '')
    # 前後スペース再トリム
    result = result.strip()

    warnings: list[str] = []
    # 数値として妥当か確認
    cleaned = result.lstrip('-')
    if cleaned and not cleaned.isdigit():
        warnings.append(f'数値以外の文字が含まれています: "{result}"')

    return NormalizedField(
        raw=raw_str, normalized=result, field_type='currency',
        changed=result != raw_str, warnings=warnings,
    )


def normalize_zipcode(raw: str) -> NormalizedField:
    """郵便番号正規化。〒除去、ハイフン統一、全角→半角。"""
    raw_str = _safe_str(raw)
    if not raw_str.strip():
        return NormalizedField(
            raw=raw_str, normalized='', field_type='zipcode',
            changed=bool(raw_str), warnings=['空の郵便番号'],
        )

    result = zen_to_han(raw_str).strip()
    # 〒記号除去
    result = result.replace('〒', '').strip()
    # 各種ハイフン類を統一
    result = re.sub('[\u2010-\u2015\u2212\uFF0D\u30FC\u2500]', '-', result)
    # スペース除去
    result = result.replace(' ', '')

    # ハイフンなし7桁 → ハイフン挿入
    if re.match(r'^\d{7}$', result):
        result = f'{result[:3]}-{result[3:]}'

    warnings: list[str] = []
    # 形式チェック
    if not re.match(r'^\d{3}-\d{4}$', result):
        warnings.append(f'郵便番号の形式が不正です: "{result}"')

    return NormalizedField(
        raw=raw_str, normalized=result, field_type='zipcode',
        changed=result != raw_str, warnings=warnings,
    )


# ---------------------------------------------------------------------------
# 一括正規化
# ---------------------------------------------------------------------------

# field_type → 正規化関数のマッピング
_NORMALIZER_MAP: dict[str, type] = {
    'company_name': normalize_company_name,
    'address': normalize_address,
    'phone': normalize_phone,
    'date': normalize_date,
    'permit_number': normalize_permit_number,
    'currency': normalize_currency,
    'zipcode': normalize_zipcode,
}


def normalize_all(
    fields: dict[str, str],
    field_types: dict[str, str],
) -> dict[str, NormalizedField]:
    """複数フィールドを一括正規化する。

    Args:
        fields: フィールド名 → 値のマッピング
        field_types: フィールド名 → field_type（"company_name", "date" 等）

    Returns:
        フィールド名 → NormalizedField のマッピング

    未知の field_type が指定された場合は zen_to_han + normalize_whitespace を適用し、
    field_type="unknown" として返す。
    """
    results: dict[str, NormalizedField] = {}
    for name, value in fields.items():
        ft = field_types.get(name, 'unknown')
        normalizer = _NORMALIZER_MAP.get(ft)
        if normalizer is not None:
            results[name] = normalizer(value)
        else:
            # 未知の型: 基本正規化のみ
            raw_str = _safe_str(value)
            normalized = normalize_whitespace(zen_to_han(raw_str))
            results[name] = NormalizedField(
                raw=raw_str, normalized=normalized,
                field_type=ft, changed=normalized != raw_str,
                warnings=[],
            )
    return results
