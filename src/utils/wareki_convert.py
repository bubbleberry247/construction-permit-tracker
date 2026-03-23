"""
wareki_convert.py
和暦（令和・平成・昭和）→ 西暦変換。建設業許可証のOCR後処理用。
"""
import re
from datetime import date

# 元号ごとの西暦オフセット（元号の年数を加算して西暦を得る）
# 例: 令和6年 -> 2018 + 6 = 2024
GANNEN_OFFSETS: dict[str, int] = {
    '令和': 2018, 'R': 2018,
    '平成': 1988, 'H': 1988,
    '昭和': 1925, 'S': 1925,
}

# 和暦パターン（漢字元号＋全角・半角数字または「元」）
_KANJI_PATTERN = re.compile(
    r'(令和|平成|昭和)'
    r'(元|\d{1,2})'
    r'年'
    r'(\d{1,2})月'
    r'(?:(\d{1,2})日)?'
)

# アルファベット略記パターン（R6.7.24 / H31.4.1 など）
_ALPHA_PATTERN = re.compile(
    r'([RHS])'
    r'(\d{1,2})'
    r'[.\-/]'
    r'(\d{1,2})'
    r'(?:[.\-/](\d{1,2}))?'
)

# 年のみ・年月のみパターン（漢字元号）
_KANJI_YM_PATTERN = re.compile(
    r'(令和|平成|昭和)'
    r'(元|\d{1,2})'
    r'年'
    r'(?:(\d{1,2})月)?'
)


def _parse_year(era: str, year_str: str) -> int | None:
    """元号と年文字列（数字または「元」）から西暦年を返す。"""
    offset = GANNEN_OFFSETS.get(era)
    if offset is None:
        return None
    if year_str == '元':
        return offset + 1
    try:
        return offset + int(year_str)
    except ValueError:
        return None


def wareki_to_date(text: str) -> date | None:
    """
    和暦文字列をdatetime.dateに変換。失敗時はNone。

    対応フォーマット:
        - 漢字元号: 「令和6年7月24日」「平成31年4月」「令和元年4月1日」
        - アルファベット略記: 「R6.7.24」「H31.4」「S63.1.1」

    日が省略された場合は 1日 を補完する。

    Examples:
        wareki_to_date("令和6年7月24日") -> date(2024, 7, 24)
        wareki_to_date("R6.7.24") -> date(2024, 7, 24)
        wareki_to_date("令和元年4月") -> date(2019, 4, 1)
        wareki_to_date("不正な文字列") -> None
    """
    if not isinstance(text, str):
        return None

    text = text.strip()

    # --- 漢字元号（年月日）---
    m = _KANJI_PATTERN.search(text)
    if m:
        era, year_str, month_str, day_str = m.groups()
        year = _parse_year(era, year_str)
        if year is None:
            return None
        try:
            month = int(month_str)
            day = int(day_str) if day_str else 1
            return date(year, month, day)
        except (ValueError, TypeError):
            return None

    # --- 漢字元号（年月のみ、または年のみ）---
    m = _KANJI_YM_PATTERN.search(text)
    if m:
        era, year_str, month_str = m.groups()
        year = _parse_year(era, year_str)
        if year is None:
            return None
        try:
            month = int(month_str) if month_str else 1
            return date(year, month, 1)
        except (ValueError, TypeError):
            return None

    # --- アルファベット略記 ---
    m = _ALPHA_PATTERN.search(text)
    if m:
        era, year_str, month_str, day_str = m.groups()
        year = _parse_year(era, year_str)
        if year is None:
            return None
        try:
            month = int(month_str)
            day = int(day_str) if day_str else 1
            return date(year, month, day)
        except (ValueError, TypeError):
            return None

    return None


def wareki_to_iso(text: str) -> str | None:
    """
    和暦文字列をISO 8601形式（YYYY-MM-DD）に変換。失敗時はNone。

    Examples:
        wareki_to_iso("令和6年7月24日") -> "2024-07-24"
        wareki_to_iso("R6.7.24") -> "2024-07-24"
    """
    result = wareki_to_date(text)
    return result.isoformat() if result is not None else None
