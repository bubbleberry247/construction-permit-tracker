"""
permit_parser.py
建設業許可証の許可番号文字列をパースして構造化データを返す。
"""
import re
from dataclasses import dataclass, field


@dataclass
class PermitParseResult:
    permit_authority_name: str        # "愛知県知事" / "国土交通大臣"
    permit_authority_type: str        # "知事" / "大臣"
    permit_category: str              # "一般" / "特定"（正規化後）
    permit_year: int                  # 許可年度（例: 6）
    contractor_number: str            # 業者番号（例: "57805"）
    permit_number_full: str           # 入力文字列そのまま
    parse_success: bool
    parse_warnings: list[str] = field(default_factory=list)


# 全角数字→半角数字変換テーブル
_FULLWIDTH_DIGITS = str.maketrans('０１２３４５６７８９', '0123456789')

# 全角かっこ・スペースの正規化
_NORMALIZE_TABLE = str.maketrans({
    '（': '(',
    '）': ')',
    '　': ' ',
})

# 許可区分の表記ゆれ → 正規化後の値
_CATEGORY_MAP: dict[str, str] = {
    '特一': '特定',
    '特定': '特定',
    '特':   '特定',
    '般一': '一般',
    '一般': '一般',
    '一':   '一般',
    '般':   '一般',
}

# 許可番号パターン
# 例: 「愛知県知事 許可（特一 6）第57805号」
# 例: 「国土交通大臣 許可（般-6）第12345号」
# 例: 「愛知県知事許可(特-5)第57805号」
_PERMIT_PATTERN = re.compile(
    r'(.+?)'                    # (1) 許可権者名（最短一致）
    r'\s*許可\s*'               # "許可"（前後スペース許容）
    r'[(（]'                    # 開きかっこ（半角・全角）
    r'\s*'
    r'(特一|般一|特定|一般|特|一|般)'  # (2) 許可区分
    r'[\s\-‐－ー]+'             # 区切り（スペース・ハイフン類）
    r'(\d+)'                    # (3) 許可年度
    r'\s*'
    r'[)）]'                    # 閉じかっこ
    r'\s*第\s*'
    r'(\d+)'                    # (4) 業者番号
    r'\s*号'
)


def _normalize(text: str) -> str:
    """全角かっこ・全角数字・全角スペースを半角に統一する。"""
    text = text.translate(_NORMALIZE_TABLE)
    text = text.translate(_FULLWIDTH_DIGITS)
    return text


def parse_permit_number(text: str) -> PermitParseResult:
    """
    建設業許可番号文字列をパースして PermitParseResult を返す。

    パース失敗時は例外を上げず parse_success=False で返す。
    部分的に取得できたフィールドは設定される。

    Examples:
        parse_permit_number("愛知県知事 許可（特一 6）第57805号")
        -> PermitParseResult(
               permit_authority_name="愛知県知事",
               permit_authority_type="知事",
               permit_category="特定",
               permit_year=6,
               contractor_number="57805",
               permit_number_full="愛知県知事 許可（特一 6）第57805号",
               parse_success=True,
               parse_warnings=[],
           )
    """
    warnings: list[str] = []
    original = text

    if not isinstance(text, str) or not text.strip():
        return PermitParseResult(
            permit_authority_name='',
            permit_authority_type='',
            permit_category='',
            permit_year=0,
            contractor_number='',
            permit_number_full=original if isinstance(text, str) else '',
            parse_success=False,
            parse_warnings=['入力が空または無効です'],
        )

    normalized = _normalize(text.strip())

    m = _PERMIT_PATTERN.search(normalized)
    if not m:
        return PermitParseResult(
            permit_authority_name='',
            permit_authority_type='',
            permit_category='',
            permit_year=0,
            contractor_number='',
            permit_number_full=original,
            parse_success=False,
            parse_warnings=['許可番号のパターンに一致しませんでした'],
        )

    raw_authority, raw_category, raw_year, raw_number = m.groups()

    # --- 許可権者名・許可権者種別 ---
    authority_name = raw_authority.strip()
    authority_type = '大臣' if '大臣' in authority_name else '知事'

    # --- 許可区分の正規化 ---
    category_raw = raw_category.strip()
    category = _CATEGORY_MAP.get(category_raw)
    if category is None:
        # フォールバック: 「特」を含むか否かで判定
        category = '特定' if '特' in category_raw else '一般'
        warnings.append(f'許可区分 "{category_raw}" をゆれ補正で "{category}" に変換しました')
    elif category_raw not in ('特定', '一般'):
        warnings.append(f'許可区分 "{category_raw}" を "{category}" に正規化しました')

    # --- 許可年度 ---
    try:
        permit_year = int(raw_year)
    except ValueError:
        return PermitParseResult(
            permit_authority_name=authority_name,
            permit_authority_type=authority_type,
            permit_category=category or '',
            permit_year=0,
            contractor_number='',
            permit_number_full=original,
            parse_success=False,
            parse_warnings=warnings + [f'許可年度のパースに失敗しました: {raw_year}'],
        )

    # --- 業者番号 ---
    contractor_number = raw_number.strip()

    return PermitParseResult(
        permit_authority_name=authority_name,
        permit_authority_type=authority_type,
        permit_category=category,
        permit_year=permit_year,
        contractor_number=contractor_number,
        permit_number_full=original,
        parse_success=True,
        parse_warnings=warnings,
    )
