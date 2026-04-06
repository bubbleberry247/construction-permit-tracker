"""
tests/test_jp_field_pack.py — jp_field_pack ユニットテスト

テスト対象:
  - src/utils/jp_field_pack.py: 日本語フィールド正規化統合パック

実行:
    cd <project_root>
    pytest tests/test_jp_field_pack.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# sys.path 設定 — src/ を参照できるようにする
# ---------------------------------------------------------------------------
_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from utils.jp_field_pack import (  # noqa: E402
    NormalizedField,
    han_to_zen_kana,
    normalize_address,
    normalize_all,
    normalize_company_name,
    normalize_currency,
    normalize_date,
    normalize_permit_number,
    normalize_phone,
    normalize_whitespace,
    normalize_zipcode,
    zen_to_han,
)


# ===========================================================================
# zen_to_han: 全角英数字・記号→半角変換
# ===========================================================================
class TestZenToHan:

    def test_fullwidth_digits(self):
        assert zen_to_han("０１２３４５６７８９") == "0123456789"

    def test_fullwidth_uppercase(self):
        assert zen_to_han("ＡＢＣＤ") == "ABCD"

    def test_fullwidth_lowercase(self):
        assert zen_to_han("ａｂｃｄ") == "abcd"

    def test_fullwidth_symbols(self):
        assert zen_to_han("（）") == "()"

    def test_fullwidth_space(self):
        assert zen_to_han("Ａ　Ｂ") == "A B"

    def test_mixed_zen_han(self):
        """全角と半角が混在するケース"""
        assert zen_to_han("ABＣ123４５") == "ABC12345"

    def test_katakana_not_converted(self):
        """全角カタカナは変換されない"""
        assert zen_to_han("アイウ") == "アイウ"

    def test_empty_string(self):
        assert zen_to_han("") == ""

    def test_none_like_empty(self):
        """空文字列をそのまま返す"""
        assert zen_to_han("") == ""


# ===========================================================================
# han_to_zen_kana: 半角カタカナ→全角カタカナ変換
# ===========================================================================
class TestHanToZenKana:

    def test_basic_katakana(self):
        assert han_to_zen_kana("ｱｲｳ") == "アイウ"

    def test_all_basic_kana(self):
        """基本的な半角カナが全角に変換される"""
        assert han_to_zen_kana("ｶｷｸｹｺ") == "カキクケコ"

    def test_dakuten(self):
        """濁音（半角カナ+濁点）が全角濁音に結合される"""
        assert han_to_zen_kana("ｶﾞｷﾞｸﾞ") == "ガギグ"

    def test_handakuten(self):
        """半濁音（半角カナ+半濁点）が全角半濁音に結合される"""
        assert han_to_zen_kana("ﾊﾟﾋﾟﾌﾟ") == "パピプ"

    def test_long_vowel(self):
        """半角長音符が全角に変換される"""
        assert han_to_zen_kana("ﾗｰﾒﾝ") == "ラーメン"

    def test_mixed_with_ascii(self):
        """ASCII文字は変換されない"""
        assert han_to_zen_kana("ABCｱｲｳ") == "ABCアイウ"

    def test_empty_string(self):
        assert han_to_zen_kana("") == ""

    def test_vu(self):
        """ヴ（ウ+濁点）"""
        assert han_to_zen_kana("ｳﾞ") == "ヴ"


# ===========================================================================
# normalize_whitespace
# ===========================================================================
class TestNormalizeWhitespace:

    def test_fullwidth_space(self):
        assert normalize_whitespace("A\u3000B") == "A B"

    def test_consecutive_spaces(self):
        assert normalize_whitespace("A   B") == "A B"

    def test_leading_trailing(self):
        assert normalize_whitespace("  hello  ") == "hello"

    def test_mixed(self):
        assert normalize_whitespace("  A\u3000\u3000B  C  ") == "A B C"

    def test_empty(self):
        assert normalize_whitespace("") == ""


# ===========================================================================
# normalize_company_name: 会社名正規化
# ===========================================================================
class TestNormalizeCompanyName:

    def test_maru_kabu_to_full(self):
        """㈱ → 株式会社"""
        result = normalize_company_name("㈱ABC建設")
        assert result.normalized == "株式会社ABC建設"
        assert result.changed is True

    def test_maru_yuu_to_full(self):
        """㈲ → 有限会社"""
        result = normalize_company_name("㈲テスト工業")
        assert result.normalized == "有限会社テスト工業"
        assert result.changed is True

    def test_paren_kabu_half_to_full(self):
        """(株) → 株式会社（半角括弧）"""
        result = normalize_company_name("(株)ABC建設")
        assert result.normalized == "株式会社ABC建設"
        assert result.changed is True

    def test_paren_kabu_full_to_full(self):
        """（株） → 株式会社（全角括弧）"""
        result = normalize_company_name("（株）ABC建設")
        assert "株式会社" in result.normalized
        assert result.changed is True

    def test_fullwidth_digits_in_name(self):
        """全角数字が半角に変換される"""
        result = normalize_company_name("第１建設")
        assert result.normalized == "第1建設"
        assert result.changed is True

    def test_trim_whitespace(self):
        """前後空白が除去される"""
        result = normalize_company_name("  株式会社ABC  ")
        assert result.normalized == "株式会社ABC"
        assert result.changed is True

    def test_no_change(self):
        """変換不要な場合"""
        result = normalize_company_name("株式会社ABC建設")
        assert result.normalized == "株式会社ABC建設"
        assert result.changed is False

    def test_field_type(self):
        result = normalize_company_name("テスト")
        assert result.field_type == "company_name"

    def test_raw_preserved(self):
        """raw は原文を保持"""
        original = "㈱ABC建設"
        result = normalize_company_name(original)
        assert result.raw == original

    def test_empty_string(self):
        result = normalize_company_name("")
        assert result.normalized == ""
        assert result.changed is False
        assert len(result.warnings) > 0

    def test_none_input(self):
        result = normalize_company_name(None)  # type: ignore[arg-type]
        assert result.normalized == ""
        assert result.field_type == "company_name"

    def test_warning_on_corp_abbreviation(self):
        """法人格略称変換時に警告が記録される"""
        result = normalize_company_name("㈱テスト")
        assert any("法人格略称" in w for w in result.warnings)


# ===========================================================================
# normalize_address: 住所正規化
# ===========================================================================
class TestNormalizeAddress:

    def test_chome_banchi_go(self):
        """丁目番地号 → ハイフン形式"""
        result = normalize_address("名古屋市中区栄3丁目5番12号")
        assert "3-5-12" in result.normalized

    def test_banchi_go(self):
        """番地号 → ハイフン形式"""
        result = normalize_address("名古屋市中区栄5番12号")
        assert "5-12" in result.normalized

    def test_fullwidth_digits(self):
        """全角数字が半角に変換される"""
        result = normalize_address("名古屋市中区栄３丁目５番１２号")
        assert "3-5-12" in result.normalized

    def test_no_change_when_already_hyphened(self):
        """既にハイフン形式の住所"""
        result = normalize_address("名古屋市中区栄3-5-12")
        assert result.normalized == "名古屋市中区栄3-5-12"

    def test_empty_string(self):
        result = normalize_address("")
        assert result.normalized == ""
        assert len(result.warnings) > 0

    def test_none_input(self):
        result = normalize_address(None)  # type: ignore[arg-type]
        assert result.normalized == ""

    def test_field_type(self):
        result = normalize_address("テスト")
        assert result.field_type == "address"


# ===========================================================================
# normalize_phone: 電話番号正規化
# ===========================================================================
class TestNormalizePhone:

    def test_fullwidth_digits(self):
        """全角数字→半角"""
        result = normalize_phone("０５２−１２３−４５６７")
        assert result.normalized == "052-123-4567"

    def test_various_hyphens(self):
        """各種ハイフン類が統一される"""
        result = normalize_phone("052‐123－4567")
        assert result.normalized == "052-123-4567"

    def test_fullwidth_hyphens(self):
        """全角ハイフン"""
        result = normalize_phone("052ー123ー4567")
        assert result.normalized == "052-123-4567"

    def test_remove_spaces(self):
        """スペースが除去される"""
        result = normalize_phone("052 123 4567")
        assert result.normalized == "052-123-4567" or ' ' not in result.normalized

    def test_already_normalized(self):
        result = normalize_phone("052-123-4567")
        assert result.normalized == "052-123-4567"
        assert result.changed is False

    def test_empty_string(self):
        result = normalize_phone("")
        assert result.normalized == ""

    def test_none_input(self):
        result = normalize_phone(None)  # type: ignore[arg-type]
        assert result.normalized == ""

    def test_field_type(self):
        result = normalize_phone("test")
        assert result.field_type == "phone"


# ===========================================================================
# normalize_date: 日付正規化（和暦→西暦含む）
# ===========================================================================
class TestNormalizeDate:

    def test_reiwa_full(self):
        """令和6年7月24日 → 2024-07-24"""
        result = normalize_date("令和6年7月24日")
        assert result.normalized == "2024-07-24"
        assert result.changed is True

    def test_heisei(self):
        """平成31年4月1日 → 2019-04-01"""
        result = normalize_date("平成31年4月1日")
        assert result.normalized == "2019-04-01"

    def test_showa(self):
        """昭和63年1月7日 → 1988-01-07"""
        result = normalize_date("昭和63年1月7日")
        assert result.normalized == "1988-01-07"

    def test_reiwa_gannen(self):
        """令和元年 = 2019年"""
        result = normalize_date("令和元年5月1日")
        assert result.normalized == "2019-05-01"

    def test_alpha_format(self):
        """R6.7.24 → 2024-07-24"""
        result = normalize_date("R6.7.24")
        assert result.normalized == "2024-07-24"

    def test_iso_passthrough(self):
        """ISO 8601 形式はそのまま正規化"""
        result = normalize_date("2024-07-24")
        assert result.normalized == "2024-07-24"

    def test_slash_format(self):
        """YYYY/MM/DD → YYYY-MM-DD"""
        result = normalize_date("2024/7/24")
        assert result.normalized == "2024-07-24"
        assert result.changed is True

    def test_fullwidth_wareki(self):
        """全角数字の和暦"""
        result = normalize_date("令和６年７月２４日")
        assert result.normalized == "2024-07-24"

    def test_unrecognized_format(self):
        """認識不能な日付"""
        result = normalize_date("いつか")
        assert len(result.warnings) > 0

    def test_empty_string(self):
        result = normalize_date("")
        assert result.normalized == ""

    def test_none_input(self):
        result = normalize_date(None)  # type: ignore[arg-type]
        assert result.normalized == ""

    def test_field_type(self):
        result = normalize_date("test")
        assert result.field_type == "date"


# ===========================================================================
# normalize_permit_number: 許可番号正規化
# ===========================================================================
class TestNormalizePermitNumber:

    def test_standard_format(self):
        """標準的な許可番号が正規化される"""
        result = normalize_permit_number("愛知県知事 許可（特一 6）第57805号")
        assert result.field_type == "permit_number"
        assert "愛知県知事" in result.normalized
        assert "特定" in result.normalized
        assert "57805" in result.normalized
        assert result.changed is True

    def test_parse_success_flag(self):
        """パース成功時の NormalizedField"""
        result = normalize_permit_number("愛知県知事 許可（特一 6）第57805号")
        assert result.changed is True

    def test_parse_failure(self):
        """パース失敗時は raw がそのまま返る"""
        result = normalize_permit_number("これは許可番号ではない")
        assert result.normalized == "これは許可番号ではない"
        assert result.changed is False
        assert len(result.warnings) > 0

    def test_fullwidth_digits(self):
        """全角数字入力も正規化される"""
        result = normalize_permit_number("愛知県知事 許可（特一 ６）第５７８０５号")
        assert "57805" in result.normalized

    def test_empty_string(self):
        result = normalize_permit_number("")
        assert result.normalized == ""

    def test_none_input(self):
        result = normalize_permit_number(None)  # type: ignore[arg-type]
        assert result.normalized == ""


# ===========================================================================
# normalize_currency: 金額正規化
# ===========================================================================
class TestNormalizeCurrency:

    def test_comma_removal(self):
        """カンマが除去される"""
        result = normalize_currency("1,234,567")
        assert result.normalized == "1234567"

    def test_yen_removal(self):
        """「円」が除去される"""
        result = normalize_currency("1234円")
        assert result.normalized == "1234"

    def test_yen_mark_removal(self):
        """¥記号が除去される"""
        result = normalize_currency("¥1,234")
        assert result.normalized == "1234"

    def test_fullwidth_digits(self):
        """全角数字→半角"""
        result = normalize_currency("１，２３４")
        assert result.normalized == "1234"

    def test_combined(self):
        """全角+カンマ+円の複合ケース"""
        result = normalize_currency("￥１，２３４，５６７円")
        assert result.normalized == "1234567"

    def test_negative(self):
        """マイナス金額"""
        result = normalize_currency("-500")
        assert result.normalized == "-500"

    def test_warning_on_non_numeric(self):
        """数値以外の文字が含まれる場合に警告"""
        result = normalize_currency("約1000万")
        assert len(result.warnings) > 0

    def test_empty_string(self):
        result = normalize_currency("")
        assert result.normalized == ""

    def test_none_input(self):
        result = normalize_currency(None)  # type: ignore[arg-type]
        assert result.normalized == ""

    def test_field_type(self):
        result = normalize_currency("100")
        assert result.field_type == "currency"


# ===========================================================================
# normalize_zipcode: 郵便番号正規化
# ===========================================================================
class TestNormalizeZipcode:

    def test_yuubin_mark_removal(self):
        """〒記号が除去される"""
        result = normalize_zipcode("〒460-0008")
        assert result.normalized == "460-0008"

    def test_fullwidth_digits(self):
        """全角数字→半角"""
        result = normalize_zipcode("４６０ー０００８")
        assert result.normalized == "460-0008"

    def test_no_hyphen_7digits(self):
        """ハイフンなし7桁 → ハイフン挿入"""
        result = normalize_zipcode("4600008")
        assert result.normalized == "460-0008"

    def test_already_normalized(self):
        result = normalize_zipcode("460-0008")
        assert result.normalized == "460-0008"
        assert result.changed is False

    def test_warning_on_invalid_format(self):
        """形式不正の場合に警告"""
        result = normalize_zipcode("123")
        assert len(result.warnings) > 0

    def test_empty_string(self):
        result = normalize_zipcode("")
        assert result.normalized == ""

    def test_none_input(self):
        result = normalize_zipcode(None)  # type: ignore[arg-type]
        assert result.normalized == ""

    def test_field_type(self):
        result = normalize_zipcode("test")
        assert result.field_type == "zipcode"


# ===========================================================================
# normalize_all: 一括正規化
# ===========================================================================
class TestNormalizeAll:

    def test_multiple_fields(self):
        """複数フィールドが一括正規化される"""
        fields = {
            "name": "㈱テスト建設",
            "phone": "０５２−１２３−４５６７",
            "date": "令和6年7月24日",
        }
        field_types = {
            "name": "company_name",
            "phone": "phone",
            "date": "date",
        }
        results = normalize_all(fields, field_types)

        assert "name" in results
        assert "phone" in results
        assert "date" in results

        assert "株式会社" in results["name"].normalized
        assert results["phone"].normalized == "052-123-4567"
        assert results["date"].normalized == "2024-07-24"

    def test_unknown_field_type(self):
        """未知の field_type は基本正規化のみ適用"""
        fields = {"memo": "ＡＢＣ　メモ"}
        field_types = {"memo": "memo"}
        results = normalize_all(fields, field_types)

        assert results["memo"].field_type == "memo"
        assert results["memo"].normalized == "ABC メモ"

    def test_missing_field_type(self):
        """field_types に含まれないフィールドは unknown として処理"""
        fields = {"extra": "テスト"}
        field_types = {}
        results = normalize_all(fields, field_types)

        assert results["extra"].field_type == "unknown"

    def test_empty_fields(self):
        """空の fields"""
        results = normalize_all({}, {})
        assert results == {}

    def test_all_field_types(self):
        """全 field_type を一括処理"""
        fields = {
            "f1": "㈱テスト",
            "f2": "名古屋市中区栄3丁目5番12号",
            "f3": "052-123-4567",
            "f4": "令和6年7月24日",
            "f5": "愛知県知事 許可（特一 6）第57805号",
            "f6": "1,234円",
            "f7": "〒460-0008",
        }
        field_types = {
            "f1": "company_name",
            "f2": "address",
            "f3": "phone",
            "f4": "date",
            "f5": "permit_number",
            "f6": "currency",
            "f7": "zipcode",
        }
        results = normalize_all(fields, field_types)
        assert len(results) == 7
        for key in fields:
            assert isinstance(results[key], NormalizedField)


# ===========================================================================
# NormalizedField: データクラスの振る舞い
# ===========================================================================
class TestNormalizedField:

    def test_raw_normalized_changed_correct(self):
        """raw/normalized/changed が正しく設定される"""
        result = normalize_company_name("㈱テスト")
        assert result.raw == "㈱テスト"
        assert result.normalized != result.raw
        assert result.changed is True

    def test_no_change_case(self):
        """変更なしの場合"""
        result = normalize_phone("052-123-4567")
        assert result.raw == "052-123-4567"
        assert result.normalized == "052-123-4567"
        assert result.changed is False

    def test_warnings_is_list(self):
        result = normalize_company_name("テスト")
        assert isinstance(result.warnings, list)

    def test_field_type_preserved(self):
        result = normalize_currency("100")
        assert result.field_type == "currency"
