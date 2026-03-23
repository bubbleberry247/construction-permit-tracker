"""
tests/test_core.py — 建設業許可証管理システム コアロジック ユニットテスト

テスト対象:
  - src/utils/wareki_convert.py  : wareki_to_date
  - src/utils/permit_parser.py   : parse_permit_number
  - src/utils/trade_master.py    : normalize_trade_list
  - src/ocr_permit.py            : normalize_authority_name, estimate_confidence, match_company
  - src/register_sheets.py       : determine_current_status, _is_blank
  - src/mlit_confirm.py          : _a1（A1記法変換の回帰テスト）

実行:
    cd <project_root>
    pytest tests/test_core.py -v
"""
from __future__ import annotations

import csv
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# sys.path 設定 — src/ を参照できるようにする
# ---------------------------------------------------------------------------
_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from utils.wareki_convert import wareki_to_date, wareki_to_iso  # noqa: E402
from utils.permit_parser import parse_permit_number  # noqa: E402
from utils.trade_master import normalize_trade_list, TRADE_CATEGORIES  # noqa: E402
from ocr_permit import normalize_authority_name, estimate_confidence, match_company  # noqa: E402
from ocr_permit import _PREFECTURE_SUFFIX, is_valid_authority_name  # noqa: E402
from register_sheets import _is_blank, determine_current_status  # noqa: E402
from mlit_confirm import _a1  # noqa: E402


# ===========================================================================
# wareki_to_date
# ===========================================================================
class TestWarekiToDate:

    # --- 正常系: 漢字元号（年月日）---
    def test_reiwa_full(self):
        assert wareki_to_date("令和6年7月24日") == date(2024, 7, 24)

    def test_reiwa_no_day(self):
        """日省略 → 1日補完"""
        assert wareki_to_date("令和6年7月") == date(2024, 7, 1)

    def test_reiwa_gannen(self):
        """令和元年 = 2019年"""
        assert wareki_to_date("令和元年5月1日") == date(2019, 5, 1)

    def test_reiwa_gannen_no_day(self):
        assert wareki_to_date("令和元年4月") == date(2019, 4, 1)

    def test_heisei_full(self):
        assert wareki_to_date("平成31年4月1日") == date(2019, 4, 1)

    def test_heisei_gannen(self):
        """平成元年 = 1989年"""
        assert wareki_to_date("平成元年4月1日") == date(1989, 4, 1)

    def test_showa_full(self):
        assert wareki_to_date("昭和63年1月7日") == date(1988, 1, 7)

    def test_showa_gannen(self):
        """昭和元年 = 1926年"""
        assert wareki_to_date("昭和元年1月1日") == date(1926, 1, 1)

    # --- 正常系: アルファベット略記 ---
    def test_alpha_reiwa_full(self):
        assert wareki_to_date("R6.7.24") == date(2024, 7, 24)

    def test_alpha_heisei_full(self):
        assert wareki_to_date("H31.4.1") == date(2019, 4, 1)

    def test_alpha_showa_full(self):
        assert wareki_to_date("S63.1.1") == date(1988, 1, 1)

    def test_alpha_no_day(self):
        """日省略 → 1日補完"""
        assert wareki_to_date("R6.7") == date(2024, 7, 1)

    def test_alpha_hyphen_separator(self):
        assert wareki_to_date("R6-7-24") == date(2024, 7, 24)

    def test_alpha_slash_separator(self):
        assert wareki_to_date("R6/7/24") == date(2024, 7, 24)

    # --- 異常系 ---
    def test_none_input_returns_none(self):
        assert wareki_to_date(None) is None  # type: ignore[arg-type]

    def test_empty_string_returns_none(self):
        assert wareki_to_date("") is None

    def test_whitespace_only_returns_none(self):
        assert wareki_to_date("   ") is None

    def test_invalid_string_returns_none(self):
        assert wareki_to_date("不正な文字列") is None

    def test_western_year_string_returns_none(self):
        """ISO 8601 形式はサポートしない"""
        assert wareki_to_date("2024-07-24") is None

    def test_integer_input_returns_none(self):
        assert wareki_to_date(20240724) is None  # type: ignore[arg-type]

    # --- 境界値 ---
    def test_reiwa_1_as_number(self):
        """「令和1年」は数値1として処理される（元年と別扱い）"""
        assert wareki_to_date("令和1年5月1日") == date(2019, 5, 1)

    def test_showa_64_boundary(self):
        """昭和64年1月7日（昭和最終日）= 1989年1月7日"""
        assert wareki_to_date("昭和64年1月7日") == date(1989, 1, 7)


class TestWarekiToIso:
    def test_basic(self):
        assert wareki_to_iso("令和6年7月24日") == "2024-07-24"

    def test_invalid_returns_none(self):
        assert wareki_to_iso("invalid") is None

    def test_none_returns_none(self):
        assert wareki_to_iso(None) is None  # type: ignore[arg-type]


# ===========================================================================
# parse_permit_number
# ===========================================================================
class TestParsePermitNumber:

    # --- 正常系 ---
    def test_aichi_tokutei_standard(self):
        """代表的な愛知県知事・特定許可のパース"""
        r = parse_permit_number("愛知県知事 許可（特一 6）第57805号")
        assert r.parse_success is True
        assert r.permit_authority_name == "愛知県知事"
        assert r.permit_authority_type == "知事"
        assert r.permit_category == "特定"
        assert r.permit_year == 6
        assert r.contractor_number == "57805"
        assert r.permit_number_full == "愛知県知事 許可（特一 6）第57805号"

    def test_aichi_ippan(self):
        r = parse_permit_number("愛知県知事 許可（般一 5）第12345号")
        assert r.parse_success is True
        assert r.permit_category == "一般"
        assert r.permit_year == 5

    def test_minister_tokutei(self):
        r = parse_permit_number("国土交通大臣 許可（特一 6）第99999号")
        assert r.parse_success is True
        assert r.permit_authority_type == "大臣"
        assert r.permit_category == "特定"

    def test_minister_ippan_hyphen(self):
        """「般-6」パターン"""
        r = parse_permit_number("国土交通大臣 許可（般-6）第12345号")
        assert r.parse_success is True
        assert r.permit_category == "一般"

    def test_fullwidth_digits_normalized(self):
        """全角数字が正規化される"""
        r = parse_permit_number("愛知県知事 許可（特一 ６）第５７８０５号")
        assert r.parse_success is True
        assert r.permit_year == 6
        assert r.contractor_number == "57805"

    def test_no_space_before_permit(self):
        """スペースなしの表記"""
        r = parse_permit_number("愛知県知事許可(特-5)第57805号")
        assert r.parse_success is True
        assert r.permit_category == "特定"
        assert r.permit_year == 5

    def test_category_toku_short(self):
        """「特」のみ → 「特定」"""
        r = parse_permit_number("愛知県知事 許可（特-6）第57805号")
        assert r.parse_success is True
        assert r.permit_category == "特定"

    def test_category_ippan_single_char(self):
        """「一」のみ → 「一般」"""
        r = parse_permit_number("愛知県知事 許可（一-6）第57805号")
        assert r.parse_success is True
        assert r.permit_category == "一般"

    def test_permit_number_full_preserved(self):
        """permit_number_full は入力原文そのまま"""
        text = "愛知県知事 許可（特一 6）第57805号"
        r = parse_permit_number(text)
        assert r.permit_number_full == text

    # --- 異常系 ---
    def test_empty_string_fails(self):
        r = parse_permit_number("")
        assert r.parse_success is False
        assert len(r.parse_warnings) > 0
        assert "空" in r.parse_warnings[0]

    def test_whitespace_only_fails(self):
        r = parse_permit_number("   ")
        assert r.parse_success is False

    def test_none_input_fails(self):
        r = parse_permit_number(None)  # type: ignore[arg-type]
        assert r.parse_success is False

    def test_unmatched_string_fails(self):
        r = parse_permit_number("これは許可番号ではありません")
        assert r.parse_success is False
        assert r.permit_number_full == "これは許可番号ではありません"

    # --- 警告フィールド ---
    def test_warnings_on_category_abbreviation(self):
        """「特一」→「特定」変換時に警告が記録される"""
        r = parse_permit_number("愛知県知事 許可（特一 6）第57805号")
        assert isinstance(r.parse_warnings, list)
        assert any("特一" in w for w in r.parse_warnings)


# ===========================================================================
# normalize_trade_list
# ===========================================================================
class TestNormalizeTradeList:

    # --- 正常系: 完全一致 ---
    def test_exact_match_all_three(self):
        raw = ["土木工事業", "建築工事業", "電気工事業"]
        normalized, failed = normalize_trade_list(raw)
        assert normalized == ["土木工事業", "建築工事業", "電気工事業"]
        assert failed == []

    # --- エイリアス変換 ---
    def test_alias_tobi_dokkou(self):
        normalized, failed = normalize_trade_list(["とび土工工事業"])
        assert normalized == ["とび・土工工事業"]
        assert failed == []

    def test_alias_tobi_dokkou_full(self):
        normalized, failed = normalize_trade_list(["とび・土工・コンクリート工事業"])
        assert normalized == ["とび・土工工事業"]
        assert failed == []

    def test_alias_tile_rengaburo(self):
        normalized, failed = normalize_trade_list(["タイル・レンガ・ブロック工事業"])
        assert normalized == ["タイル・れんが・ブロック工事業"]
        assert failed == []

    def test_alias_naiso_shiagege(self):
        normalized, failed = normalize_trade_list(["内装仕上げ工事業"])
        assert normalized == ["内装仕上工事業"]
        assert failed == []

    def test_alias_shunsetsu_kanji(self):
        normalized, failed = normalize_trade_list(["浚渫工事業"])
        assert normalized == ["しゅんせつ工事業"]
        assert failed == []

    # --- 部分一致 ---
    def test_partial_match_denki(self):
        """「電気工事」→「電気工事業」"""
        normalized, failed = normalize_trade_list(["電気工事"])
        assert normalized == ["電気工事業"]
        assert failed == []

    # --- 失敗 ---
    def test_unknown_returns_failed(self):
        normalized, failed = normalize_trade_list(["存在しない業種"])
        assert normalized == []
        assert failed == ["存在しない業種"]

    # --- 混在ケース ---
    def test_mixed_valid_and_invalid(self):
        raw = ["土木工事業", "XXX未知業種", "電気工事業"]
        normalized, failed = normalize_trade_list(raw)
        assert "土木工事業" in normalized
        assert "電気工事業" in normalized
        assert "XXX未知業種" in failed

    # --- 境界値 ---
    def test_empty_list(self):
        normalized, failed = normalize_trade_list([])
        assert normalized == []
        assert failed == []

    def test_single_item_exact(self):
        normalized, failed = normalize_trade_list(["解体工事業"])
        assert normalized == ["解体工事業"]
        assert failed == []

    def test_all_29_valid_trades(self):
        """29業種すべてが正規化成功する"""
        normalized, failed = normalize_trade_list(TRADE_CATEGORIES)
        assert failed == []
        assert len(normalized) == 29

    def test_duplicate_entries_preserved(self):
        """同一業種が2件あれば2件とも正規化リストに含まれる"""
        normalized, failed = normalize_trade_list(["電気工事業", "電気工事業"])
        assert normalized == ["電気工事業", "電気工事業"]
        assert failed == []


# ===========================================================================
# normalize_authority_name
# ===========================================================================
class TestNormalizeAuthorityName:

    def test_aichi_short_to_full(self):
        """「愛知知事」→「愛知県知事」"""
        assert normalize_authority_name("愛知知事") == "愛知県知事"

    def test_aichi_full_unchanged(self):
        assert normalize_authority_name("愛知県知事") == "愛知県知事"

    def test_tokyo_to_metropolis(self):
        """「東京知事」→「東京都知事」"""
        assert normalize_authority_name("東京知事") == "東京都知事"

    def test_osaka_to_fu(self):
        """「大阪知事」→「大阪府知事」"""
        assert normalize_authority_name("大阪知事") == "大阪府知事"

    def test_kyoto_to_fu(self):
        assert normalize_authority_name("京都知事") == "京都府知事"

    def test_hokkaido_unchanged(self):
        assert normalize_authority_name("北海道知事") == "北海道知事"

    def test_minister_normalized(self):
        assert normalize_authority_name("国土交通大臣") == "国土交通大臣"

    def test_minister_variant(self):
        """「大臣」を含む文字列は「国土交通大臣」に正規化"""
        assert normalize_authority_name("国交省大臣") == "国土交通大臣"

    def test_empty_string(self):
        assert normalize_authority_name("") == ""

    def test_nfkc_fullwidth_space(self):
        """全角スペースが除去される"""
        assert normalize_authority_name("愛知県　知事") == "愛知県知事"

    def test_all_47_prefectures(self):
        """47都道府県の短縮形がすべて正しく展開される"""
        for short, full_pref in _PREFECTURE_SUFFIX.items():
            result = normalize_authority_name(f"{short}知事")
            assert result == f"{full_pref}知事", f"Failed for: {short}"


# ===========================================================================
# is_valid_authority_name  (upsert キー安全性の回帰テスト)
# ===========================================================================
class TestIsValidAuthorityName:
    """normalize_authority_name の結果が既知の正式表記かを検証する。
    OCR誤字で正規化できなかった場合にTIER3_AUTH_NAME_UNKNOWNが発火する設計の前提。"""

    def test_known_prefecture_valid(self):
        assert is_valid_authority_name("愛知県知事") is True

    def test_all_47_prefectures_valid(self):
        for short, full in _PREFECTURE_SUFFIX.items():
            assert is_valid_authority_name(f"{full}知事") is True, f"Failed: {full}知事"

    def test_minister_valid(self):
        assert is_valid_authority_name("国土交通大臣") is True

    def test_ocr_typo_invalid(self):
        """OCR誤字（「懸」vs「県」）は invalid → TIER3_AUTH_NAME_UNKNOWN 発火が期待される"""
        assert is_valid_authority_name("愛知懸知事") is False

    def test_short_form_invalid(self):
        """短縮形（正規化前の値）は invalid"""
        assert is_valid_authority_name("愛知知事") is False

    def test_empty_string_invalid(self):
        assert is_valid_authority_name("") is False

    def test_garbage_invalid(self):
        assert is_valid_authority_name("不明な行政庁") is False


# ===========================================================================
# estimate_confidence
# ===========================================================================
class TestEstimateConfidence:

    def test_empty_pages_returns_zero(self):
        assert estimate_confidence([]) == 0.0

    def test_low_quality_below_100_chars(self):
        """平均50文字 → 0.2"""
        assert estimate_confidence(["a" * 50]) == pytest.approx(0.2)

    def test_medium_quality_100_to_299_chars(self):
        """平均150文字 → 0.45"""
        assert estimate_confidence(["a" * 150]) == pytest.approx(0.45)

    def test_high_quality_300_plus_chars(self):
        """平均400文字 → 0.8"""
        assert estimate_confidence(["a" * 400]) == pytest.approx(0.8)

    def test_boundary_exactly_100_chars(self):
        """ちょうど100文字 → 0.45（100 未満でない）"""
        assert estimate_confidence(["c" * 100]) == pytest.approx(0.45)

    def test_boundary_exactly_300_chars(self):
        """ちょうど300文字 → 0.8（300 未満でない）"""
        assert estimate_confidence(["d" * 300]) == pytest.approx(0.8)

    def test_multi_page_average(self):
        """平均 (50 + 400) / 2 = 225 → 0.45"""
        assert estimate_confidence(["a" * 50, "a" * 400]) == pytest.approx(0.45)

    def test_single_empty_page(self):
        """1ページのみで空文字 → 0.2"""
        assert estimate_confidence([""]) == pytest.approx(0.2)

    def test_multi_page_all_high(self):
        """複数ページすべて高品質 → 0.8"""
        assert estimate_confidence(["a" * 400, "b" * 400]) == pytest.approx(0.8)


# ===========================================================================
# match_company
# ===========================================================================
class TestMatchCompany:

    def _make_csv(self, tmp_path: Path, rows: list[dict]) -> Path:
        """テスト用 companies.csv を tmp_path に作成して返す。"""
        csv_path = tmp_path / "companies.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["company_id", "company_name_normalized"])
            writer.writeheader()
            writer.writerows(rows)
        return csv_path

    def test_exact_match_returns_exact(self, tmp_path: Path):
        csv_path = self._make_csv(tmp_path, [
            {"company_id": "C001", "company_name_normalized": "株式会社ABC建設"},
        ])
        cid, mtype = match_company("株式会社ABC建設", csv_path)
        assert cid == "C001"
        assert mtype == "exact"

    def test_partial_match_is_ambiguous(self, tmp_path: Path):
        """部分一致（input が CSV の name に含まれる）→ ambiguous"""
        csv_path = self._make_csv(tmp_path, [
            {"company_id": "C002", "company_name_normalized": "株式会社ABC建設工業"},
        ])
        cid, mtype = match_company("ABC建設", csv_path)
        assert cid == "C002"
        assert mtype == "ambiguous"

    def test_reverse_partial_match_is_ambiguous(self, tmp_path: Path):
        """CSV の name が input に含まれる（逆向き部分一致）→ ambiguous"""
        csv_path = self._make_csv(tmp_path, [
            {"company_id": "C020", "company_name_normalized": "東海インプル建設"},
        ])
        cid, mtype = match_company("東海インプル建設株式会社", csv_path)
        assert cid == "C020"
        assert mtype == "ambiguous"

    def test_multiple_exact_is_ambiguous(self, tmp_path: Path):
        """完全一致が複数 → ambiguous"""
        csv_path = self._make_csv(tmp_path, [
            {"company_id": "C001", "company_name_normalized": "ABC建設"},
            {"company_id": "C002", "company_name_normalized": "ABC建設"},
        ])
        _, mtype = match_company("ABC建設", csv_path)
        assert mtype == "ambiguous"

    def test_not_found(self, tmp_path: Path):
        csv_path = self._make_csv(tmp_path, [
            {"company_id": "C001", "company_name_normalized": "全然関係ない会社"},
        ])
        cid, mtype = match_company("ABC建設", csv_path)
        assert cid == ""
        assert mtype == "not_found"

    def test_csv_not_exists_returns_not_found(self, tmp_path: Path):
        missing = tmp_path / "nonexistent.csv"
        cid, mtype = match_company("どんな会社", missing)
        assert cid == ""
        assert mtype == "not_found"

    def test_empty_csv_returns_not_found(self, tmp_path: Path):
        """ヘッダ行のみ（データなし）"""
        csv_path = self._make_csv(tmp_path, [])
        cid, mtype = match_company("株式会社ABC建設", csv_path)
        assert cid == ""
        assert mtype == "not_found"


# ===========================================================================
# _is_blank
# ===========================================================================
class TestIsBlank:

    def test_none_is_blank(self):
        assert _is_blank(None) is True

    def test_empty_string_is_blank(self):
        assert _is_blank("") is True

    def test_whitespace_is_blank(self):
        assert _is_blank("   ") is True

    def test_nan_string_is_blank(self):
        assert _is_blank("nan") is True

    def test_none_string_is_blank(self):
        assert _is_blank("None") is True

    def test_null_string_is_blank(self):
        assert _is_blank("null") is True

    def test_valid_string_is_not_blank(self):
        assert _is_blank("ABC") is False

    def test_zero_int_is_not_blank(self):
        """整数 0 → str(0) = "0" は空でないので False"""
        assert _is_blank(0) is False

    def test_nonzero_int_is_not_blank(self):
        assert _is_blank(42) is False

    def test_date_string_is_not_blank(self):
        assert _is_blank("2024-07-24") is False


# ===========================================================================
# determine_current_status
# ===========================================================================
class TestDetermineCurrentStatus:
    """
    date.today() に依存するため、timedelta で相対日付を使用。
    固定日付を必要とするケースは直接 isoformat 文字列で指定する。
    """

    def _row(self, **kwargs) -> dict:
        """最小限の有効行を生成し、kwargs で上書き"""
        today = date.today()
        base = {
            "company_id": "C001",
            "contractor_number": "12345",
            "trade_categories": "建築工事業",
            "expiry_date": (today + timedelta(days=200)).isoformat(),
            "renewal_deadline_date": (today + timedelta(days=170)).isoformat(),
            "renewal_application_date": "",
            "evidence_renewal_application": "false",
            "evidence_file_path": "",
        }
        base.update(kwargs)
        return base

    def test_valid(self):
        """有効期限まで200日 → VALID"""
        assert determine_current_status(self._row()) == "VALID"

    def test_expiring_within_90_days(self):
        """有効期限まで60日 → EXPIRING"""
        today = date.today()
        row = self._row(
            expiry_date=(today + timedelta(days=60)).isoformat(),
            renewal_deadline_date=(today + timedelta(days=30)).isoformat(),
        )
        assert determine_current_status(row) == "EXPIRING"

    def test_expiring_boundary_90days(self):
        """有効期限ちょうど90日 → EXPIRING（<=90 の条件）"""
        today = date.today()
        row = self._row(
            expiry_date=(today + timedelta(days=90)).isoformat(),
            renewal_deadline_date=(today + timedelta(days=60)).isoformat(),
        )
        assert determine_current_status(row) == "EXPIRING"

    def test_valid_boundary_91days(self):
        """有効期限91日 → VALID（>90 の条件）"""
        today = date.today()
        row = self._row(
            expiry_date=(today + timedelta(days=91)).isoformat(),
            renewal_deadline_date=(today + timedelta(days=61)).isoformat(),
        )
        assert determine_current_status(row) == "VALID"

    def test_renewal_overdue(self):
        """更新期限切れ（有効期限はまだ先）→ RENEWAL_OVERDUE"""
        today = date.today()
        row = self._row(
            expiry_date=(today + timedelta(days=20)).isoformat(),
            renewal_deadline_date=(today - timedelta(days=5)).isoformat(),
        )
        assert determine_current_status(row) == "RENEWAL_OVERDUE"

    def test_expired(self):
        """有効期限切れ（更新申請なし）→ EXPIRED"""
        today = date.today()
        row = self._row(
            expiry_date=(today - timedelta(days=10)).isoformat(),
            renewal_deadline_date=(today - timedelta(days=40)).isoformat(),
        )
        assert determine_current_status(row) == "EXPIRED"

    def test_renewal_in_progress(self):
        """期限切れ後に更新申請中 → RENEWAL_IN_PROGRESS"""
        today = date.today()
        expiry = today - timedelta(days=5)
        row = self._row(
            expiry_date=expiry.isoformat(),
            renewal_deadline_date=(expiry - timedelta(days=30)).isoformat(),
            renewal_application_date=(expiry - timedelta(days=10)).isoformat(),
            evidence_renewal_application="true",
            evidence_file_path="/path/to/receipt.pdf",
        )
        assert determine_current_status(row) == "RENEWAL_IN_PROGRESS"

    def test_renewal_in_progress_requires_evidence_file(self):
        """受付票ファイルがない → RENEWAL_IN_PROGRESS にならず EXPIRED"""
        today = date.today()
        expiry = today - timedelta(days=5)
        row = self._row(
            expiry_date=expiry.isoformat(),
            renewal_deadline_date=(expiry - timedelta(days=30)).isoformat(),
            renewal_application_date=(expiry - timedelta(days=10)).isoformat(),
            evidence_renewal_application="true",
            evidence_file_path="",  # ← 証跡なし
        )
        assert determine_current_status(row) == "EXPIRED"

    def test_deficient_missing_expiry(self):
        """expiry_date が空 → DEFICIENT"""
        assert determine_current_status(self._row(expiry_date="")) == "DEFICIENT"

    def test_deficient_missing_contractor_number(self):
        """contractor_number が空 → DEFICIENT"""
        assert determine_current_status(self._row(contractor_number="")) == "DEFICIENT"

    def test_deficient_missing_trade_categories(self):
        """trade_categories が空 → DEFICIENT"""
        assert determine_current_status(self._row(trade_categories="")) == "DEFICIENT"

    def test_requires_action_no_company_id(self):
        """company_id が空 → REQUIRES_ACTION"""
        assert determine_current_status(self._row(company_id="")) == "REQUIRES_ACTION"

    def test_deficient_takes_priority_over_requires_action(self):
        """expiry_date も company_id も空 → DEFICIENT が優先"""
        row = self._row(expiry_date="", company_id="")
        assert determine_current_status(row) == "DEFICIENT"


# ===========================================================================
# _a1 (A1記法変換の回帰テスト)
# ===========================================================================
class TestA1Notation:
    """
    gspread の batch_update は A1 記法のみサポート（R1C1 は INVALID_ARGUMENT）。
    _a1 が正しく A1 記法を返すことを確認する回帰テスト。
    """

    def test_a1_col_a(self):
        assert _a1(1, 1) == "A1"

    def test_a1_col_b(self):
        assert _a1(1, 2) == "B1"

    def test_a1_col_z(self):
        assert _a1(2, 26) == "Z2"

    def test_a1_col_aa(self):
        """27列目は AA"""
        assert _a1(3, 27) == "AA3"

    def test_a1_col_ab(self):
        assert _a1(1, 28) == "AB1"

    def test_mlit_date_col(self):
        """Permits シート mlit_confirmed_date = 18列目"""
        assert _a1(5, 18) == "R5"

    def test_mlit_result_col(self):
        """Permits シート mlit_confirm_result = 19列目"""
        assert _a1(5, 19) == "S5"

    def test_no_r1c1_format(self):
        """R{n}C{n} 形式になっていないことを確認（C2 など正しいA1形式）"""
        result = _a1(2, 3)
        assert result == "C2"
        # R1C1 形式（例: "R2C3"）ではないことを確認
        assert not (result.startswith("R") and "C" in result and not result[1:].replace("C", "").isdigit())
