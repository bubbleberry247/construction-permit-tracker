"""
tests/test_core.py — 建設業許可証管理システム コアロジック ユニットテスト

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

# src/ を sys.path に追加
_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# ---------------------------------------------------------------------------
# wareki_convert
# ---------------------------------------------------------------------------
from utils.wareki_convert import wareki_to_date, wareki_to_iso  # noqa: E402


class TestWarekiToDate:
    def test_reiwa_full(self):
        assert wareki_to_date("令和6年7月24日") == date(2024, 7, 24)

    def test_reiwa_no_day(self):
        assert wareki_to_date("令和6年7月") == date(2024, 7, 1)

    def test_reiwa_gannen(self):
        assert wareki_to_date("令和元年5月1日") == date(2019, 5, 1)

    def test_heisei_full(self):
        assert wareki_to_date("平成31年4月1日") == date(2019, 4, 1)

    def test_showa_full(self):
        assert wareki_to_date("昭和63年1月7日") == date(1988, 1, 7)

    def test_alpha_reiwa(self):
        assert wareki_to_date("R6.7.24") == date(2024, 7, 24)

    def test_alpha_heisei(self):
        assert wareki_to_date("H31.4.1") == date(2019, 4, 1)

    def test_alpha_no_day(self):
        assert wareki_to_date("R6.7") == date(2024, 7, 1)

    def test_invalid_returns_none(self):
        assert wareki_to_date("不正な文字列") is None

    def test_empty_returns_none(self):
        assert wareki_to_date("") is None

    def test_none_returns_none(self):
        assert wareki_to_date(None) is None  # type: ignore[arg-type]


class TestWarekiToIso:
    def test_basic(self):
        assert wareki_to_iso("令和6年7月24日") == "2024-07-24"

    def test_invalid_returns_none(self):
        assert wareki_to_iso("invalid") is None


# ---------------------------------------------------------------------------
# permit_parser
# ---------------------------------------------------------------------------
from utils.permit_parser import parse_permit_number  # noqa: E402


class TestParsePermitNumber:
    def test_standard_tokutei(self):
        r = parse_permit_number("愛知県知事 許可（特一 6）第57805号")
        assert r.parse_success is True
        assert r.permit_authority_name == "愛知県知事"
        assert r.permit_authority_type == "知事"
        assert r.permit_category == "特定"
        assert r.permit_year == 6
        assert r.contractor_number == "57805"

    def test_standard_ippan(self):
        r = parse_permit_number("愛知県知事 許可（般一 5）第12345号")
        assert r.parse_success is True
        assert r.permit_category == "一般"

    def test_daijin(self):
        r = parse_permit_number("国土交通大臣 許可（特一 6）第99999号")
        assert r.parse_success is True
        assert r.permit_authority_type == "大臣"
        assert r.permit_category == "特定"

    def test_fullwidth_digits(self):
        r = parse_permit_number("愛知県知事 許可（特一 ６）第５７８０５号")
        assert r.parse_success is True
        assert r.permit_year == 6
        assert r.contractor_number == "57805"

    def test_abbreviation_toku(self):
        r = parse_permit_number("愛知県知事許可(特-5)第57805号")
        assert r.parse_success is True
        assert r.permit_category == "特定"

    def test_empty_string(self):
        r = parse_permit_number("")
        assert r.parse_success is False
        assert "空" in r.parse_warnings[0]

    def test_no_match(self):
        r = parse_permit_number("これは許可番号ではありません")
        assert r.parse_success is False
        assert r.permit_number_full == "これは許可番号ではありません"


# ---------------------------------------------------------------------------
# ocr_permit: normalize_authority_name, estimate_confidence, match_company
# ---------------------------------------------------------------------------
from ocr_permit import estimate_confidence, match_company, normalize_authority_name  # noqa: E402


class TestNormalizeAuthorityName:
    def test_aichi_short(self):
        assert normalize_authority_name("愛知知事") == "愛知県知事"

    def test_aichi_full(self):
        assert normalize_authority_name("愛知県知事") == "愛知県知事"

    def test_tokyo(self):
        assert normalize_authority_name("東京知事") == "東京都知事"

    def test_osaka(self):
        assert normalize_authority_name("大阪知事") == "大阪府知事"

    def test_kyoto(self):
        assert normalize_authority_name("京都知事") == "京都府知事"

    def test_daijin(self):
        assert normalize_authority_name("国土交通大臣") == "国土交通大臣"

    def test_daijin_variant(self):
        assert normalize_authority_name("国交省大臣") == "国土交通大臣"

    def test_empty(self):
        assert normalize_authority_name("") == ""


class TestEstimateConfidence:
    def test_empty_pages(self):
        assert estimate_confidence([]) == 0.0

    def test_low_confidence(self):
        # 50文字/ページ → 0.2
        assert estimate_confidence(["a" * 50]) == pytest.approx(0.2)

    def test_medium_confidence(self):
        # 150文字/ページ → 0.45
        assert estimate_confidence(["a" * 150]) == pytest.approx(0.45)

    def test_high_confidence(self):
        # 400文字/ページ → 0.8
        assert estimate_confidence(["a" * 400]) == pytest.approx(0.8)

    def test_multiple_pages_avg(self):
        # 平均: (50 + 400) / 2 = 225 → 0.45
        result = estimate_confidence(["a" * 50, "a" * 400])
        assert result == pytest.approx(0.45)


class TestMatchCompany:
    def _make_csv(self, tmp_path: Path, rows: list[dict]) -> Path:
        csv_path = tmp_path / "companies.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["company_id", "company_name_normalized"])
            writer.writeheader()
            writer.writerows(rows)
        return csv_path

    def test_exact_match(self, tmp_path):
        csv_path = self._make_csv(tmp_path, [
            {"company_id": "C001", "company_name_normalized": "株式会社ABC建設"},
        ])
        cid, mtype = match_company("株式会社ABC建設", csv_path)
        assert cid == "C001"
        assert mtype == "exact"

    def test_partial_match_is_ambiguous(self, tmp_path):
        csv_path = self._make_csv(tmp_path, [
            {"company_id": "C002", "company_name_normalized": "株式会社ABC建設工業"},
        ])
        cid, mtype = match_company("ABC建設", csv_path)
        assert cid == "C002"
        assert mtype == "ambiguous"  # 部分一致は1件でも ambiguous

    def test_multiple_exact_is_ambiguous(self, tmp_path):
        csv_path = self._make_csv(tmp_path, [
            {"company_id": "C001", "company_name_normalized": "ABC建設"},
            {"company_id": "C002", "company_name_normalized": "ABC建設"},
        ])
        cid, mtype = match_company("ABC建設", csv_path)
        assert mtype == "ambiguous"

    def test_not_found(self, tmp_path):
        csv_path = self._make_csv(tmp_path, [
            {"company_id": "C001", "company_name_normalized": "全然関係ない会社"},
        ])
        cid, mtype = match_company("ABC建設", csv_path)
        assert cid == ""
        assert mtype == "not_found"

    def test_csv_not_exist(self, tmp_path):
        cid, mtype = match_company("ABC", tmp_path / "nonexistent.csv")
        assert cid == ""
        assert mtype == "not_found"


# ---------------------------------------------------------------------------
# register_sheets: _is_blank, determine_current_status
# ---------------------------------------------------------------------------
from register_sheets import _is_blank, determine_current_status  # noqa: E402


# ---------------------------------------------------------------------------
# mlit_confirm: _a1 (batch_update range 記法の回帰テスト)
# ---------------------------------------------------------------------------
from mlit_confirm import _a1  # noqa: E402


class TestA1Notation:
    """gspread batch_update は A1 記法のみサポート（R1C1 は INVALID_ARGUMENT）。回帰テスト。"""

    def test_a1_col_a(self):
        assert _a1(1, 1) == "A1"

    def test_a1_col_z(self):
        assert _a1(2, 26) == "Z2"

    def test_a1_col_aa(self):
        assert _a1(3, 27) == "AA3"

    def test_mlit_typical_row(self):
        # Permits シートは33列。mlit_confirmed_date=18列目, mlit_confirm_result=19列目
        assert _a1(5, 18) == "R5"
        assert _a1(5, 19) == "S5"

    def test_no_r1c1_format(self):
        """R{n}C{n} 形式になっていないことを確認"""
        result = _a1(2, 3)
        assert not result.startswith("R") or not "C" in result or result[1:].isdigit()
        assert result == "C2"


class TestIsBlank:
    def test_none(self):
        assert _is_blank(None) is True

    def test_empty_string(self):
        assert _is_blank("") is True

    def test_whitespace(self):
        assert _is_blank("   ") is True

    def test_nan_string(self):
        assert _is_blank("nan") is True

    def test_none_string(self):
        assert _is_blank("None") is True

    def test_null_string(self):
        assert _is_blank("null") is True

    def test_valid_string(self):
        assert _is_blank("ABC") is False

    def test_zero_int(self):
        # 0 は空とみなさない
        assert _is_blank(0) is False


class TestDetermineCurrentStatus:
    """date.today() に依存するため、固定オフセットで相対日付を使用"""

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
        assert determine_current_status(self._row()) == "VALID"

    def test_expiring_within_90_days(self):
        today = date.today()
        row = self._row(
            expiry_date=(today + timedelta(days=60)).isoformat(),
            renewal_deadline_date=(today + timedelta(days=30)).isoformat(),
        )
        assert determine_current_status(row) == "EXPIRING"

    def test_renewal_overdue(self):
        today = date.today()
        row = self._row(
            expiry_date=(today + timedelta(days=20)).isoformat(),
            renewal_deadline_date=(today - timedelta(days=5)).isoformat(),
        )
        assert determine_current_status(row) == "RENEWAL_OVERDUE"

    def test_expired(self):
        today = date.today()
        row = self._row(
            expiry_date=(today - timedelta(days=10)).isoformat(),
        )
        assert determine_current_status(row) == "EXPIRED"

    def test_renewal_in_progress(self):
        today = date.today()
        expiry = today - timedelta(days=5)  # 満了超過
        row = self._row(
            expiry_date=expiry.isoformat(),
            renewal_application_date=(expiry - timedelta(days=10)).isoformat(),  # 期限内申請
            evidence_renewal_application="true",
            evidence_file_path="/path/to/receipt.pdf",
        )
        assert determine_current_status(row) == "RENEWAL_IN_PROGRESS"

    def test_renewal_in_progress_requires_evidence_file(self):
        """受付票パスがないとRENEWAL_IN_PROGRESSにならない→EXPIRED"""
        today = date.today()
        expiry = today - timedelta(days=5)
        row = self._row(
            expiry_date=expiry.isoformat(),
            renewal_application_date=(expiry - timedelta(days=10)).isoformat(),
            evidence_renewal_application="true",
            evidence_file_path="",  # ← 証跡なし
        )
        assert determine_current_status(row) == "EXPIRED"

    def test_deficient_missing_expiry(self):
        row = self._row(expiry_date="")
        assert determine_current_status(row) == "DEFICIENT"

    def test_deficient_missing_contractor_number(self):
        row = self._row(contractor_number="")
        assert determine_current_status(row) == "DEFICIENT"

    def test_deficient_missing_trade_categories(self):
        row = self._row(trade_categories="")
        assert determine_current_status(row) == "DEFICIENT"

    def test_requires_action_no_company_id(self):
        row = self._row(company_id="")
        # expiry_date は有効だが company_id が空
        assert determine_current_status(row) == "REQUIRES_ACTION"
