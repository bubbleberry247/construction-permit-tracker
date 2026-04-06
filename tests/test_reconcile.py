"""
tests/test_reconcile.py -- reconcile.py のユニットテスト

テスト対象:
  - reconcile_by_email: メールアドレス完全一致突合
  - reconcile_by_name: 会社名完全一致突合
  - reconcile_batch: バッチ突合
  - generate_reconcile_report: サマリーレポート生成

実行:
    cd <project_root>
    pytest tests/test_reconcile.py -v
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# sys.path 設定
# ---------------------------------------------------------------------------
_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from reconcile import (  # noqa: E402
    ReconcileResult,
    _normalize_email,
    generate_reconcile_report,
    load_email_map,
    load_master_companies,
    reconcile_batch,
    reconcile_by_email,
    reconcile_by_name,
)


# ===========================================================================
# _normalize_email
# ===========================================================================
class TestNormalizeEmail:

    def test_plain_email(self):
        assert _normalize_email("user@example.com") == "user@example.com"

    def test_angle_brackets_removed(self):
        assert _normalize_email("<user@example.com>") == "user@example.com"

    def test_uppercase_lowered(self):
        assert _normalize_email("User@Example.COM") == "user@example.com"

    def test_whitespace_stripped(self):
        assert _normalize_email("  user@example.com  ") == "user@example.com"

    def test_empty_string(self):
        assert _normalize_email("") == ""

    def test_none_returns_empty(self):
        # type: ignore は呼び出し側の責任だが防御的に動作確認
        assert _normalize_email(None) == ""  # type: ignore[arg-type]


# ===========================================================================
# reconcile_by_email
# ===========================================================================
class TestReconcileByEmail:

    @pytest.fixture()
    def email_map(self) -> dict[str, str]:
        return {
            "tanaka@abc-kensetsu.co.jp": "C001",
            "suzuki@xyz-kougyou.co.jp": "C002",
            "info@shared-domain.co.jp": "C003",
        }

    def test_exact_match(self, email_map: dict[str, str]):
        """メールアドレス完全一致で正しくマッチする"""
        r = reconcile_by_email("tanaka@abc-kensetsu.co.jp", email_map)
        assert r.company_id == "C001"
        assert r.match_method == "exact_email"
        assert r.confidence == 1.0
        assert r.warnings == []

    def test_exact_match_with_angle_brackets(self, email_map: dict[str, str]):
        """山括弧付きメールアドレスでもマッチする"""
        r = reconcile_by_email("<tanaka@abc-kensetsu.co.jp>", email_map)
        assert r.company_id == "C001"
        assert r.match_method == "exact_email"

    def test_exact_match_case_insensitive(self, email_map: dict[str, str]):
        """大文字小文字を区別しない"""
        r = reconcile_by_email("Tanaka@ABC-Kensetsu.CO.JP", email_map)
        assert r.company_id == "C001"
        assert r.match_method == "exact_email"

    def test_domain_only_does_not_match(self, email_map: dict[str, str]):
        """ドメイン一致だけではマッチしない（禁止ルール）"""
        r = reconcile_by_email("yamada@abc-kensetsu.co.jp", email_map)
        assert r.company_id is None
        assert r.match_method == "unmatched"

    def test_shared_domain_no_cross_match(self, email_map: dict[str, str]):
        """共有ドメインの別ユーザーはマッチしない"""
        r = reconcile_by_email("other-user@shared-domain.co.jp", email_map)
        assert r.company_id is None
        assert r.match_method == "unmatched"

    def test_empty_email(self, email_map: dict[str, str]):
        """空メールアドレス → unmatched"""
        r = reconcile_by_email("", email_map)
        assert r.company_id is None
        assert r.match_method == "unmatched"
        assert len(r.warnings) > 0

    def test_none_email(self, email_map: dict[str, str]):
        """None → unmatched"""
        r = reconcile_by_email(None, email_map)  # type: ignore[arg-type]
        assert r.company_id is None
        assert r.match_method == "unmatched"

    def test_invalid_email_no_at(self, email_map: dict[str, str]):
        """@を含まない文字列 → unmatched"""
        r = reconcile_by_email("not-an-email", email_map)
        assert r.company_id is None
        assert r.match_method == "unmatched"
        assert any("無効" in w for w in r.warnings)

    def test_empty_map(self):
        """空のメールマップ → unmatched"""
        r = reconcile_by_email("tanaka@abc.co.jp", {})
        assert r.company_id is None
        assert r.match_method == "unmatched"


# ===========================================================================
# reconcile_by_name
# ===========================================================================
class TestReconcileByName:

    @pytest.fixture()
    def master_companies(self) -> list[dict]:
        return [
            {"company_id": "C001", "official_name": "株式会社ABC建設"},
            {"company_id": "C002", "official_name": "有限会社XYZ工業"},
            {"company_id": "C003", "official_name": "田中建設株式会社"},
        ]

    def test_exact_match(self, master_companies: list[dict]):
        """会社名完全一致でマッチする"""
        r = reconcile_by_name("株式会社ABC建設", master_companies)
        assert r.company_id == "C001"
        assert r.official_name == "株式会社ABC建設"
        assert r.match_method == "exact_name"
        assert r.confidence == 1.0

    def test_kabushiki_maru_does_not_match(self, master_companies: list[dict]):
        """㈱と株式会社の違いで自動マッチしない（正規化禁止ルール）"""
        r = reconcile_by_name("㈱ABC建設", master_companies)
        assert r.company_id is None
        assert r.match_method == "unmatched"

    def test_kaisha_suffix_order_does_not_match(self, master_companies: list[dict]):
        """法人格の前後位置が違うとマッチしない"""
        r = reconcile_by_name("ABC建設株式会社", master_companies)
        assert r.company_id is None
        assert r.match_method == "unmatched"

    def test_partial_name_does_not_match(self, master_companies: list[dict]):
        """部分一致ではマッチしない"""
        r = reconcile_by_name("ABC建設", master_companies)
        assert r.company_id is None
        assert r.match_method == "unmatched"

    def test_extra_space_does_not_match(self, master_companies: list[dict]):
        """余分なスペースが含まれるとマッチしない（strip後の完全一致）"""
        # stripは行うが、内部のスペース違いはマッチしない
        r = reconcile_by_name("株式会社 ABC建設", master_companies)
        assert r.company_id is None
        assert r.match_method == "unmatched"

    def test_leading_trailing_space_stripped(self, master_companies: list[dict]):
        """前後の空白はstripされてマッチする"""
        r = reconcile_by_name("  株式会社ABC建設  ", master_companies)
        assert r.company_id == "C001"
        assert r.match_method == "exact_name"

    def test_empty_name(self, master_companies: list[dict]):
        """空の会社名 → unmatched"""
        r = reconcile_by_name("", master_companies)
        assert r.company_id is None
        assert r.match_method == "unmatched"
        assert len(r.warnings) > 0

    def test_none_name(self, master_companies: list[dict]):
        """None → unmatched"""
        r = reconcile_by_name(None, master_companies)  # type: ignore[arg-type]
        assert r.company_id is None
        assert r.match_method == "unmatched"

    def test_whitespace_only_name(self, master_companies: list[dict]):
        """空白のみ → unmatched"""
        r = reconcile_by_name("   ", master_companies)
        assert r.company_id is None
        assert r.match_method == "unmatched"

    def test_empty_master_list(self):
        """空のマスタリスト → unmatched"""
        r = reconcile_by_name("株式会社ABC建設", [])
        assert r.company_id is None
        assert r.match_method == "unmatched"

    def test_duplicate_names_in_master(self):
        """マスタに同名企業が複数 → unmatched（手動確認必要）"""
        master = [
            {"company_id": "C001", "official_name": "株式会社ABC建設"},
            {"company_id": "C099", "official_name": "株式会社ABC建設"},
        ]
        r = reconcile_by_name("株式会社ABC建設", master)
        assert r.company_id is None
        assert r.match_method == "unmatched"
        assert any("複数" in w for w in r.warnings)


# ===========================================================================
# load_master_companies / load_email_map (CSV形式)
# ===========================================================================
class TestLoadMasterCompanies:

    def test_load_csv(self, tmp_path: Path):
        csv_path = tmp_path / "master.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["company_id", "official_name"])
            writer.writeheader()
            writer.writerow({"company_id": "C001", "official_name": "株式会社ABC建設"})
            writer.writerow({"company_id": "C002", "official_name": "有限会社XYZ工業"})
        result = load_master_companies(csv_path)
        assert len(result) == 2
        assert result[0]["company_id"] == "C001"

    def test_nonexistent_file(self, tmp_path: Path):
        result = load_master_companies(tmp_path / "nonexistent.csv")
        assert result == []

    def test_unsupported_extension(self, tmp_path: Path):
        txt_path = tmp_path / "master.txt"
        txt_path.write_text("dummy", encoding="utf-8")
        result = load_master_companies(txt_path)
        assert result == []


class TestLoadEmailMap:

    def test_load_csv(self, tmp_path: Path):
        csv_path = tmp_path / "email_map.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["email", "company_id"])
            writer.writeheader()
            writer.writerow({"email": "tanaka@abc.co.jp", "company_id": "C001"})
        result = load_email_map(csv_path)
        assert result == {"tanaka@abc.co.jp": "C001"}

    def test_nonexistent_file(self, tmp_path: Path):
        result = load_email_map(tmp_path / "nonexistent.csv")
        assert result == {}


# ===========================================================================
# reconcile_batch
# ===========================================================================
class TestReconcileBatch:

    def _make_master_csv(self, tmp_path: Path) -> Path:
        csv_path = tmp_path / "master.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["company_id", "official_name"])
            writer.writeheader()
            writer.writerow({"company_id": "C001", "official_name": "株式会社ABC建設"})
            writer.writerow({"company_id": "C002", "official_name": "有限会社XYZ工業"})
        return csv_path

    def _make_email_csv(self, tmp_path: Path) -> Path:
        csv_path = tmp_path / "email_map.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["email", "company_id"])
            writer.writeheader()
            writer.writerow({"email": "tanaka@abc-kensetsu.co.jp", "company_id": "C001"})
            writer.writerow({"email": "suzuki@xyz-kougyou.co.jp", "company_id": "C002"})
        return csv_path

    def test_email_match_in_batch(self, tmp_path: Path):
        """バッチ: メールアドレスでマッチ"""
        master = self._make_master_csv(tmp_path)
        email_map = self._make_email_csv(tmp_path)
        records = [{"sender_email": "tanaka@abc-kensetsu.co.jp", "company_name": ""}]
        results, unmatched = reconcile_batch(records, master, email_map)
        assert len(results) == 1
        assert results[0].company_id == "C001"
        assert results[0].match_method == "exact_email"
        assert len(unmatched) == 0

    def test_name_match_in_batch(self, tmp_path: Path):
        """バッチ: 会社名でマッチ"""
        master = self._make_master_csv(tmp_path)
        email_map = self._make_email_csv(tmp_path)
        records = [{"sender_email": "", "company_name": "株式会社ABC建設"}]
        results, unmatched = reconcile_batch(records, master, email_map)
        assert len(results) == 1
        assert results[0].company_id == "C001"
        assert results[0].match_method == "exact_name"
        assert len(unmatched) == 0

    def test_email_priority_over_name(self, tmp_path: Path):
        """バッチ: メール+名前両方ある場合、メールが優先される"""
        master = self._make_master_csv(tmp_path)
        email_map = self._make_email_csv(tmp_path)
        records = [{
            "sender_email": "tanaka@abc-kensetsu.co.jp",
            "company_name": "有限会社XYZ工業",
        }]
        results, unmatched = reconcile_batch(records, master, email_map)
        assert results[0].company_id == "C001"  # メール側のC001が優先
        assert results[0].match_method == "exact_email"

    def test_unmatched_separated(self, tmp_path: Path):
        """バッチ: 未マッチレコードが正しく分離される"""
        master = self._make_master_csv(tmp_path)
        email_map = self._make_email_csv(tmp_path)
        records = [
            {"sender_email": "tanaka@abc-kensetsu.co.jp", "company_name": ""},
            {"sender_email": "unknown@nobody.co.jp", "company_name": "存在しない会社"},
        ]
        results, unmatched = reconcile_batch(records, master, email_map)
        assert len(results) == 2
        assert len(unmatched) == 1
        assert unmatched[0]["sender_email"] == "unknown@nobody.co.jp"

    def test_empty_records(self, tmp_path: Path):
        """バッチ: 空のレコードリスト"""
        master = self._make_master_csv(tmp_path)
        email_map = self._make_email_csv(tmp_path)
        results, unmatched = reconcile_batch([], master, email_map)
        assert results == []
        assert unmatched == []

    def test_all_unmatched(self, tmp_path: Path):
        """バッチ: 全レコードが未マッチ"""
        master = self._make_master_csv(tmp_path)
        email_map = self._make_email_csv(tmp_path)
        records = [
            {"sender_email": "nobody@x.co.jp", "company_name": "不明な会社A"},
            {"sender_email": "nobody2@y.co.jp", "company_name": "不明な会社B"},
        ]
        results, unmatched = reconcile_batch(records, master, email_map)
        assert len(unmatched) == 2
        assert all(r.match_method == "unmatched" for r in results)

    def test_fallback_to_name_when_email_fails(self, tmp_path: Path):
        """バッチ: メール不一致 → 会社名でフォールバック"""
        master = self._make_master_csv(tmp_path)
        email_map = self._make_email_csv(tmp_path)
        records = [{
            "sender_email": "unknown@nobody.co.jp",
            "company_name": "有限会社XYZ工業",
        }]
        results, unmatched = reconcile_batch(records, master, email_map)
        assert results[0].company_id == "C002"
        assert results[0].match_method == "exact_name"
        assert len(unmatched) == 0


# ===========================================================================
# generate_reconcile_report
# ===========================================================================
class TestGenerateReconcileReport:

    def test_basic_report(self):
        """サマリーレポートが正しく生成される"""
        results = [
            ReconcileResult("C001", "ABC建設", "exact_email", 1.0),
            ReconcileResult("C002", "XYZ工業", "exact_name", 1.0),
            ReconcileResult(None, "", "unmatched", 0.0, ["未マッチ"]),
        ]
        report = generate_reconcile_report(results)
        assert report["total"] == 3
        assert report["matched"] == 2
        assert report["unmatched"] == 1
        assert report["by_method"]["exact_email"] == 1
        assert report["by_method"]["exact_name"] == 1
        assert report["by_method"]["unmatched"] == 1
        assert report["avg_confidence"] == pytest.approx(2.0 / 3.0, abs=0.001)
        assert "未マッチ" in report["warnings"]

    def test_all_matched_report(self):
        """全件マッチ時のレポート"""
        results = [
            ReconcileResult("C001", "ABC", "exact_email", 1.0),
            ReconcileResult("C002", "XYZ", "exact_email", 1.0),
        ]
        report = generate_reconcile_report(results)
        assert report["matched"] == 2
        assert report["unmatched"] == 0
        assert report["avg_confidence"] == 1.0
        assert report["warnings"] == []

    def test_all_unmatched_report(self):
        """全件未マッチ時のレポート"""
        results = [
            ReconcileResult(None, "", "unmatched", 0.0, ["w1"]),
            ReconcileResult(None, "", "unmatched", 0.0, ["w2"]),
        ]
        report = generate_reconcile_report(results)
        assert report["matched"] == 0
        assert report["unmatched"] == 2
        assert report["avg_confidence"] == 0.0
        assert len(report["warnings"]) == 2

    def test_empty_results_report(self):
        """空の結果リスト"""
        report = generate_reconcile_report([])
        assert report["total"] == 0
        assert report["matched"] == 0
        assert report["unmatched"] == 0
        assert report["avg_confidence"] == 0.0
        assert report["by_method"] == {}
        assert report["warnings"] == []

    def test_report_warns_collected(self):
        """全レコードの warnings が集約される"""
        results = [
            ReconcileResult("C001", "", "exact_email", 1.0, ["warn-a"]),
            ReconcileResult(None, "", "unmatched", 0.0, ["warn-b", "warn-c"]),
        ]
        report = generate_reconcile_report(results)
        assert set(report["warnings"]) == {"warn-a", "warn-b", "warn-c"}
