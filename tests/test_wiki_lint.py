"""
tests/test_wiki_lint.py -- wiki_lint.py のユニットテスト

実行:
    cd <project_root>
    pytest tests/test_wiki_lint.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# sys.path 設定 -- src/ を参照できるようにする
# ---------------------------------------------------------------------------
_SRC = Path(__file__).parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from wiki_lint import (  # noqa: E402
    CONFIG_REQUIRED_KEYS,
    LintIssue,
    format_report,
    lint_all,
    lint_claude_md,
    lint_config,
    lint_dead_links,
    lint_memory_files,
)


# ===========================================================================
# Helper
# ===========================================================================

def _write(path: Path, content: str) -> Path:
    """UTF-8 でファイルを書き出す。親ディレクトリも自動作成。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _minimal_claude_md() -> str:
    """issue が出ない最小限の CLAUDE.md。"""
    return (
        "# Project\n\n"
        "## TODO\n"
        "- [ ] 未完了タスク\n"
    )


def _minimal_config() -> dict:
    """必須キーをすべて含む最小限の config。"""
    return {
        "GOOGLE_CREDENTIALS_FILE": "creds.json",
        "GOOGLE_SHEETS_ID": "abc123",
        "DATA_ROOT": "/tmp/data",
        "STAGING_CSV_DIR": "output",
        "LOG_LEVEL": "INFO",
    }


# ===========================================================================
# lint_claude_md
# ===========================================================================
class TestLintClaudeMd:

    def test_no_issues_on_valid_file(self, tmp_path: Path):
        """正常な CLAUDE.md では issue が出ない。"""
        _write(tmp_path / "CLAUDE.md", _minimal_claude_md())
        issues = lint_claude_md(tmp_path)
        assert issues == []

    def test_missing_claude_md(self, tmp_path: Path):
        """CLAUDE.md が存在しない場合 WL001 エラー。"""
        issues = lint_claude_md(tmp_path)
        assert len(issues) == 1
        assert issues[0].rule_id == "WL001"
        assert issues[0].severity == "error"

    def test_wl008_completed_todo_detected(self, tmp_path: Path):
        """完了済み TODO (~~strikethrough~~) が WL008 として検出される。"""
        content = (
            "# Project\n\n"
            "## TODO\n"
            "- [x] ~~古いタスク~~ → 完了\n"
            "- [ ] 未完了タスク\n"
        )
        _write(tmp_path / "CLAUDE.md", content)
        issues = lint_claude_md(tmp_path)
        wl008 = [i for i in issues if i.rule_id == "WL008"]
        assert len(wl008) == 1
        assert wl008[0].severity == "info"
        assert wl008[0].line_number == 4

    def test_wl008_multiple_completed_todos(self, tmp_path: Path):
        """複数の完了済み TODO がそれぞれ検出される。"""
        content = (
            "- [x] ~~タスクA~~ → done\n"
            "- [x] ~~タスクB~~ → done\n"
            "- [ ] 未完了\n"
        )
        _write(tmp_path / "CLAUDE.md", content)
        issues = lint_claude_md(tmp_path)
        wl008 = [i for i in issues if i.rule_id == "WL008"]
        assert len(wl008) == 2


# ===========================================================================
# lint_config
# ===========================================================================
class TestLintConfig:

    def test_valid_config_no_issues(self, tmp_path: Path):
        """必須キーがすべて揃った config では issue が出ない。"""
        cfg_path = tmp_path / "config.json"
        _write(cfg_path, json.dumps(_minimal_config()))
        issues = lint_config(cfg_path)
        assert issues == []

    def test_missing_config_file(self, tmp_path: Path):
        """config.json が存在しない場合 WL002 エラー。"""
        issues = lint_config(tmp_path / "config.json")
        assert len(issues) == 1
        assert issues[0].rule_id == "WL002"
        assert issues[0].severity == "error"

    def test_invalid_json(self, tmp_path: Path):
        """パース不能な JSON で WL002 エラー。"""
        cfg_path = _write(tmp_path / "config.json", "{invalid json}")
        issues = lint_config(cfg_path)
        assert len(issues) == 1
        assert issues[0].rule_id == "WL002"

    def test_wl002_missing_required_key(self, tmp_path: Path):
        """必須キーが 1 つ欠落した場合 WL002 が 1 件出る。"""
        cfg = _minimal_config()
        del cfg["GOOGLE_SHEETS_ID"]
        cfg_path = _write(tmp_path / "config.json", json.dumps(cfg))
        issues = lint_config(cfg_path)
        wl002 = [i for i in issues if i.rule_id == "WL002"]
        assert len(wl002) == 1
        assert "GOOGLE_SHEETS_ID" in wl002[0].message

    def test_wl002_multiple_missing_keys(self, tmp_path: Path):
        """複数の必須キー欠落で WL002 が複数件出る。"""
        cfg_path = _write(tmp_path / "config.json", json.dumps({"_comment": "empty"}))
        issues = lint_config(cfg_path)
        wl002 = [i for i in issues if i.rule_id == "WL002"]
        assert len(wl002) == len(CONFIG_REQUIRED_KEYS)

    def test_wl003_wrong_type(self, tmp_path: Path):
        """値の型が不正な場合 WL003 が出る。"""
        cfg = _minimal_config()
        cfg["GOOGLE_SHEETS_ID"] = 12345  # str のはずが int
        cfg_path = _write(tmp_path / "config.json", json.dumps(cfg))
        issues = lint_config(cfg_path)
        wl003 = [i for i in issues if i.rule_id == "WL003"]
        assert len(wl003) == 1
        assert "GOOGLE_SHEETS_ID" in wl003[0].message
        assert wl003[0].severity == "warning"


# ===========================================================================
# lint_memory_files
# ===========================================================================
class TestLintMemoryFiles:

    def test_no_memory_dir_returns_empty(self, tmp_path: Path):
        """memory/ が存在しなければ空リスト。"""
        issues = lint_memory_files(tmp_path / "memory")
        assert issues == []

    def test_valid_frontmatter_no_issues(self, tmp_path: Path):
        """正しい frontmatter + インデックス一致なら issue なし。"""
        mem_dir = tmp_path / "memory"
        _write(mem_dir / "MEMORY.md", "# Index\n- [note](note.md)\n")
        _write(mem_dir / "note.md", "---\ntitle: Note\n---\n# Note\n")
        issues = lint_memory_files(mem_dir)
        assert issues == []

    def test_wl004_missing_frontmatter(self, tmp_path: Path):
        """frontmatter のない .md ファイルで WL004 が出る。"""
        mem_dir = tmp_path / "memory"
        _write(mem_dir / "MEMORY.md", "# Index\n- [note](note.md)\n")
        _write(mem_dir / "note.md", "# Note without frontmatter\n")
        issues = lint_memory_files(mem_dir)
        wl004 = [i for i in issues if i.rule_id == "WL004"]
        assert len(wl004) == 1

    def test_wl005_indexed_but_missing(self, tmp_path: Path):
        """インデックスに記載されているがファイルが存在しない → WL005。"""
        mem_dir = tmp_path / "memory"
        _write(mem_dir / "MEMORY.md", "# Index\n- [gone](ghost.md)\n")
        issues = lint_memory_files(mem_dir)
        wl005 = [i for i in issues if i.rule_id == "WL005"]
        assert len(wl005) == 1
        assert "ghost.md" in wl005[0].message
        assert wl005[0].severity == "error"

    def test_wl006_file_not_indexed(self, tmp_path: Path):
        """ファイルは存在するがインデックスに記載なし → WL006。"""
        mem_dir = tmp_path / "memory"
        _write(mem_dir / "MEMORY.md", "# Index\n")
        _write(mem_dir / "orphan.md", "---\ntitle: Orphan\n---\n")
        issues = lint_memory_files(mem_dir)
        wl006 = [i for i in issues if i.rule_id == "WL006"]
        assert len(wl006) == 1
        assert "orphan.md" in wl006[0].message
        assert wl006[0].severity == "warning"

    def test_empty_md_file_no_frontmatter_warning(self, tmp_path: Path):
        """空の .md ファイルは frontmatter チェック対象外。"""
        mem_dir = tmp_path / "memory"
        _write(mem_dir / "MEMORY.md", "# Index\n")
        _write(mem_dir / "empty.md", "")
        issues = lint_memory_files(mem_dir)
        wl004 = [i for i in issues if i.rule_id == "WL004"]
        assert len(wl004) == 0


# ===========================================================================
# lint_dead_links
# ===========================================================================
class TestLintDeadLinks:

    def test_valid_link_no_issues(self, tmp_path: Path):
        """存在するファイルへのリンクは issue にならない。"""
        _write(tmp_path / "README.md", "[config](config.json)\n")
        _write(tmp_path / "config.json", "{}")
        issues = lint_dead_links(tmp_path)
        assert issues == []

    def test_wl007_dead_link_detected(self, tmp_path: Path):
        """存在しないファイルへのリンクで WL007 が出る。"""
        _write(tmp_path / "README.md", "[missing](no_such_file.txt)\n")
        issues = lint_dead_links(tmp_path)
        wl007 = [i for i in issues if i.rule_id == "WL007"]
        assert len(wl007) == 1
        assert "no_such_file.txt" in wl007[0].message
        assert wl007[0].line_number == 1

    def test_external_url_ignored(self, tmp_path: Path):
        """外部 URL (http/https) はチェック対象外。"""
        _write(tmp_path / "README.md", "[Google](https://google.com)\n")
        issues = lint_dead_links(tmp_path)
        assert issues == []

    def test_anchor_link_ignored(self, tmp_path: Path):
        """アンカーリンク (#section) はチェック対象外。"""
        _write(tmp_path / "README.md", "[section](#overview)\n")
        issues = lint_dead_links(tmp_path)
        assert issues == []

    def test_multiple_dead_links(self, tmp_path: Path):
        """同一ファイル内の複数デッドリンクがそれぞれ検出される。"""
        content = (
            "[a](aaa.md)\n"
            "[b](bbb.md)\n"
            "[c](https://example.com)\n"
        )
        _write(tmp_path / "docs.md", content)
        issues = lint_dead_links(tmp_path)
        wl007 = [i for i in issues if i.rule_id == "WL007"]
        assert len(wl007) == 2

    def test_dotfiles_skipped(self, tmp_path: Path):
        """.git 等のドットディレクトリ配下はスキップされる。"""
        dot_dir = tmp_path / ".git"
        _write(dot_dir / "info.md", "[missing](no.md)\n")
        issues = lint_dead_links(tmp_path)
        assert issues == []


# ===========================================================================
# severity フィルタリング
# ===========================================================================
class TestSeverityFiltering:

    def test_filter_errors_only(self, tmp_path: Path):
        """severity='error' だけを抽出できる。"""
        issues = [
            LintIssue("a.md", 1, "error", "WL001", "err"),
            LintIssue("b.md", 2, "warning", "WL004", "warn"),
            LintIssue("c.md", 3, "info", "WL008", "info"),
        ]
        errors = [i for i in issues if i.severity == "error"]
        assert len(errors) == 1
        assert errors[0].rule_id == "WL001"

    def test_filter_warnings_only(self):
        issues = [
            LintIssue("a.md", 1, "error", "WL002", "e"),
            LintIssue("b.md", 2, "warning", "WL007", "w"),
        ]
        warnings = [i for i in issues if i.severity == "warning"]
        assert len(warnings) == 1
        assert warnings[0].rule_id == "WL007"


# ===========================================================================
# format_report
# ===========================================================================
class TestFormatReport:

    def test_no_issues_message(self):
        """issue がなければ 'All checks passed' メッセージ。"""
        report = format_report([])
        assert "All checks passed" in report

    def test_report_contains_rule_id(self):
        """レポートにルール ID が含まれる。"""
        issues = [LintIssue("x.md", 10, "error", "WL002", "key missing")]
        report = format_report(issues)
        assert "WL002" in report
        assert "x.md" in report

    def test_report_groups_by_severity(self):
        """severity 別にグループ化される。"""
        issues = [
            LintIssue("a.md", 1, "error", "WL001", "err"),
            LintIssue("b.md", 2, "warning", "WL007", "warn"),
            LintIssue("c.md", 3, "info", "WL008", "info"),
        ]
        report = format_report(issues)
        assert "ERROR" in report
        assert "WARNING" in report
        assert "INFO" in report
        # ERROR が WARNING より先に表示される
        assert report.index("ERROR") < report.index("WARNING")
        assert report.index("WARNING") < report.index("INFO")

    def test_report_count_header(self):
        """レポート先頭に issue 数が表示される。"""
        issues = [
            LintIssue("a.md", 1, "error", "WL001", "e"),
            LintIssue("b.md", 2, "error", "WL002", "e"),
        ]
        report = format_report(issues)
        assert "2 issue(s)" in report

    def test_report_line_number_format(self):
        """行番号がある場合は file:line 形式で表示される。"""
        issues = [LintIssue("test.md", 42, "warning", "WL007", "dead")]
        report = format_report(issues)
        assert "test.md:42" in report

    def test_report_no_line_number(self):
        """行番号が None の場合はコロンなし。"""
        issues = [LintIssue("test.md", None, "error", "WL002", "missing")]
        report = format_report(issues)
        assert "test.md:" not in report
        assert "test.md" in report


# ===========================================================================
# lint_all (統合テスト)
# ===========================================================================
class TestLintAll:

    def test_clean_project_no_issues(self, tmp_path: Path):
        """最小限の正常プロジェクトで issue が出ない。"""
        _write(tmp_path / "CLAUDE.md", _minimal_claude_md())
        _write(tmp_path / "config.json", json.dumps(_minimal_config()))
        issues = lint_all(tmp_path)
        assert issues == []

    def test_mixed_issues_collected(self, tmp_path: Path):
        """複数チェックからの issue がまとめて返される。"""
        # CLAUDE.md なし → WL001
        # config.json 必須キー欠落 → WL002
        _write(tmp_path / "config.json", json.dumps({"_comment": "empty"}))
        issues = lint_all(tmp_path)
        rule_ids = {i.rule_id for i in issues}
        assert "WL001" in rule_ids
        assert "WL002" in rule_ids
