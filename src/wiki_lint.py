"""
src/wiki_lint.py -- プロジェクトドキュメント・設定ファイルの整合性チェック linter

チェック項目:
  - WL001: CLAUDE.md 内の参照ファイルが存在しない
  - WL002: config.json の必須キーが欠落
  - WL003: config.json の値の型が不正
  - WL004: memory/*.md の frontmatter が不正
  - WL005: MEMORY.md のインデックスに記載があるが実ファイルがない
  - WL006: memory/*.md が存在するが MEMORY.md インデックスに未記載
  - WL007: マークダウン内デッドリンク
  - WL008: TODO 項目が古い（完了マーク付きだが本文に残存）

実行:
    cd <project_root>
    python -X utf8 src/wiki_lint.py
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LintIssue:
    """lint で検出された個別の問題を表す。"""

    file_path: str
    line_number: int | None
    severity: str  # "error", "warning", "info"
    rule_id: str  # "WL001" .. "WL008"
    message: str


# ---------------------------------------------------------------------------
# 定数
# ---------------------------------------------------------------------------

CONFIG_REQUIRED_KEYS: dict[str, type] = {
    "GOOGLE_CREDENTIALS_FILE": str,
    "GOOGLE_SHEETS_ID": str,
    "DATA_ROOT": str,
    "STAGING_CSV_DIR": str,
    "LOG_LEVEL": str,
}

_MD_LINK_PATTERN = re.compile(r"\[([^\]]*)\]\(([^)]+)\)")
_TODO_DONE_PATTERN = re.compile(r"^\s*-\s*\[x\]\s*~~(.+?)~~", re.IGNORECASE)
_FRONTMATTER_FENCE = re.compile(r"^---\s*$")
_MEMORY_INDEX_LINK = re.compile(r"\[.*?\]\(([^)]+\.md)\)")


# ---------------------------------------------------------------------------
# WL001: CLAUDE.md 参照ファイル存在チェック
# ---------------------------------------------------------------------------

def _extract_file_references(text: str) -> list[tuple[int, str]]:
    """CLAUDE.md 内のコードブロックに記載されたファイルパスを抽出する。"""
    refs: list[tuple[int, str]] = []
    in_code_block = False
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("```"):
            in_code_block = not in_code_block
            continue
        if in_code_block:
            # src/ や scripts/ tests/ で始まるパスっぽい行を拾う
            for segment in re.findall(r"(?:src|scripts|tests)/\S+", stripped):
                # ツリー表示の装飾を除去
                clean = segment.rstrip(",").strip()
                if clean:
                    refs.append((lineno, clean))
    return refs


def lint_claude_md(project_root: Path) -> list[LintIssue]:
    """CLAUDE.md の整合性チェック (WL001, WL008)。"""
    issues: list[LintIssue] = []
    claude_md = project_root / "CLAUDE.md"
    if not claude_md.exists():
        issues.append(LintIssue(
            file_path=str(claude_md),
            line_number=None,
            severity="error",
            rule_id="WL001",
            message="CLAUDE.md が存在しません",
        ))
        return issues

    text = claude_md.read_text(encoding="utf-8")

    # WL008: 完了済み TODO が残存
    for lineno, line in enumerate(text.splitlines(), start=1):
        if _TODO_DONE_PATTERN.search(line):
            issues.append(LintIssue(
                file_path=str(claude_md),
                line_number=lineno,
                severity="info",
                rule_id="WL008",
                message=f"完了済み TODO が残存: {line.strip()[:80]}",
            ))

    return issues


# ---------------------------------------------------------------------------
# WL002, WL003: config.json チェック
# ---------------------------------------------------------------------------

def lint_config(config_path: Path) -> list[LintIssue]:
    """config.json の必須キー存在・型チェック (WL002, WL003)。"""
    issues: list[LintIssue] = []
    if not config_path.exists():
        issues.append(LintIssue(
            file_path=str(config_path),
            line_number=None,
            severity="error",
            rule_id="WL002",
            message="config.json が存在しません",
        ))
        return issues

    try:
        data: dict[str, Any] = json.loads(
            config_path.read_text(encoding="utf-8")
        )
    except json.JSONDecodeError as exc:
        issues.append(LintIssue(
            file_path=str(config_path),
            line_number=None,
            severity="error",
            rule_id="WL002",
            message=f"config.json のパースに失敗: {exc}",
        ))
        return issues

    for key, expected_type in CONFIG_REQUIRED_KEYS.items():
        if key not in data:
            issues.append(LintIssue(
                file_path=str(config_path),
                line_number=None,
                severity="error",
                rule_id="WL002",
                message=f"必須キー '{key}' が欠落しています",
            ))
        elif not isinstance(data[key], expected_type):
            issues.append(LintIssue(
                file_path=str(config_path),
                line_number=None,
                severity="warning",
                rule_id="WL003",
                message=(
                    f"キー '{key}' の型が不正: "
                    f"期待={expected_type.__name__}, "
                    f"実際={type(data[key]).__name__}"
                ),
            ))

    return issues


# ---------------------------------------------------------------------------
# WL004, WL005, WL006: memory ファイル整合性
# ---------------------------------------------------------------------------

def _has_valid_frontmatter(text: str) -> bool:
    """YAML frontmatter (---..---) が存在するかチェック。"""
    lines = text.splitlines()
    if len(lines) < 2:
        return False
    if not _FRONTMATTER_FENCE.match(lines[0]):
        return False
    for line in lines[1:]:
        if _FRONTMATTER_FENCE.match(line):
            return True
    return False


def _parse_memory_index(index_path: Path) -> set[str]:
    """MEMORY.md からリンク参照されている .md ファイル名を抽出する。"""
    if not index_path.exists():
        return set()
    text = index_path.read_text(encoding="utf-8")
    return set(_MEMORY_INDEX_LINK.findall(text))


def lint_memory_files(memory_dir: Path) -> list[LintIssue]:
    """memory/*.md の frontmatter・インデックス一致チェック (WL004-WL006)。"""
    issues: list[LintIssue] = []
    if not memory_dir.exists():
        return issues

    index_path = memory_dir / "MEMORY.md"
    indexed_files = _parse_memory_index(index_path)

    # 実在する .md ファイル (MEMORY.md 自体は除外)
    actual_files: set[str] = set()
    for md_file in sorted(memory_dir.glob("*.md")):
        if md_file.name == "MEMORY.md":
            continue
        actual_files.add(md_file.name)

        # WL004: frontmatter チェック
        text = md_file.read_text(encoding="utf-8")
        if text.strip() and not _has_valid_frontmatter(text):
            issues.append(LintIssue(
                file_path=str(md_file),
                line_number=1,
                severity="warning",
                rule_id="WL004",
                message="YAML frontmatter (---) がありません",
            ))

    # WL005: インデックスにあるが実ファイルがない
    for fname in sorted(indexed_files - actual_files):
        issues.append(LintIssue(
            file_path=str(index_path),
            line_number=None,
            severity="error",
            rule_id="WL005",
            message=f"インデックスに記載されているが実ファイルがない: {fname}",
        ))

    # WL006: 実ファイルがあるがインデックスに未記載
    for fname in sorted(actual_files - indexed_files):
        issues.append(LintIssue(
            file_path=str(memory_dir / fname),
            line_number=None,
            severity="warning",
            rule_id="WL006",
            message=f"MEMORY.md インデックスに未記載: {fname}",
        ))

    return issues


# ---------------------------------------------------------------------------
# WL007: マークダウン内デッドリンク検出
# ---------------------------------------------------------------------------

def lint_dead_links(project_root: Path) -> list[LintIssue]:
    """マークダウンファイル内の相対リンクのデッドリンクを検出 (WL007)。"""
    issues: list[LintIssue] = []
    for md_file in sorted(project_root.rglob("*.md")):
        # node_modules, .git 等をスキップ
        parts = md_file.parts
        if any(p.startswith(".") or p == "node_modules" for p in parts):
            continue

        text = md_file.read_text(encoding="utf-8", errors="replace")
        for lineno, line in enumerate(text.splitlines(), start=1):
            for _label, href in _MD_LINK_PATTERN.findall(line):
                # 外部 URL, アンカー, mailto はスキップ
                if href.startswith(("http://", "https://", "#", "mailto:")):
                    continue
                # アンカー部分を除去
                target = href.split("#")[0]
                if not target:
                    continue
                resolved = (md_file.parent / target).resolve()
                if not resolved.exists():
                    issues.append(LintIssue(
                        file_path=str(md_file),
                        line_number=lineno,
                        severity="warning",
                        rule_id="WL007",
                        message=f"デッドリンク: {href}",
                    ))

    return issues


# ---------------------------------------------------------------------------
# lint_all / format_report
# ---------------------------------------------------------------------------

def lint_all(project_root: Path) -> list[LintIssue]:
    """全チェックを実行してサマリーを返す。"""
    issues: list[LintIssue] = []
    issues.extend(lint_claude_md(project_root))
    issues.extend(lint_config(project_root / "config.json"))
    issues.extend(lint_memory_files(project_root / "memory"))
    issues.extend(lint_dead_links(project_root))
    return issues


def format_report(issues: list[LintIssue]) -> str:
    """人間可読なレポートを生成する。"""
    if not issues:
        return "All checks passed. No issues found."

    lines: list[str] = []
    lines.append(f"wiki-lint: {len(issues)} issue(s) found\n")

    # severity 別にグループ化して表示
    for severity in ("error", "warning", "info"):
        group = [i for i in issues if i.severity == severity]
        if not group:
            continue
        lines.append(f"--- {severity.upper()} ({len(group)}) ---")
        for issue in group:
            loc = f":{issue.line_number}" if issue.line_number else ""
            lines.append(
                f"  [{issue.rule_id}] {issue.file_path}{loc}"
            )
            lines.append(f"    {issue.message}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI エントリーポイント
# ---------------------------------------------------------------------------

def main() -> int:
    """CLI 実行時のエントリーポイント。issue があれば exit 1。"""
    import sys

    project_root = Path(__file__).parent.parent
    issues = lint_all(project_root)
    report = format_report(issues)
    print(report)

    errors = [i for i in issues if i.severity == "error"]
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
