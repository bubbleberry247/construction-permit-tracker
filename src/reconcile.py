"""
reconcile.py -- 145社マスタと受信データ（メール・PDF）の突合統合モジュール

データ設計ルール（絶対）:
  - マスタ = 教師データ（145社.xlsx）。145社を無条件に全社登録
  - メールアドレスのドメイン一致での自動マッチは禁止
  - 教師データの会社名が正。表記ゆれは都度確認（自動正規化で同一判定しない）
  - 対象メール: shinsei.tic から kalimistk に転送されたメールのみ

Usage:
    from reconcile import reconcile_by_email, reconcile_by_name, reconcile_batch
"""
from __future__ import annotations

import csv
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class ReconcileResult:
    """突合結果。1レコード分。"""
    company_id: Optional[str]
    official_name: str
    match_method: str  # "exact_email", "exact_name", "manual", "unmatched"
    confidence: float  # 0.0-1.0
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# メールアドレス正規化（内部ユーティリティ）
# ---------------------------------------------------------------------------

def _normalize_email(raw: str) -> str:
    """山括弧除去 + 小文字化 + strip。fix_unmatched_emails.py と同一ロジック。"""
    if not raw:
        return ""
    m = re.search(r"<([^>]+)>", raw)
    if m:
        return m.group(1).strip().lower()
    m = re.search(r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)", raw)
    if m:
        return m.group(1).strip().lower()
    return raw.strip().lower()


# ---------------------------------------------------------------------------
# メールアドレス完全一致突合
# ---------------------------------------------------------------------------

def reconcile_by_email(
    sender_email: str,
    email_map: dict[str, str],
) -> ReconcileResult:
    """メールアドレス完全一致で突合。ドメイン一致は禁止。

    Args:
        sender_email: 送信者メールアドレス（raw文字列OK、山括弧除去される）
        email_map: {メールアドレス(小文字): company_id} の辞書

    Returns:
        ReconcileResult with match_method="exact_email" or "unmatched"
    """
    if not sender_email:
        return ReconcileResult(
            company_id=None,
            official_name="",
            match_method="unmatched",
            confidence=0.0,
            warnings=["sender_email が空です"],
        )

    normalized = _normalize_email(sender_email)
    if not normalized or "@" not in normalized:
        return ReconcileResult(
            company_id=None,
            official_name="",
            match_method="unmatched",
            confidence=0.0,
            warnings=[f"無効なメールアドレス: {sender_email}"],
        )

    # 完全一致のみ許可（ドメイン一致での自動マッチは禁止）
    company_id = email_map.get(normalized)
    if company_id:
        return ReconcileResult(
            company_id=company_id,
            official_name="",  # 呼び出し元で補完可能
            match_method="exact_email",
            confidence=1.0,
        )

    return ReconcileResult(
        company_id=None,
        official_name="",
        match_method="unmatched",
        confidence=0.0,
        warnings=[f"メールアドレス完全一致なし: {normalized}"],
    )


# ---------------------------------------------------------------------------
# 会社名完全一致突合
# ---------------------------------------------------------------------------

def reconcile_by_name(
    company_name: str,
    master_companies: list[dict],
) -> ReconcileResult:
    """会社名完全一致で突合。自動正規化での同一判定は禁止。

    Args:
        company_name: 突合対象の会社名
        master_companies: [{"company_id": ..., "official_name": ...}, ...] のリスト

    Returns:
        ReconcileResult with match_method="exact_name" or "unmatched"
    """
    if not company_name or not isinstance(company_name, str):
        return ReconcileResult(
            company_id=None,
            official_name="",
            match_method="unmatched",
            confidence=0.0,
            warnings=["company_name が空または無効です"],
        )

    name_stripped = company_name.strip()
    if not name_stripped:
        return ReconcileResult(
            company_id=None,
            official_name="",
            match_method="unmatched",
            confidence=0.0,
            warnings=["company_name が空白のみです"],
        )

    # 完全一致のみ（㈱/株式会社の正規化や部分一致は行わない）
    exact_matches: list[dict] = []
    for company in master_companies:
        official = company.get("official_name", "")
        if official == name_stripped:
            exact_matches.append(company)

    if len(exact_matches) == 1:
        matched = exact_matches[0]
        return ReconcileResult(
            company_id=matched["company_id"],
            official_name=matched["official_name"],
            match_method="exact_name",
            confidence=1.0,
        )

    if len(exact_matches) > 1:
        ids = [m["company_id"] for m in exact_matches]
        return ReconcileResult(
            company_id=None,
            official_name=name_stripped,
            match_method="unmatched",
            confidence=0.0,
            warnings=[f"完全一致が複数: {ids}（手動確認が必要）"],
        )

    return ReconcileResult(
        company_id=None,
        official_name="",
        match_method="unmatched",
        confidence=0.0,
        warnings=[f"会社名完全一致なし: {name_stripped}"],
    )


# ---------------------------------------------------------------------------
# マスタデータ読み込み
# ---------------------------------------------------------------------------

def load_master_companies(master_path: Path) -> list[dict]:
    """マスタCSV/SQLiteから企業リストを読み込む。

    master_path が .db → SQLiteのcompaniesテーブル
    master_path が .csv → CSVファイル（company_id, official_name列必須）
    """
    if not master_path.exists():
        logger.warning("マスタファイルが存在しません: %s", master_path)
        return []

    if master_path.suffix == ".db":
        conn = sqlite3.connect(str(master_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT company_id, official_name FROM companies"
        ).fetchall()
        conn.close()
        return [{"company_id": r["company_id"], "official_name": r["official_name"]} for r in rows]

    if master_path.suffix == ".csv":
        results: list[dict] = []
        with master_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                cid = row.get("company_id", "").strip()
                name = row.get("official_name", "").strip()
                if cid and name:
                    results.append({"company_id": cid, "official_name": name})
        return results

    logger.warning("未対応のマスタ形式: %s", master_path.suffix)
    return []


def load_email_map(email_map_path: Path) -> dict[str, str]:
    """メールアドレスマップを読み込む。

    email_map_path が .db → SQLiteのcompany_emailsテーブル
    email_map_path が .csv → CSVファイル（email, company_id列必須）

    Returns:
        {email(小文字): company_id} の辞書
    """
    if not email_map_path.exists():
        logger.warning("メールマップファイルが存在しません: %s", email_map_path)
        return {}

    if email_map_path.suffix == ".db":
        conn = sqlite3.connect(str(email_map_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT email, company_id FROM company_emails").fetchall()
        conn.close()
        return {r["email"].strip().lower(): r["company_id"] for r in rows}

    if email_map_path.suffix == ".csv":
        result: dict[str, str] = {}
        with email_map_path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                email = row.get("email", "").strip().lower()
                cid = row.get("company_id", "").strip()
                if email and cid:
                    result[email] = cid
        return result

    logger.warning("未対応のメールマップ形式: %s", email_map_path.suffix)
    return {}


# ---------------------------------------------------------------------------
# バッチ突合
# ---------------------------------------------------------------------------

def reconcile_batch(
    inbound_records: list[dict],
    master_path: Path,
    email_map_path: Path,
) -> tuple[list[ReconcileResult], list[dict]]:
    """バッチ突合。マッチした結果と未マッチレコードを返す。

    各レコードに対して:
      1. sender_email があれば reconcile_by_email を試行
      2. company_name があれば reconcile_by_name を試行
      3. いずれもマッチしなければ未マッチ

    Args:
        inbound_records: [{"sender_email": ..., "company_name": ..., ...}, ...]
        master_path: マスタファイルパス (.db or .csv)
        email_map_path: メールマップファイルパス (.db or .csv)

    Returns:
        (matched_results, unmatched_records) のタプル
    """
    master_companies = load_master_companies(master_path)
    email_map = load_email_map(email_map_path)

    matched_results: list[ReconcileResult] = []
    unmatched_records: list[dict] = []

    for record in inbound_records:
        sender_email = record.get("sender_email", "")
        company_name = record.get("company_name", "")

        result: Optional[ReconcileResult] = None

        # Step 1: メールアドレス完全一致
        if sender_email:
            email_result = reconcile_by_email(sender_email, email_map)
            if email_result.match_method == "exact_email":
                result = email_result

        # Step 2: 会社名完全一致（メールでマッチしなかった場合）
        if result is None and company_name:
            name_result = reconcile_by_name(company_name, master_companies)
            if name_result.match_method == "exact_name":
                result = name_result

        if result is not None:
            matched_results.append(result)
        else:
            # 未マッチ: 最後の試行結果のwarningsを付与
            warnings: list[str] = []
            if sender_email:
                warnings.append(f"email未マッチ: {_normalize_email(sender_email)}")
            if company_name:
                warnings.append(f"name未マッチ: {company_name}")
            unmatched_result = ReconcileResult(
                company_id=None,
                official_name="",
                match_method="unmatched",
                confidence=0.0,
                warnings=warnings,
            )
            matched_results.append(unmatched_result)
            unmatched_records.append(record)

    return matched_results, unmatched_records


# ---------------------------------------------------------------------------
# レポート生成
# ---------------------------------------------------------------------------

def generate_reconcile_report(results: list[ReconcileResult]) -> dict:
    """突合結果のサマリーレポートを生成。

    Returns:
        {
            "total": int,
            "matched": int,
            "unmatched": int,
            "by_method": {"exact_email": int, "exact_name": int, ...},
            "avg_confidence": float,
            "warnings": list[str],
        }
    """
    total = len(results)
    matched = sum(1 for r in results if r.match_method != "unmatched")
    unmatched = total - matched

    by_method: dict[str, int] = {}
    all_warnings: list[str] = []
    confidence_sum = 0.0

    for r in results:
        by_method[r.match_method] = by_method.get(r.match_method, 0) + 1
        confidence_sum += r.confidence
        all_warnings.extend(r.warnings)

    avg_confidence = confidence_sum / total if total > 0 else 0.0

    return {
        "total": total,
        "matched": matched,
        "unmatched": unmatched,
        "by_method": by_method,
        "avg_confidence": round(avg_confidence, 4),
        "warnings": all_warnings,
    }
