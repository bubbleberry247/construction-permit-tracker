"""
backfill_sender_emails.py — 既存メールから転送元メアドを抽出し email_company_map を生成する。

inbound_log.csv の全 message_id を使って Gmail API でメッセージを再取得し、
転送メール本文から元の送信者メールアドレスを抽出する。
抽出結果を email_company_map.csv に書き出し、inbound_log.csv にも
original_sender_email カラムをバックフィルする。

Usage:
    python src/backfill_sender_emails.py            # 全message_id処理
    python src/backfill_sender_emails.py --dry-run   # 抽出結果の表示のみ
    python src/backfill_sender_emails.py --max 10    # 最大10件
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import tempfile
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# プロジェクト内モジュール
# ---------------------------------------------------------------------------
_SRC_DIR = Path(__file__).parent
_PROJECT_ROOT = _SRC_DIR.parent

# fetch_gmail.py から認証関連を再利用
from fetch_gmail import CONFIG_PATH, get_gmail_service, load_config

# email_utils.py から本文・送信者抽出を再利用
from utils.email_utils import extract_body_text, extract_original_sender

# ---------------------------------------------------------------------------
# ロギング
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 会社名抽出（ファイル名から）
# ---------------------------------------------------------------------------
def extract_company_from_filename(file_name: str) -> str:
    """ファイル名から会社名を抽出する。

    例:
        "御取引条件等説明書_株式会社井上商会.PDF" -> "株式会社井上商会"
        "「御取引条件等説明書＿株式会社井上商会.PDF」.pdf" -> "株式会社井上商会"
        "御取引条件等説明書　福釜電工㈱.pdf" -> "福釜電工㈱"

    Args:
        file_name: 添付ファイル名。
    Returns:
        抽出された会社名。抽出できない場合は空文字列。
    """
    # ファイル名のクリーニング: 拡張子除去、括弧除去
    name = file_name
    # .pdf, .PDF 等を除去（二重拡張子も対応）
    name = re.sub(r'\.pdf$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\.pdf$', '', name, flags=re.IGNORECASE)
    # 「」を除去
    name = name.replace('「', '').replace('」', '')

    # パターン1: "_" or "＿" の後の会社名
    patterns = [
        r'[_＿](.+?)$',           # アンダースコア区切り
        r'説明書[　\s]+(.+?)$',    # "説明書" + 空白の後
        r'御取引条件等説明書[_＿　\s]*(.+?)$',  # 最も具体的
    ]

    for pattern in patterns:
        match = re.search(pattern, name)
        if match:
            company = match.group(1).strip()
            if company:
                return company

    return ""


# ---------------------------------------------------------------------------
# 会社名正規化（マッチング用）
# ---------------------------------------------------------------------------
def normalize_company_name(name: str) -> str:
    """会社名を正規化する（マッチング用）。

    NFKC正規化 + 空白除去 + 法人格略称を正式名称に展開。
    """
    if not name:
        return ""
    # NFKC正規化（全角英数→半角、㈱→(株) 等）
    normalized = unicodedata.normalize("NFKC", name)
    # 空白除去
    normalized = re.sub(r'\s+', '', normalized)
    # 法人格の表記ゆれを統一
    normalized = normalized.replace('(株)', '株式会社')
    normalized = normalized.replace('(有)', '有限会社')
    normalized = normalized.replace('(合)', '合同会社')
    return normalized


# ---------------------------------------------------------------------------
# 会社マスタ読み込み
# ---------------------------------------------------------------------------
def load_company_master(master_path: Path) -> list[dict[str, Any]]:
    """company_master.csv を読み込む。

    Returns:
        各行の辞書リスト。name_aliases はリストに分割済み。
    """
    companies: list[dict[str, Any]] = []
    if not master_path.exists():
        logger.warning("会社マスタが見つかりません: %s", master_path)
        return companies

    with master_path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            aliases_raw = row.get("name_aliases", "")
            aliases = [a.strip() for a in aliases_raw.split("|") if a.strip()] if aliases_raw else []
            companies.append({
                "company_id": row.get("company_id", ""),
                "official_name": row.get("official_name", ""),
                "name_aliases": aliases,
            })
    return companies


def match_company(
    extracted_name: str, companies: list[dict[str, Any]]
) -> str:
    """抽出された会社名を会社マスタとマッチングする。

    Args:
        extracted_name: ファイル名等から抽出された会社名。
        companies: load_company_master() の戻り値。
    Returns:
        マッチした company_id。見つからない場合は空文字列。
    """
    if not extracted_name:
        return ""

    norm_input = normalize_company_name(extracted_name)
    if not norm_input:
        return ""

    for company in companies:
        # official_name との完全一致
        norm_official = normalize_company_name(company["official_name"])
        if norm_input == norm_official:
            return company["company_id"]

        # name_aliases との完全一致
        for alias in company["name_aliases"]:
            norm_alias = normalize_company_name(alias)
            if norm_input == norm_alias:
                return company["company_id"]

        # 部分一致: 入力が official_name を含む、または逆
        if norm_input in norm_official or norm_official in norm_input:
            return company["company_id"]

    return ""


# ---------------------------------------------------------------------------
# inbound_log.csv 読み込み
# ---------------------------------------------------------------------------
def load_inbound_log(log_path: Path) -> list[dict[str, str]]:
    """inbound_log.csv を全行読み込む。"""
    rows: list[dict[str, str]] = []
    if not log_path.exists():
        logger.error("inbound_log.csv が見つかりません: %s", log_path)
        return rows

    with log_path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


# ---------------------------------------------------------------------------
# Gmail メッセージ取得 → 送信者抽出
# ---------------------------------------------------------------------------
def fetch_and_extract_sender(
    service: Any, message_id: str
) -> tuple[str, str]:
    """Gmail API でメッセージを取得し、転送元送信者を抽出する。

    Args:
        service: Gmail API サービスオブジェクト。
        message_id: Gmail メッセージID。
    Returns:
        (display_name, email_address) タプル。
    """
    msg = service.users().messages().get(
        userId="me", id=message_id, format="full"
    ).execute()

    payload = msg.get("payload", {})

    # From ヘッダ取得
    headers = {
        h["name"].lower(): h["value"]
        for h in payload.get("headers", [])
    }
    from_header = headers.get("from", "")

    # 本文テキスト抽出
    body_text = extract_body_text(payload)

    # 転送元送信者抽出
    display_name, email_address = extract_original_sender(body_text, from_header)

    return display_name, email_address


# ---------------------------------------------------------------------------
# CSV 安全書き込み（temp + rename）
# ---------------------------------------------------------------------------
def safe_write_csv(
    path: Path,
    headers: list[str],
    rows: list[dict[str, str]],
) -> None:
    """一時ファイルに書き込み後、原子的にリネームする。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        suffix=".csv", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        # Windows: 上書き先が存在する場合は先に削除
        if path.exists():
            path.unlink()
        Path(tmp_path).rename(path)
    except Exception:
        # クリーンアップ
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------
def run_backfill(
    service: Any,
    log_path: Path,
    master_path: Path,
    email_map_path: Path,
    max_messages: int | None,
    dry_run: bool,
) -> dict[str, Any]:
    """バックフィル処理を実行する。

    Returns:
        処理結果の統計情報。
    """
    # 1. inbound_log.csv 読み込み
    log_rows = load_inbound_log(log_path)
    if not log_rows:
        logger.error("inbound_log.csv にデータがありません")
        return {"total": 0, "extracted": 0, "matched": 0}

    # 2. ユニークな message_id を収集（出現順を保持）
    seen_ids: set[str] = set()
    unique_message_ids: list[str] = []
    for row in log_rows:
        mid = row.get("message_id", "").strip()
        if mid and mid not in seen_ids:
            seen_ids.add(mid)
            unique_message_ids.append(mid)

    if max_messages:
        unique_message_ids = unique_message_ids[:max_messages]

    total = len(unique_message_ids)
    logger.info("処理対象メッセージ: %d件", total)

    # 3. 会社マスタ読み込み
    companies = load_company_master(master_path)
    logger.info("会社マスタ: %d社", len(companies))

    # 4. ファイル名から会社名を先に抽出（message_id → file_names マップ）
    mid_to_filenames: dict[str, list[str]] = {}
    for row in log_rows:
        mid = row.get("message_id", "").strip()
        fname = row.get("file_name", "").strip()
        if mid and fname:
            mid_to_filenames.setdefault(mid, []).append(fname)

    # 5. 各 message_id を処理
    # sender_email -> 抽出情報（重複排除用）
    email_map: dict[str, dict[str, str]] = {}
    # message_id -> original_sender_email（inbound_log バックフィル用）
    mid_to_sender: dict[str, str] = {}

    stats = {"total": total, "extracted": 0, "matched": 0, "skipped_forwarding": 0}

    for i, message_id in enumerate(unique_message_ids, start=1):
        # ファイル名から会社名を先に抽出
        filenames = mid_to_filenames.get(message_id, [])
        company_from_filename = ""
        for fname in filenames:
            company_from_filename = extract_company_from_filename(fname)
            if company_from_filename:
                break

        # Gmail API からメッセージ取得
        try:
            display_name, email_address = fetch_and_extract_sender(
                service, message_id
            )
        except Exception as e:
            logger.error(
                "[%d/%d] message_id=%s API取得失敗: %s",
                i, total, message_id, e,
            )
            continue

        # 転送アカウント自身のアドレスはスキップ
        email_lower = email_address.lower() if email_address else ""
        is_forwarding_account = email_lower in (
            "shinsei.tic@gmail.com",
            "kalimistk@gmail.com",
        )

        if is_forwarding_account or not email_address:
            stats["skipped_forwarding"] += 1
            original_email = ""
        else:
            original_email = email_lower
            stats["extracted"] += 1

        mid_to_sender[message_id] = original_email

        # 会社マッチング
        company_id = ""
        confidence = "LOW"
        if original_email and company_from_filename:
            company_id = match_company(company_from_filename, companies)
            confidence = "HIGH" if company_id else "MEDIUM"
        elif original_email:
            confidence = "MEDIUM"
        elif company_from_filename:
            company_id = match_company(company_from_filename, companies)
            confidence = "LOW"

        logger.info(
            "[%d/%d] message_id=%s original_sender=%s company=%s (from_file=%s) confidence=%s",
            i, total, message_id[:12],
            original_email or "(none)",
            company_id or "(unmatched)",
            company_from_filename or "(none)",
            confidence,
        )

        if company_id:
            stats["matched"] += 1

        # email_map に追加（重複排除: 同じメアドは1回だけ）
        if original_email and original_email not in email_map:
            email_map[original_email] = {
                "sender_email": original_email,
                "company_id": company_id,
                "match_source": "auto_body_extract",
                "confidence": confidence,
                "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            }

        # レート制限
        if i < total:
            time.sleep(0.5)

    # 6. 結果出力
    logger.info(
        "--- 結果 --- 合計=%d, メアド抽出=%d, 会社マッチ=%d, 転送元のみ=%d",
        stats["total"], stats["extracted"], stats["matched"], stats["skipped_forwarding"],
    )

    if dry_run:
        logger.info("[DRY-RUN] ファイル書き込みをスキップします")
        for email, info in sorted(email_map.items()):
            logger.info(
                "  %s -> company_id=%s confidence=%s",
                email, info["company_id"] or "(empty)", info["confidence"],
            )
        return stats

    # 7. email_company_map.csv 書き出し
    email_map_headers = ["sender_email", "company_id", "match_source", "confidence", "created_at"]
    email_map_rows = list(email_map.values())
    safe_write_csv(email_map_path, email_map_headers, email_map_rows)
    logger.info("email_company_map.csv 書き出し完了: %d件 -> %s", len(email_map_rows), email_map_path)

    # 8. inbound_log.csv にバックフィル（original_sender_email カラム追加）
    backfill_inbound_log(log_path, log_rows, mid_to_sender)

    return stats


def backfill_inbound_log(
    log_path: Path,
    log_rows: list[dict[str, str]],
    mid_to_sender: dict[str, str],
) -> None:
    """inbound_log.csv に original_sender_email カラムを追加する。"""
    if not log_rows:
        return

    # 既存ヘッダ + 新カラム
    existing_headers = list(log_rows[0].keys())
    if "original_sender_email" not in existing_headers:
        headers = existing_headers + ["original_sender_email"]
    else:
        headers = existing_headers

    for row in log_rows:
        mid = row.get("message_id", "").strip()
        if mid in mid_to_sender:
            row["original_sender_email"] = mid_to_sender[mid]
        elif "original_sender_email" not in row:
            row["original_sender_email"] = ""

    safe_write_csv(log_path, headers, log_rows)
    logger.info("inbound_log.csv バックフィル完了: original_sender_email カラム追加")


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="既存メールから転送元メアドを抽出し email_company_map を生成する"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="抽出結果の表示のみ（ファイル書き込みしない）",
    )
    parser.add_argument(
        "--max", type=int, default=None,
        help="最大処理メッセージ数",
    )
    args = parser.parse_args()

    cfg = load_config()

    log_level = getattr(logging, cfg.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.getLogger().setLevel(log_level)

    data_root = Path(cfg["DATA_ROOT"])
    log_path = data_root / "logs" / "inbound_log.csv"
    master_path = data_root / "output" / "company_master.csv"
    email_map_path = data_root / "output" / "email_company_map.csv"
    token_cache = data_root / "logs" / ".gmail_token.json"
    credentials_file = cfg["GOOGLE_CREDENTIALS_FILE"]

    # Gmail 認証
    logger.info("Gmail 認証を開始します...")
    service = get_gmail_service(credentials_file, token_cache)
    logger.info("Gmail 認証完了")

    # バックフィル実行
    stats = run_backfill(
        service=service,
        log_path=log_path,
        master_path=master_path,
        email_map_path=email_map_path,
        max_messages=args.max,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        logger.info("[DRY-RUN] 完了")
    else:
        logger.info(
            "完了: メアド抽出=%d, 会社マッチ=%d",
            stats["extracted"], stats["matched"],
        )


if __name__ == "__main__":
    main()
