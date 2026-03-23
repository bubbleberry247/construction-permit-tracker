"""
fetch_gmail.py
Gmail から許可証PDF添付ファイルを自動受信し data/inbox/ に保存する。

処理フロー:
  Gmail API 認証 → 未処理メール検索 → 添付PDF保存 → InboundLog記録
  → ラベル「許可証処理済み」付与

Usage:
    python src/fetch_gmail.py            # 通常実行（GMAIL_FETCH_MAX件取得）
    python src/fetch_gmail.py --dry-run  # 取得対象の表示のみ
    python src/fetch_gmail.py --max 10   # 最大10件取得
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------
_SRC_DIR = Path(__file__).parent
_PROJECT_ROOT = _SRC_DIR.parent
CONFIG_PATH = _PROJECT_ROOT / "config.json"

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",  # 読み取り + ラベル付与
]

# InboundLog CSV カラム定義
INBOUND_LOG_HEADERS = [
    "log_id", "message_id", "attachment_id",
    "received_at", "sender_email",
    "file_name", "file_hash", "file_size_bytes",
    "saved_path", "process_status",
    "process_started_at", "process_finished_at",
    "error_message", "staging_csv_path", "created_at",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 設定読み込み
# ---------------------------------------------------------------------------
def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open(encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# OAuth2 認証
# ---------------------------------------------------------------------------
def get_gmail_service(credentials_file: str, token_cache: Path):
    """Gmail API サービスを取得する。初回はブラウザで認証、以降はトークンキャッシュを使用。"""
    creds: Credentials | None = None

    if token_cache.exists():
        creds = Credentials.from_authorized_user_file(str(token_cache), GMAIL_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Gmailトークンをリフレッシュしています...")
            creds.refresh(Request())
        else:
            logger.info("Gmail OAuth2 認証を開始します（ブラウザが開きます）...")
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)

        token_cache.parent.mkdir(parents=True, exist_ok=True)
        token_cache.write_text(creds.to_json(), encoding="utf-8")
        logger.info("トークンをキャッシュしました: %s", token_cache)

    return build("gmail", "v1", credentials=creds)


# ---------------------------------------------------------------------------
# ラベル操作
# ---------------------------------------------------------------------------
def get_or_create_label(service, label_name: str) -> str:
    """ラベルIDを返す。存在しない場合は作成する。"""
    result = service.users().labels().list(userId="me").execute()
    for label in result.get("labels", []):
        if label["name"] == label_name:
            return label["id"]

    # 新規作成
    created = service.users().labels().create(
        userId="me",
        body={"name": label_name, "labelListVisibility": "labelShow", "messageListVisibility": "show"},
    ).execute()
    logger.info("Gmailラベル作成: %s (id=%s)", label_name, created["id"])
    return created["id"]


# ---------------------------------------------------------------------------
# InboundLog
# ---------------------------------------------------------------------------
def load_inbound_log(log_path: Path) -> set[tuple[str, str]]:
    """処理済みの (message_id, attachment_id) ペアを返す。"""
    processed: set[tuple[str, str]] = set()
    if not log_path.exists():
        return processed
    with log_path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            mid = row.get("message_id", "").strip()
            aid = row.get("attachment_id", "").strip()
            if mid and aid:
                processed.add((mid, aid))
    return processed


def append_inbound_log(log_path: Path, record: dict[str, Any]) -> None:
    """InboundLog CSV に1行追記する。ファイルが存在しない場合はヘッダ付きで新規作成。"""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not log_path.exists()
    with log_path.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=INBOUND_LOG_HEADERS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        row = {h: record.get(h, "") for h in INBOUND_LOG_HEADERS}
        writer.writerow(row)


# ---------------------------------------------------------------------------
# ファイル名サニタイズ
# ---------------------------------------------------------------------------
def sanitize_filename(name: str) -> str:
    """ファイル名から危険な文字を除去する。"""
    safe = re.sub(r'[\\/:*?"<>|]', "_", name)
    return safe.strip(". ")[:200] or "attachment.pdf"


# ---------------------------------------------------------------------------
# SHA256
# ---------------------------------------------------------------------------
def sha256_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# UUID簡易生成
# ---------------------------------------------------------------------------
def _new_id() -> str:
    import uuid
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------
def fetch_pdf_attachments(
    service,
    inbox_dir: Path,
    log_path: Path,
    label_id: str,
    max_messages: int,
    dry_run: bool,
) -> int:
    """
    Gmail から未処理の PDF 添付ファイルを取得して inbox/ に保存する。
    Returns: 保存したファイル数
    """
    processed_pairs = load_inbound_log(log_path)
    logger.info("既処理ペア数: %d", len(processed_pairs))

    # 未処理メール検索（PDFを含む、処理済みラベルなし）
    query = f'has:attachment filename:.pdf -label:"{label_id}"'
    response = service.users().messages().list(
        userId="me",
        q='has:attachment filename:.pdf',
        maxResults=max_messages,
    ).execute()
    messages = response.get("messages", [])

    if not messages:
        logger.info("対象メールなし")
        return 0

    logger.info("対象メール: %d件", len(messages))

    saved_count = 0
    inbox_dir.mkdir(parents=True, exist_ok=True)

    for msg_meta in messages:
        message_id = msg_meta["id"]

        # メッセージ詳細取得
        msg = service.users().messages().get(
            userId="me", id=message_id, format="full"
        ).execute()

        # 送信者・受信日時
        headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
        sender_email = headers.get("from", "")
        internal_date_ms = int(msg.get("internalDate", 0))
        received_at = datetime.fromtimestamp(internal_date_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")

        # 添付ファイル探索
        pdf_parts = _find_pdf_parts(msg.get("payload", {}))
        if not pdf_parts:
            continue

        any_new = False
        for part in pdf_parts:
            attachment_id = part.get("body", {}).get("attachmentId", "")
            file_name = sanitize_filename(part.get("filename", "attachment.pdf"))

            if not file_name.lower().endswith(".pdf"):
                continue

            # 冪等チェック（一次判定: message_id + attachment_id）
            if (message_id, attachment_id) in processed_pairs:
                logger.debug("SKIP (既処理): %s / %s", message_id, attachment_id)
                continue

            if dry_run:
                logger.info("[DRY-RUN] %s → %s / %s", file_name, sender_email, received_at)
                any_new = True
                continue

            # 添付データ取得
            try:
                att = service.users().messages().attachments().get(
                    userId="me", messageId=message_id, id=attachment_id
                ).execute()
                data = base64.urlsafe_b64decode(att["data"])
            except Exception as e:
                logger.error("添付取得失敗 [%s/%s]: %s", message_id, attachment_id, e)
                append_inbound_log(log_path, {
                    "log_id": _new_id(), "message_id": message_id, "attachment_id": attachment_id,
                    "received_at": received_at, "sender_email": sender_email,
                    "file_name": file_name, "file_hash": "", "file_size_bytes": 0,
                    "saved_path": "", "process_status": "FAILED",
                    "error_message": str(e), "created_at": _now(),
                })
                continue

            file_hash = sha256_of_bytes(data)
            file_size = len(data)

            # 保存先: inbox/{YYYYMMDD_HHMMSS}_{filename}
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_name = f"{ts}_{file_name}"
            save_path = inbox_dir / unique_name
            # 同名衝突回避
            stem, suffix = Path(unique_name).stem, Path(unique_name).suffix
            counter = 1
            while save_path.exists():
                save_path = inbox_dir / f"{stem}_{counter}{suffix}"
                counter += 1

            save_path.write_bytes(data)
            logger.info("保存: %s (%d bytes, hash=%s...)", save_path.name, file_size, file_hash[:8])

            append_inbound_log(log_path, {
                "log_id": _new_id(), "message_id": message_id, "attachment_id": attachment_id,
                "received_at": received_at, "sender_email": sender_email,
                "file_name": file_name, "file_hash": file_hash, "file_size_bytes": file_size,
                "saved_path": str(save_path), "process_status": "PENDING",
                "created_at": _now(),
            })

            processed_pairs.add((message_id, attachment_id))
            saved_count += 1
            any_new = True

        # 1件でも新規添付があった場合にのみ処理済みラベルを付与
        if any_new and not dry_run:
            try:
                service.users().messages().modify(
                    userId="me",
                    id=message_id,
                    body={"addLabelIds": [label_id], "removeLabelIds": ["UNREAD"]},
                ).execute()
                logger.debug("ラベル付与: %s", message_id)
            except Exception as e:
                logger.warning("ラベル付与失敗 [%s]: %s", message_id, e)

    return saved_count


def _find_pdf_parts(payload: dict) -> list[dict]:
    """メッセージ payload から PDF パートを再帰的に探す。"""
    parts: list[dict] = []
    if payload.get("filename", "").lower().endswith(".pdf") and payload.get("body", {}).get("attachmentId"):
        parts.append(payload)
    for child in payload.get("parts", []):
        parts.extend(_find_pdf_parts(child))
    return parts


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Gmail から許可証PDF添付を自動受信する")
    parser.add_argument("--dry-run", action="store_true", help="取得対象の表示のみ（保存しない）")
    parser.add_argument("--max", type=int, default=None, help="最大取得件数（省略時はconfig.jsonのGMAIL_FETCH_MAX）")
    args = parser.parse_args()

    cfg = load_config()

    log_level = getattr(logging, cfg.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.getLogger().setLevel(log_level)

    data_root = Path(cfg["DATA_ROOT"])
    inbox_dir = data_root / "data" / "inbox"
    log_path = data_root / "logs" / "inbound_log.csv"
    token_cache = data_root / "logs" / ".gmail_token.json"
    credentials_file = cfg["GOOGLE_CREDENTIALS_FILE"]
    label_name = cfg.get("GMAIL_LABEL_PROCESSED", "許可証処理済み")
    max_messages = args.max or cfg.get("GMAIL_FETCH_MAX", 50)

    # 認証
    service = get_gmail_service(credentials_file, token_cache)

    # ラベル確認・作成
    label_id = get_or_create_label(service, label_name)

    # PDF取得
    saved = fetch_pdf_attachments(
        service=service,
        inbox_dir=inbox_dir,
        log_path=log_path,
        label_id=label_id,
        max_messages=max_messages,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        logger.info("[DRY-RUN] 完了")
    else:
        logger.info("完了: %d件保存 → %s", saved, inbox_dir)
        if saved > 0:
            logger.info("次: python src/ocr_permit.py")


if __name__ == "__main__":
    main()
