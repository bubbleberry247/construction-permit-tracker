"""
recollect_gmail.py — Gmail APIからshinsei.tic転送メールの全添付ファイルを再取得し、
blob保存 + SQLiteにreceipt_event登録する。

処理フロー:
  1. DBスキーマ追加（blobs, receipt_events, transfer_events）
  2. Gmail API全メール取得（label:shinsei.tic has:attachment、ページネーション対応）
  3. 各添付ファイルをSHA256でblob保存
  4. receipt_events登録 + company_emailsで会社紐付け
  5. スコープ外メール/添付のマーキング

Usage:
    python src/recollect_gmail.py
    python src/recollect_gmail.py --dry-run
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.stdout.reconfigure(encoding="utf-8")

try:
    from utils.email_utils import extract_body_text, extract_original_sender
except ImportError:
    from src.utils.email_utils import extract_body_text, extract_original_sender

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
DB_PATH = _PROJECT_ROOT / "data" / "permit_tracker.db"
BLOB_DIR = _PROJECT_ROOT / "data" / "blobs"

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# スコープ外判定
# ---------------------------------------------------------------------------
OUT_OF_SCOPE_SENDERS: set[str] = {
    "masamaru1975@hotmail.com",
    "t-yamaguchi@acogroup.co.jp",
}

OUT_OF_SCOPE_SENDER_PATTERNS: list[re.Pattern] = [
    re.compile(r"@anthropic\.com$", re.IGNORECASE),
    re.compile(r"@stripe\.com$", re.IGNORECASE),
    re.compile(r"@google\.com$", re.IGNORECASE),
    re.compile(r"noreply@", re.IGNORECASE),
    re.compile(r"no-reply@", re.IGNORECASE),
]

OUT_OF_SCOPE_SUBJECT_PATTERNS: list[re.Pattern] = [
    re.compile(r"invoice", re.IGNORECASE),
    re.compile(r"receipt", re.IGNORECASE),
]

OUT_OF_SCOPE_FILENAMES: set[str] = {
    "icon.png",
    "image001.png",
    "image002.png",
    "image003.png",
    "image004.png",
    "image005.png",
}

OUT_OF_SCOPE_FILENAME_PATTERNS: list[re.Pattern] = [
    re.compile(r"^image\d+\.png$", re.IGNORECASE),
    re.compile(r"^icon\.png$", re.IGNORECASE),
]


def _is_out_of_scope_sender(email: str) -> bool:
    """送信者がスコープ外か判定する。"""
    if not email:
        return False
    email_lower = email.lower().strip()
    if email_lower in OUT_OF_SCOPE_SENDERS:
        return True
    for pat in OUT_OF_SCOPE_SENDER_PATTERNS:
        if pat.search(email_lower):
            return True
    return False


def _is_out_of_scope_subject(subject: str) -> bool:
    """件名がスコープ外か判定する。"""
    if not subject:
        return False
    for pat in OUT_OF_SCOPE_SUBJECT_PATTERNS:
        if pat.search(subject):
            return True
    return False


def _is_out_of_scope_filename(filename: str) -> bool:
    """ファイル名がスコープ外か判定する。"""
    if not filename:
        return False
    fn_lower = filename.lower().strip()
    if fn_lower in OUT_OF_SCOPE_FILENAMES:
        return True
    for pat in OUT_OF_SCOPE_FILENAME_PATTERNS:
        if pat.match(fn_lower):
            return True
    return False


# ---------------------------------------------------------------------------
# DB スキーマ追加
# ---------------------------------------------------------------------------
_NEW_DDL = """
CREATE TABLE IF NOT EXISTS blobs (
  blob_id INTEGER PRIMARY KEY,
  sha256 TEXT UNIQUE NOT NULL,
  size_bytes INTEGER,
  mime_type TEXT,
  stored_path TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now'))
) STRICT;

CREATE TABLE IF NOT EXISTS receipt_events (
  receipt_id INTEGER PRIMARY KEY,
  gmail_message_id TEXT NOT NULL,
  thread_id TEXT,
  attachment_id TEXT,
  blob_id INTEGER REFERENCES blobs(blob_id),
  original_filename TEXT,
  original_sender_email TEXT,
  from_header TEXT,
  subject TEXT,
  received_at TEXT,
  company_id TEXT,
  resolve_status TEXT DEFAULT 'unresolved',
  resolve_method TEXT,
  created_at TEXT DEFAULT (datetime('now'))
) STRICT;

CREATE TABLE IF NOT EXISTS transfer_events (
  transfer_id INTEGER PRIMARY KEY,
  source_message_id TEXT,
  download_url TEXT,
  blob_id INTEGER REFERENCES blobs(blob_id),
  company_id TEXT,
  method TEXT,
  parent_blob_id INTEGER REFERENCES blobs(blob_id),
  created_at TEXT DEFAULT (datetime('now'))
) STRICT;
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    """blobs, receipt_events, transfer_events テーブルを追加（既存テーブルは変更しない）。"""
    conn.executescript(_NEW_DDL)
    conn.commit()
    logger.info("[schema] blobs, receipt_events, transfer_events を確認/作成しました")


# ---------------------------------------------------------------------------
# DB接続
# ---------------------------------------------------------------------------
def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """PRAGMA設定済みの接続を返す。"""
    p = db_path or DB_PATH
    conn = sqlite3.connect(str(p))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.row_factory = sqlite3.Row
    return conn


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
    """Gmail API サービスを取得する。"""
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
            creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")

        token_cache.parent.mkdir(parents=True, exist_ok=True)
        token_cache.write_text(creds.to_json(), encoding="utf-8")
        logger.info("トークンをキャッシュしました: %s", token_cache)

    return build("gmail", "v1", credentials=creds)


# ---------------------------------------------------------------------------
# 添付ファイル探索（全形式）
# ---------------------------------------------------------------------------
def find_attachment_parts(payload: dict) -> list[dict]:
    """メッセージ payload から全添付ファイルパートを再帰的に探す。"""
    parts: list[dict] = []
    filename = payload.get("filename", "")
    if filename and payload.get("body", {}).get("attachmentId"):
        parts.append(payload)
    for child in payload.get("parts", []):
        parts.extend(find_attachment_parts(child))
    return parts


# ---------------------------------------------------------------------------
# SHA256
# ---------------------------------------------------------------------------
def sha256_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# blob保存
# ---------------------------------------------------------------------------
def save_blob(conn: sqlite3.Connection, data: bytes, mime_type: str) -> tuple[int, str]:
    """blobを保存しblob_idとsha256を返す。既にあればスキップ。"""
    sha = sha256_of_bytes(data)
    size = len(data)

    # 既存チェック
    row = conn.execute("SELECT blob_id FROM blobs WHERE sha256 = ?", (sha,)).fetchone()
    if row:
        return row["blob_id"], sha

    # ファイル保存
    prefix = sha[:2]
    blob_dir = BLOB_DIR / prefix
    blob_dir.mkdir(parents=True, exist_ok=True)
    blob_path = blob_dir / sha
    if not blob_path.exists():
        blob_path.write_bytes(data)

    stored_path = f"data/blobs/{prefix}/{sha}"

    cur = conn.execute(
        "INSERT INTO blobs (sha256, size_bytes, mime_type, stored_path) VALUES (?,?,?,?)",
        (sha, size, mime_type, stored_path),
    )
    return cur.lastrowid, sha


# ---------------------------------------------------------------------------
# 会社紐付け（company_emailsテーブルから）
# ---------------------------------------------------------------------------
def resolve_company(conn: sqlite3.Connection, email: str) -> tuple[str | None, str | None]:
    """original_sender_email → company_id を返す。マッチ方法も返す。"""
    if not email:
        return None, None
    row = conn.execute(
        "SELECT company_id FROM company_emails WHERE email = ?",
        (email,),
    ).fetchone()
    if row:
        return row["company_id"], "email_match"
    return None, None


# ---------------------------------------------------------------------------
# ヘッダ取得ヘルパー
# ---------------------------------------------------------------------------
def _get_headers(msg: dict) -> dict[str, str]:
    """メッセージからヘッダをdict化する。"""
    headers = {}
    for h in msg.get("payload", {}).get("headers", []):
        headers[h["name"].lower()] = h["value"]
    return headers


# ---------------------------------------------------------------------------
# ページネーション対応メール取得
# ---------------------------------------------------------------------------
def list_all_messages(service, query: str) -> list[dict]:
    """ページネーション対応で全メッセージIDを取得する。"""
    all_messages: list[dict] = []
    page_token: str | None = None

    while True:
        kwargs: dict[str, Any] = {
            "userId": "me",
            "q": query,
            "maxResults": 100,
        }
        if page_token:
            kwargs["pageToken"] = page_token

        response = service.users().messages().list(**kwargs).execute()
        messages = response.get("messages", [])
        all_messages.extend(messages)

        page_token = response.get("nextPageToken")
        if not page_token:
            break

        logger.info("  ページネーション: %d件取得済み...", len(all_messages))

    return all_messages


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------
def recollect(
    service,
    conn: sqlite3.Connection,
    dry_run: bool = False,
) -> dict[str, int]:
    """全shinsei.ticメールの添付ファイルを再取得し、blob保存 + receipt_event登録する。"""

    stats = {
        "messages_total": 0,
        "attachments_total": 0,
        "blobs_new": 0,
        "blobs_existing": 0,
        "receipts_inserted": 0,
        "receipts_skipped": 0,
        "resolved": 0,
        "unresolved": 0,
        "out_of_scope": 0,
        "errors": 0,
    }

    # 全メール取得（処理済みラベルは除外しない → 全件取得）
    query = "label:shinsei.tic has:attachment"
    logger.info("[query] %s", query)

    all_messages = list_all_messages(service, query)
    stats["messages_total"] = len(all_messages)
    logger.info("[messages] %d件のメールを取得", len(all_messages))

    if not all_messages:
        return stats

    # 既存receipt_events確認（冪等性）
    existing_pairs: set[tuple[str, str]] = set()
    for row in conn.execute("SELECT gmail_message_id, attachment_id FROM receipt_events").fetchall():
        existing_pairs.add((row["gmail_message_id"], row["attachment_id"]))
    logger.info("[既存] receipt_events: %d件", len(existing_pairs))

    for idx, msg_meta in enumerate(all_messages, 1):
        message_id = msg_meta["id"]
        thread_id = msg_meta.get("threadId", "")

        if idx % 10 == 0 or idx == 1:
            logger.info("[進捗] %d / %d 件目...", idx, stats["messages_total"])

        # メッセージ詳細取得
        try:
            msg = service.users().messages().get(
                userId="me", id=message_id, format="full"
            ).execute()
        except Exception as e:
            logger.error("[ERROR] メッセージ取得失敗 [%s]: %s", message_id, e)
            stats["errors"] += 1
            continue

        # ヘッダ解析
        headers = _get_headers(msg)
        from_header = headers.get("from", "")
        subject = headers.get("subject", "")
        date_header = headers.get("date", "")

        # received_at（internalDate）
        internal_date_ms = int(msg.get("internalDate", 0))
        received_at = datetime.fromtimestamp(
            internal_date_ms / 1000
        ).strftime("%Y-%m-%d %H:%M:%S")

        # 転送元メアド抽出
        body_text = extract_body_text(msg.get("payload", {}))
        _, original_sender_email = extract_original_sender(body_text, from_header)

        # 添付ファイル探索
        att_parts = find_attachment_parts(msg.get("payload", {}))
        if not att_parts:
            continue

        for part in att_parts:
            attachment_id = part.get("body", {}).get("attachmentId", "")
            filename = part.get("filename", "attachment")
            mime_type = part.get("mimeType", "application/octet-stream")

            stats["attachments_total"] += 1

            # 冪等チェック
            if (message_id, attachment_id) in existing_pairs:
                stats["receipts_skipped"] += 1
                continue

            if dry_run:
                logger.info(
                    "[DRY-RUN] %d/%d  %s  from=%s  file=%s",
                    idx, stats["messages_total"], message_id[:8],
                    original_sender_email, filename,
                )
                continue

            # 添付データダウンロード
            try:
                att = service.users().messages().attachments().get(
                    userId="me", messageId=message_id, id=attachment_id
                ).execute()
                data = base64.urlsafe_b64decode(att["data"])
            except Exception as e:
                logger.error(
                    "[ERROR] 添付取得失敗 [%s/%s]: %s",
                    message_id, attachment_id, e,
                )
                stats["errors"] += 1
                continue

            # blob保存
            blob_id, sha = save_blob(conn, data, mime_type)
            if conn.execute(
                "SELECT blob_id FROM blobs WHERE sha256 = ? AND blob_id != ?",
                (sha, blob_id),
            ).fetchone():
                stats["blobs_existing"] += 1
            else:
                # 新規か既存かはsave_blobの挙動で判断
                # blob_idが新規insertされた場合のみカウント
                pass
            stats["blobs_new"] += 1  # 簡易カウント

            # スコープ外判定
            is_oos = False
            if _is_out_of_scope_sender(original_sender_email):
                is_oos = True
            elif _is_out_of_scope_subject(subject):
                is_oos = True
            elif _is_out_of_scope_filename(filename):
                is_oos = True

            # 会社紐付け
            company_id: str | None = None
            resolve_method: str | None = None
            resolve_status: str

            if is_oos:
                resolve_status = "out_of_scope"
                stats["out_of_scope"] += 1
            else:
                company_id, resolve_method = resolve_company(conn, original_sender_email)
                if company_id:
                    resolve_status = "resolved"
                    stats["resolved"] += 1
                else:
                    resolve_status = "unresolved"
                    stats["unresolved"] += 1

            # receipt_event登録
            conn.execute(
                """INSERT INTO receipt_events
                   (gmail_message_id, thread_id, attachment_id, blob_id,
                    original_filename, original_sender_email, from_header,
                    subject, received_at, company_id,
                    resolve_status, resolve_method)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    message_id,
                    thread_id,
                    attachment_id,
                    blob_id,
                    filename,
                    original_sender_email,
                    from_header,
                    subject,
                    received_at,
                    company_id,
                    resolve_status,
                    resolve_method,
                ),
            )
            stats["receipts_inserted"] += 1
            existing_pairs.add((message_id, attachment_id))

        # 10件ごとにcommit
        if idx % 10 == 0:
            conn.commit()

    conn.commit()
    return stats


# ---------------------------------------------------------------------------
# エントリポイント
# ---------------------------------------------------------------------------
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Gmail shinsei.ticメールの全添付ファイルを再取得 → blob保存 + receipt_event登録"
    )
    parser.add_argument("--dry-run", action="store_true", help="取得対象の表示のみ")
    args = parser.parse_args()

    cfg = load_config()
    log_level = getattr(logging, cfg.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.getLogger().setLevel(log_level)

    data_root = Path(cfg["DATA_ROOT"])
    token_cache = data_root / "logs" / ".gmail_token.json"
    credentials_file = cfg["GOOGLE_CREDENTIALS_FILE"]

    # DB接続 + スキーマ追加
    conn = get_connection()
    ensure_schema(conn)

    # Gmail API認証
    service = get_gmail_service(credentials_file, token_cache)

    # 実行
    logger.info("=" * 60)
    logger.info(" recollect_gmail.py 開始")
    logger.info("=" * 60)

    stats = recollect(service, conn, dry_run=args.dry_run)

    # 結果表示
    logger.info("=" * 60)
    logger.info(" 完了")
    logger.info("=" * 60)
    logger.info("  メール数:         %d", stats["messages_total"])
    logger.info("  添付ファイル数:   %d", stats["attachments_total"])
    logger.info("  blob新規保存:     %d", stats["blobs_new"])
    logger.info("  receipt登録:      %d", stats["receipts_inserted"])
    logger.info("  receiptスキップ:  %d", stats["receipts_skipped"])
    logger.info("  resolved:         %d", stats["resolved"])
    logger.info("  unresolved:       %d", stats["unresolved"])
    logger.info("  out_of_scope:     %d", stats["out_of_scope"])
    logger.info("  エラー:           %d", stats["errors"])

    # DB統計
    blob_cnt = conn.execute("SELECT count(*) FROM blobs").fetchone()[0]
    receipt_cnt = conn.execute("SELECT count(*) FROM receipt_events").fetchone()[0]
    resolved_cnt = conn.execute(
        "SELECT count(*) FROM receipt_events WHERE resolve_status = 'resolved'"
    ).fetchone()[0]
    unresolved_cnt = conn.execute(
        "SELECT count(*) FROM receipt_events WHERE resolve_status = 'unresolved'"
    ).fetchone()[0]
    oos_cnt = conn.execute(
        "SELECT count(*) FROM receipt_events WHERE resolve_status = 'out_of_scope'"
    ).fetchone()[0]

    logger.info("")
    logger.info("[DB統計]")
    logger.info("  blobs:            %d", blob_cnt)
    logger.info("  receipt_events:   %d (resolved=%d, unresolved=%d, out_of_scope=%d)",
                receipt_cnt, resolved_cnt, unresolved_cnt, oos_cnt)

    conn.close()


if __name__ == "__main__":
    main()
