"""
reimport_from_gmail.py

別PCで取り込まれた shinsei.tic ラベル付きメールをこのPCに再取込する
一回限りの補完スクリプト。

設計方針:
  - Gmailラベル操作は一切行わない（別PCの処理済みラベルを尊重）
  - ローカルの inbound_log.csv (message_id, attachment_id) で dedup
  - 「許可証処理済み」ラベルの有無に関係なく shinsei.tic ラベル付きを対象
  - 期間を after: で絞って全件スキャンを回避

Usage:
  python src/reimport_from_gmail.py --dry-run --after 2026/03/27
  python src/reimport_from_gmail.py --after 2026/03/27 --max 200
"""
from __future__ import annotations

import argparse
import base64
import logging
from datetime import datetime
from pathlib import Path

import fetch_gmail
from fetch_gmail import (
    _find_attachment_parts,
    _new_id,
    _now,
    append_inbound_log,
    get_gmail_service,
    load_config,
    load_inbound_log,
    sanitize_filename,
    sha256_of_bytes,
)
from utils.email_utils import extract_body_text, extract_original_sender

logger = logging.getLogger("reimport")


def build_query(after_date: str) -> str:
    """ラベルフィルタは除外しない。shinsei.tic + ノイズ除外 + 期間のみ。"""
    return (
        f"label:shinsei.tic after:{after_date}"
        f" -from:mailer-daemon -from:noreply -from:no-reply"
        f" -subject:セキュリティ通知 -subject:ワンタイムパスワード"
        f' -subject:"Google 検索" -subject:"Delivery Status"'
    )


def _load_processed_message_ids(log_path: Path) -> set[str]:
    """inbound_log.csv から処理済み message_id だけを収集（attachment_idは不安定なため）"""
    import csv as _csv
    seen: set[str] = set()
    if not log_path.exists():
        return seen
    with log_path.open(encoding="utf-8-sig", newline="") as f:
        for row in _csv.DictReader(f):
            mid = (row.get("message_id") or "").strip()
            if mid:
                seen.add(mid)
    return seen


def reimport(service, inbox_dir: Path, log_path: Path, query: str,
             max_messages: int, dry_run: bool) -> dict:
    processed_message_ids = _load_processed_message_ids(log_path)
    logger.info("既処理 message_id 数: %d", len(processed_message_ids))
    logger.info("Gmail query: %s", query)

    resp = service.users().messages().list(
        userId="me", q=query, maxResults=max_messages
    ).execute()
    messages = resp.get("messages", [])
    logger.info("Gmail ヒット: %d件（query上限=%d）", len(messages), max_messages)

    if not messages:
        return {"hits": 0, "already": 0, "no_attach": 0, "new_saved": 0}

    already = 0
    no_attach = 0
    new_saved = 0

    inbox_dir.mkdir(parents=True, exist_ok=True)

    for msg_meta in messages:
        message_id = msg_meta["id"]

        # message_id単位で dedup（attachment_idはGmail APIで毎回別値が返るため不安定）
        if message_id in processed_message_ids:
            already += 1
            continue

        msg = service.users().messages().get(
            userId="me", id=message_id, format="full"
        ).execute()

        headers = {h["name"].lower(): h["value"]
                   for h in msg.get("payload", {}).get("headers", [])}
        sender_raw = headers.get("from", "")
        import re
        m = re.search(r"<([^>]+)>", sender_raw)
        sender_email = m.group(1) if m else sender_raw.strip()
        internal_ms = int(msg.get("internalDate", 0))
        received_at = datetime.fromtimestamp(internal_ms / 1000).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        body_text = extract_body_text(msg.get("payload", {}))
        _, original_sender_email = extract_original_sender(body_text, sender_email)

        attach_parts = _find_attachment_parts(msg.get("payload", {}))
        if not attach_parts:
            no_attach += 1
            logger.info("添付なし [%s] %s %s",
                        received_at, sender_email[:40],
                        headers.get("subject", "")[:40])
            continue

        any_new_here = False
        for part in attach_parts:
            attachment_id = part.get("body", {}).get("attachmentId", "")
            file_name = sanitize_filename(part.get("filename", "attachment"))

            if dry_run:
                logger.info("[DRY-RUN] %s | %s | %s",
                            received_at, sender_email, file_name)
                any_new_here = True
                new_saved += 1
                continue

            try:
                att = service.users().messages().attachments().get(
                    userId="me", messageId=message_id, id=attachment_id
                ).execute()
                data = base64.urlsafe_b64decode(att["data"])
            except Exception as e:
                logger.error("添付取得失敗 [%s]: %s", message_id, e)
                append_inbound_log(log_path, {
                    "log_id": _new_id(),
                    "message_id": message_id,
                    "attachment_id": attachment_id,
                    "received_at": received_at,
                    "sender_email": sender_email,
                    "original_sender_email": original_sender_email,
                    "file_name": file_name,
                    "file_hash": "",
                    "file_size_bytes": 0,
                    "saved_path": "",
                    "process_status": "FAILED",
                    "error_message": str(e),
                    "created_at": _now(),
                })
                continue

            file_hash = sha256_of_bytes(data)
            file_size = len(data)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_name = f"{ts}_{file_name}"
            save_path = inbox_dir / unique_name
            stem, suffix = Path(unique_name).stem, Path(unique_name).suffix
            counter = 1
            while save_path.exists():
                save_path = inbox_dir / f"{stem}_{counter}{suffix}"
                counter += 1

            save_path.write_bytes(data)
            logger.info("保存: %s (%d bytes, hash=%s...)",
                        save_path.name, file_size, file_hash[:8])

            append_inbound_log(log_path, {
                "log_id": _new_id(),
                "message_id": message_id,
                "attachment_id": attachment_id,
                "received_at": received_at,
                "sender_email": sender_email,
                "original_sender_email": original_sender_email,
                "file_name": file_name,
                "file_hash": file_hash,
                "file_size_bytes": file_size,
                "saved_path": str(save_path),
                "process_status": "PENDING",
                "created_at": _now(),
            })
            new_saved += 1
            any_new_here = True

        if any_new_here:
            # 今回取込したmsg_idも以降の重複スキップ対象に追加
            processed_message_ids.add(message_id)

    # NOTE: Gmail側の「許可証処理済み」ラベル付与はしない（別PCの状態を尊重）
    return {
        "hits": len(messages),
        "already": already,
        "no_attach": no_attach,
        "new_saved": new_saved,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--after", default="2026/03/27",
                        help="Gmail query after: YYYY/MM/DD")
    parser.add_argument("--max", type=int, default=200)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    cfg = load_config()
    data_root = Path(cfg["DATA_ROOT"])
    inbox_dir = data_root / "data" / "inbox"
    log_path = data_root / "logs" / "inbound_log.csv"
    token_cache = data_root / "logs" / ".gmail_token.json"
    credentials_file = cfg["GOOGLE_CREDENTIALS_FILE"]

    service = get_gmail_service(credentials_file, token_cache)
    query = build_query(args.after)
    stats = reimport(service, inbox_dir, log_path, query, args.max, args.dry_run)

    print("\n=== サマリ ===")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    if args.dry_run:
        print("  ※ --dry-run。実保存は行っていません。")


if __name__ == "__main__":
    main()
