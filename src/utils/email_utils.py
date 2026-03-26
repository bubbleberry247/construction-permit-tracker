"""
email_utils.py — 転送メールから元送信者を抽出するユーティリティ。

shinsei.tic@gmail.com 経由で転送されたメールの本文から、
実際の送信者（業者）のメールアドレスと表示名を抽出する。
"""
from __future__ import annotations

import base64
import re


def extract_body_text(payload: dict) -> str:
    """Extract plain text body from Gmail API email payload.

    Recursively searches MIME parts for text/plain content.
    Falls back to text/html with tag stripping if no plain text found.

    Args:
        payload: Gmail API message payload dict.
    Returns:
        Decoded body text, or empty string if extraction fails.
    """
    if payload.get("mimeType") == "text/plain":
        data = payload.get("body", {}).get("data", "")
        if data:
            try:
                return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
            except Exception:
                return ""

    for part in payload.get("parts", []):
        text = extract_body_text(part)
        if text:
            return text

    # Try HTML if no plain text
    if payload.get("mimeType") == "text/html":
        data = payload.get("body", {}).get("data", "")
        if data:
            try:
                html = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                return re.sub(r"<[^>]+>", " ", html)
            except Exception:
                return ""

    return ""


def extract_original_sender(body: str, from_header: str) -> tuple[str, str]:
    """Extract original sender from forwarded email body.

    Searches for forwarded message headers (From:, 差出人:, 送信者:)
    in the email body text to find the actual sender before forwarding.

    Args:
        body: Email body text (plain text).
        from_header: Original From header value (fallback).
    Returns:
        Tuple of (display_name, email_address).
        email_address may be empty string if not found.
    """
    patterns = [
        r"From:\s*(.+?)[\r\n]",
        r"差出人:\s*(.+?)[\r\n]",
        r"送信者:\s*(.+?)[\r\n]",
        r"From：\s*(.+?)[\r\n]",
    ]

    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            sender_str = match.group(1).strip()
            # Extract email from "Name <email>" format
            email_match = re.search(r"<([^>]+@[^>]+)>", sender_str)
            if email_match:
                email = email_match.group(1)
                name = re.sub(r"\s*<[^>]+>\s*", "", sender_str).strip().strip('"')
                return (name or email, email)
            # Might be just an email address
            email_match = re.search(
                r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)", sender_str
            )
            if email_match:
                return (sender_str, email_match.group(1))
            return (sender_str, "")

    # Fallback: use From header itself
    email_match = re.search(r"<([^>]+@[^>]+)>", from_header)
    if email_match:
        email = email_match.group(1)
        name = re.sub(r"\s*<[^>]+>\s*", "", from_header).strip().strip('"')
        return (name or email, email)

    return (from_header, "")
