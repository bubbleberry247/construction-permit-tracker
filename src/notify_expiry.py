"""
notify_expiry.py — 建設業許可の期限通知（開発・テスト用）

NOTE: 本番通知は GAS Mailer.gs (毎朝8時トリガー) が担当。
      このスクリプトはテスト・手動確認用として残す。
      GAS と二重送信しないよう、本番では --dry-run で使用すること。

Usage:
    python src/notify_expiry.py --dry-run    # 対象一覧のみ（推奨）
    python src/notify_expiry.py --test-email # 自分宛にテスト送信
    python src/notify_expiry.py              # 手動送信（GAS無効時のみ）
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent.parent / "data" / "permit_tracker.db"
ALERT_DAYS = 150  # 150日前に通知（県知事・大臣共通）
ADMIN_CC = "kanri.tic@tokai-ic.co.jp"
DEV_CC = "karimistk@gmail.com"
COMPANY_NAME = "東海インプル建設株式会社"

SUBJECT_TEMPLATE = "【ご案内】建設業許可の有効期限について（残{days_remaining}日）"

BODY_TEMPLATE = """\
<html>
<body style="font-family: 'Segoe UI', 'Meiryo', sans-serif; font-size: 14px; line-height: 1.8;">
<p>{company_name} 御中</p>
<p>いつもお世話になっております。<br>
{our_company}です。</p>
<p>貴社の建設業許可につきまして、有効期限が近づいておりますのでご案内申し上げます。</p>
<table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-size: 13px;">
<tr style="background-color: #4472C4; color: white;">
  <th>項目</th><th>内容</th>
</tr>
<tr><td>許可番号</td><td>{permit_authority} 許可（{permit_category}）第{permit_number}号</td></tr>
<tr><td>有効期限</td><td><strong style="color: #C00000;">{expiry_date}</strong>（残り{days_remaining}日）</td></tr>
<tr><td>許可業種</td><td>{trades}</td></tr>
</table>
<p>更新申請は有効期限の<strong>90日前から30日前</strong>までに行う必要がございます。<br>
お手続きがまだの場合は、早めのご対応をお願いいたします。</p>
<p>ご不明な点がございましたらお気軽にお問い合わせください。</p>
<p>何卒よろしくお願いいたします。</p>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def ensure_notification_log(conn: sqlite3.Connection) -> None:
    """Create notification_log table if not exists."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS notification_log (
            log_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            permit_id     INTEGER NOT NULL,
            company_id    TEXT NOT NULL,
            expiry_year   TEXT NOT NULL,
            recipient     TEXT NOT NULL,
            sent_at       TEXT NOT NULL,
            subject       TEXT,
            UNIQUE(permit_id, expiry_year)
        )
    """)
    conn.commit()


def get_expiring_permits(conn: sqlite3.Connection) -> list[dict]:
    """Get permits expiring within ALERT_DAYS that haven't been notified yet."""
    cur = conn.cursor()
    cur.execute("""
        SELECT p.permit_id, p.company_id, c.official_name,
               p.permit_number, p.permit_authority, p.permit_category,
               p.expiry_date,
               CAST(julianday(p.expiry_date) - julianday('now','localtime') AS INTEGER) as days_remaining,
               GROUP_CONCAT(pt.trade_name, '、') as trades
        FROM permits p
        JOIN companies c ON p.company_id = c.company_id
        LEFT JOIN permit_trades pt ON p.permit_id = pt.permit_id
        WHERE p.current_flag = 1
          AND p.expiry_date IS NOT NULL
          AND p.expiry_date != 'UNCERTAIN'
          AND julianday(p.expiry_date) - julianday('now','localtime') <= ?
          AND julianday(p.expiry_date) - julianday('now','localtime') > 0
          AND NOT EXISTS (
              SELECT 1 FROM notification_log nl
              WHERE nl.permit_id = p.permit_id
                AND nl.expiry_year = strftime('%Y', p.expiry_date)
          )
        GROUP BY p.permit_id
        ORDER BY days_remaining ASC
    """, (ALERT_DAYS,))

    results = []
    for r in cur.fetchall():
        results.append({
            "permit_id": r[0],
            "company_id": r[1],
            "company_name": r[2],
            "permit_number": r[3] or "",
            "permit_authority": r[4] or "",
            "permit_category": r[5] or "",
            "expiry_date": r[6],
            "days_remaining": r[7],
            "trades": r[8] or "",
        })
    return results


def get_company_email(conn: sqlite3.Connection, company_id: str) -> str | None:
    """Get the best email for a company (prefer 'recipient' type)."""
    cur = conn.cursor()
    # Prefer recipient type, then sender
    cur.execute("""
        SELECT email FROM company_emails
        WHERE company_id = ?
        ORDER BY CASE email_type WHEN 'recipient' THEN 0 ELSE 1 END
        LIMIT 1
    """, (company_id,))
    r = cur.fetchone()
    return r[0] if r else None


def record_notification(
    conn: sqlite3.Connection,
    permit_id: int,
    company_id: str,
    expiry_date: str,
    recipient: str,
    subject: str,
) -> None:
    """Record that a notification was sent."""
    expiry_year = expiry_date[:4] if expiry_date else ""
    conn.execute("""
        INSERT OR IGNORE INTO notification_log
            (permit_id, company_id, expiry_year, recipient, sent_at, subject)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (permit_id, company_id, expiry_year, recipient,
          datetime.now().strftime("%Y-%m-%d %H:%M:%S"), subject))
    conn.commit()


# ---------------------------------------------------------------------------
# Email (Outlook COM)
# ---------------------------------------------------------------------------

def send_outlook_html(
    to: str,
    cc: str,
    subject: str,
    html_body: str,
) -> bool:
    """Send HTML email via Outlook COM."""
    try:
        import win32com.client
        outlook = win32com.client.Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        mail.To = to
        mail.CC = cc
        mail.Subject = subject
        mail.HTMLBody = html_body
        mail.Send()
        return True
    except Exception as e:
        print(f"  [ERROR] Outlook send failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="建設業許可期限通知")
    parser.add_argument("--dry-run", action="store_true", help="対象一覧のみ（送信なし）")
    parser.add_argument("--test-email", action="store_true", help="自分宛にテスト送信")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    ensure_notification_log(conn)

    permits = get_expiring_permits(conn)

    print(f"{'='*60}")
    print(f"  建設業許可 期限通知 ({ALERT_DAYS}日前)")
    print(f"  対象: {len(permits)} 件")
    print(f"  Mode: {'DRY-RUN' if args.dry_run else 'TEST' if args.test_email else 'LIVE'}")
    print(f"{'='*60}")

    if not permits:
        print("\n  通知対象なし")
        conn.close()
        return

    sent = 0
    failed = 0
    no_email = 0

    for p in permits:
        email = get_company_email(conn, p["company_id"])
        subject = SUBJECT_TEMPLATE.format(days_remaining=p["days_remaining"])
        body = BODY_TEMPLATE.format(
            company_name=p["company_name"],
            our_company=COMPANY_NAME,
            permit_authority=p["permit_authority"],
            permit_category=p["permit_category"],
            permit_number=p["permit_number"],
            expiry_date=p["expiry_date"],
            days_remaining=p["days_remaining"],
            trades=p["trades"],
        )

        print(f"\n  [{p['days_remaining']}日] {p['company_name']}")
        print(f"    許可: {p['permit_authority']} ({p['permit_category']}) 第{p['permit_number']}号")
        print(f"    期限: {p['expiry_date']}")
        print(f"    業種: {p['trades'][:60]}{'...' if len(p['trades']) > 60 else ''}")
        print(f"    宛先: {email or 'NO EMAIL'}")

        if not email:
            no_email += 1
            print(f"    -> SKIP (メールアドレスなし)")
            continue

        if args.dry_run:
            print(f"    -> DRY-RUN (送信スキップ)")
            continue

        # Test mode: send to dev only
        actual_to = DEV_CC if args.test_email else email
        actual_cc = "" if args.test_email else f"{ADMIN_CC}; {DEV_CC}"

        if args.test_email:
            subject = f"[TEST] {subject}"

        ok = send_outlook_html(actual_to, actual_cc, subject, body)

        if ok:
            sent += 1
            print(f"    -> SENT to {actual_to}")
            if not args.test_email:
                record_notification(conn, p["permit_id"], p["company_id"],
                                    p["expiry_date"], email, subject)
        else:
            failed += 1
            print(f"    -> FAILED")

    # Summary
    print(f"\n{'='*60}")
    print(f"  Summary")
    print(f"    対象:     {len(permits)} 件")
    print(f"    送信済:   {sent} 件")
    print(f"    失敗:     {failed} 件")
    print(f"    メールなし: {no_email} 件")
    print(f"{'='*60}")

    conn.close()


if __name__ == "__main__":
    main()
