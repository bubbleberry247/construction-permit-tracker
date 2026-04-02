"""
fix_unmatched_emails.py — inbound_messages の未紐付けメールを再マッチング

Usage:
    python -X utf8 src/fix_unmatched_emails.py          # dry-run
    python -X utf8 src/fix_unmatched_emails.py --apply   # 実際に更新
"""
import re
import sys

sys.stdout.reconfigure(encoding="utf-8")

from pathlib import Path

# ── プロジェクトルートを sys.path に追加 ──
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from db import get_connection

# ── ノイズメール（マッチ対象外） ──
NOISE_DOMAINS = {"anthropic.com", "mail.anthropic.com", "stripe.com", "google.com"}
NOISE_EMAILS = {"masamaru1975@hotmail.com", "kalimistk@gmail.com", "shinsei.tic@gmail.com"}


def normalize_email(raw: str) -> str:
    """山括弧除去 + 小文字化 + strip"""
    if not raw:
        return ""
    # <email> 形式
    match = re.search(r"<([^>]+)>", raw)
    if match:
        return match.group(1).strip().lower()
    # @を含む部分を抽出
    match = re.search(r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)", raw)
    if match:
        return match.group(1).strip().lower()
    return raw.strip().lower()


def is_noise(email: str) -> bool:
    """ノイズメールかどうか判定"""
    if not email:
        return False
    if email in NOISE_EMAILS:
        return True
    domain = email.split("@", 1)[-1] if "@" in email else ""
    return domain in NOISE_DOMAINS


def build_email_lookup(conn) -> dict[str, str]:
    """company_emails テーブルから email -> company_id の辞書を構築"""
    cur = conn.execute("SELECT email, company_id FROM company_emails")
    lookup: dict[str, str] = {}
    for row in cur.fetchall():
        lookup[row["email"].strip().lower()] = row["company_id"]
    return lookup


def find_unmatched(conn) -> list[dict]:
    """company_id IS NULL の inbound_messages を取得"""
    cur = conn.execute(
        "SELECT message_id, sender_email, original_sender "
        "FROM inbound_messages WHERE company_id IS NULL"
    )
    return [dict(row) for row in cur.fetchall()]


def match_records(
    unmatched: list[dict], lookup: dict[str, str]
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Returns:
        matched: [{message_id, company_id, matched_email, source}, ...]
        skipped: [{message_id, sender_email, reason}, ...]  (ノイズ)
        unresolved: [{message_id, sender_email, original_sender}, ...]
    """
    matched: list[dict] = []
    skipped: list[dict] = []
    unresolved: list[dict] = []

    for rec in unmatched:
        mid = rec["message_id"]
        sender_raw = rec["sender_email"] or ""
        orig_raw = rec["original_sender"] or ""

        sender_norm = normalize_email(sender_raw)
        orig_norm = normalize_email(orig_raw)

        # ノイズ判定（両方ノイズならスキップ）
        sender_is_noise = is_noise(sender_norm)
        orig_is_noise = is_noise(orig_norm) or not orig_norm

        if sender_is_noise and orig_is_noise:
            skipped.append(
                {"message_id": mid, "sender_email": sender_raw, "reason": "noise"}
            )
            continue

        # マッチング: sender_email → original_sender の順
        company_id = None
        matched_email = ""
        source = ""

        if not sender_is_noise and sender_norm in lookup:
            company_id = lookup[sender_norm]
            matched_email = sender_norm
            source = "sender_email"
        elif not orig_is_noise and orig_norm in lookup:
            company_id = lookup[orig_norm]
            matched_email = orig_norm
            source = "original_sender"

        if company_id:
            matched.append(
                {
                    "message_id": mid,
                    "company_id": company_id,
                    "matched_email": matched_email,
                    "source": source,
                }
            )
        else:
            unresolved.append(
                {
                    "message_id": mid,
                    "sender_email": sender_raw,
                    "original_sender": orig_raw,
                    "sender_norm": sender_norm,
                    "orig_norm": orig_norm,
                }
            )

    return matched, skipped, unresolved


def apply_updates(conn, matched: list[dict]) -> tuple[int, int]:
    """inbound_messages と files の company_id を更新"""
    msg_count = 0
    file_count = 0

    for rec in matched:
        mid = rec["message_id"]
        cid = rec["company_id"]

        # inbound_messages 更新
        conn.execute(
            "UPDATE inbound_messages SET company_id = ? WHERE message_id = ?",
            (cid, mid),
        )
        msg_count += 1

        # files 更新
        cur = conn.execute(
            "UPDATE files SET company_id = ? WHERE message_id = ? AND company_id IS NULL",
            (cid, mid),
        )
        file_count += cur.rowcount

    conn.commit()
    return msg_count, file_count


def main() -> None:
    apply = "--apply" in sys.argv

    conn = get_connection()
    lookup = build_email_lookup(conn)
    unmatched = find_unmatched(conn)

    print(f"[info] company_emails 登録数: {len(lookup)}")
    print(f"[info] company_id=NULL メッセージ数: {len(unmatched)}")
    print()

    matched, skipped, unresolved = match_records(unmatched, lookup)

    # ── サマリー ──
    print("=" * 60)
    print(f"  マッチ成功: {len(matched)} 件")
    print(f"  ノイズスキップ: {len(skipped)} 件")
    print(f"  未マッチ: {len(unresolved)} 件")
    print("=" * 60)
    print()

    # ── マッチ詳細 ──
    if matched:
        print("--- マッチ成功 ---")
        for rec in matched:
            print(
                f"  {rec['message_id']}  -> {rec['company_id']}  "
                f"({rec['matched_email']}, via {rec['source']})"
            )
        print()

    # ── 未マッチ詳細 ──
    if unresolved:
        print("--- 未マッチ ---")
        for rec in unresolved:
            print(
                f"  {rec['message_id']}  sender={rec['sender_norm']}  "
                f"orig={rec['orig_norm']}"
            )
        print()

    # ── ノイズ詳細 ──
    if skipped:
        print(f"--- ノイズスキップ ({len(skipped)} 件) ---")
        # ドメイン別集計
        noise_summary: dict[str, int] = {}
        for rec in skipped:
            email = normalize_email(rec["sender_email"])
            noise_summary[email] = noise_summary.get(email, 0) + 1
        for email, cnt in sorted(noise_summary.items(), key=lambda x: -x[1]):
            print(f"  {email}: {cnt} 件")
        print()

    # ── 適用 ──
    if apply:
        if not matched:
            print("[info] マッチ対象なし。更新スキップ。")
        else:
            msg_count, file_count = apply_updates(conn, matched)
            print(f"[apply] inbound_messages 更新: {msg_count} 件")
            print(f"[apply] files 更新: {file_count} 件")
    else:
        print("[dry-run] --apply を付けて実行すると更新されます。")

    conn.close()


if __name__ == "__main__":
    main()
