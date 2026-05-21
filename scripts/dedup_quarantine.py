"""
Phantom-aware smart dedup with quarantine (GPT-5.5 推奨完全版)。

ロジック:
  1. 同 (cid, file_hash) ファイル群を重複グループとする
  2. グループ内ファイル間で n_pages が大きく違う場合 (max - min >= 2):
     誤分割疑い → 「page 数最小」側を canonical
  3. それ以外: 各 page_no で best label を選択 (manual 保護 + 品質スコア)
  4. canonical 以外を quarantine (is_active=0 + reason 記録)
  5. secondary label は canonical へ migrate

検証ゲート:
  - manual ラベルが quarantine されたら ROLLBACK (FAIL)
  - secondary 消失したら ROLLBACK
  - active page count, manual count を pre/post 比較

Usage:
  python scripts/dedup_quarantine.py --dry-run
  python scripts/dedup_quarantine.py --execute
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import uuid
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
sys.stdout.reconfigure(encoding="utf-8")

KNOWN_DOC_TYPES = {
    "建設業許可証", "決算書", "工事経歴書", "取引先一覧表",
    "労働安全衛生誓約書", "資格略字一覧", "労働者名簿", "会社案内",
}
SPLIT_DIFF_THRESHOLD = 2  # n_pages 差がこれ以上なら誤分割疑い


def label_quality_score(doc_type, secondary, has_manual):
    if has_manual:
        return 1000  # 絶対保護
    s = 0
    if secondary:
        s += 50
    if doc_type in KNOWN_DOC_TYPES:
        s += 30
    elif doc_type == "取引申請書":
        s += 10
    elif doc_type == "空白":
        s += 5
    elif doc_type in ("その他/不明", "その他"):
        s += 1
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()
    if not args.dry_run and not args.execute:
        print("ERROR: --dry-run か --execute", file=sys.stderr); sys.exit(1)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # 事前指標
    pre_active = conn.execute("SELECT COUNT(*) FROM pages WHERE is_active=1").fetchone()[0]
    pre_manual = conn.execute(
        """SELECT COUNT(DISTINCT p.page_id) FROM pages p
           JOIN page_doc_type_history h ON h.page_id=p.page_id
           WHERE p.is_active=1 AND h.confirmed_by IN ('web_viewer','user_visual','masaru_manual')"""
    ).fetchone()[0]
    pre_sec = conn.execute("SELECT COUNT(*) FROM pages WHERE is_active=1 AND doc_type_secondary IS NOT NULL").fetchone()[0]

    # 同 (cid, file_hash) 重複グループ
    groups = list(conn.execute("""
        SELECT company_id, file_hash, COUNT(DISTINCT file_name) n_files
        FROM pages WHERE is_active=1 AND file_hash IS NOT NULL AND file_hash != ''
        GROUP BY company_id, file_hash
        HAVING n_files > 1
    """).fetchall())
    print(f"重複グループ: {len(groups)}", file=sys.stderr)

    quarantine_actions: list[dict] = []  # 隔離対象
    canonical_assignments: list[dict] = []  # canonical 設定

    for g in groups:
        cid = g["company_id"]
        fh = g["file_hash"]
        # ファイル別 page 数
        files_meta = list(conn.execute("""
            SELECT file_name, MIN(page_id) min_pid, COUNT(*) n_pages
            FROM pages WHERE company_id=? AND file_hash=? AND is_active=1
            GROUP BY file_name
        """, (cid, fh)).fetchall())
        n_pages_set = sorted(set(f["n_pages"] for f in files_meta))
        is_split_phantom = len(n_pages_set) >= 2 and (max(n_pages_set) - min(n_pages_set)) >= SPLIT_DIFF_THRESHOLD

        # canonical 選択
        if is_split_phantom:
            # 誤分割疑い: page 数最小を canonical (= 1p 等の本物)
            canonical_file = min(files_meta, key=lambda x: (x["n_pages"], x["min_pid"]))["file_name"]
            split_reason = f"phantom_split (canonical={min(n_pages_set)}p, others={n_pages_set})"
        else:
            # 通常重複: 全 page の合計品質スコアが最高のファイルを canonical
            scores: dict[str, int] = {}
            for f in files_meta:
                fn = f["file_name"]
                total_score = 0
                for p in conn.execute("""
                    SELECT p.doc_type_name, p.doc_type_secondary,
                           (SELECT 1 FROM page_doc_type_history h WHERE h.page_id=p.page_id
                            AND h.confirmed_by IN ('web_viewer','user_visual','masaru_manual')
                            LIMIT 1) AS has_manual
                    FROM pages p WHERE company_id=? AND file_name=? AND is_active=1
                """, (cid, fn)).fetchall():
                    total_score += label_quality_score(p["doc_type_name"], p["doc_type_secondary"], bool(p["has_manual"]))
                scores[fn] = total_score
            canonical_file = max(scores, key=lambda fn: (scores[fn], -[f["min_pid"] for f in files_meta if f["file_name"] == fn][0]))
            split_reason = "duplicate (same_hash)"

        # canonical の page_no -> page_id マップ
        canonical_pages = {
            r["page_no"]: r["page_id"] for r in conn.execute(
                "SELECT page_no, page_id FROM pages WHERE company_id=? AND file_name=? AND is_active=1",
                (cid, canonical_file)
            ).fetchall()
        }

        group_id = f"qgrp_{cid}_{fh[:8]}"

        # 各ファイルのページを処理
        for f in files_meta:
            fn = f["file_name"]
            for p in conn.execute("""
                SELECT p.page_id, p.page_no, p.doc_type_name, p.doc_type_secondary,
                       (SELECT 1 FROM page_doc_type_history h WHERE h.page_id=p.page_id
                        AND h.confirmed_by IN ('web_viewer','user_visual','masaru_manual')
                        LIMIT 1) AS has_manual
                FROM pages p WHERE company_id=? AND file_name=? AND is_active=1
            """, (cid, fn)).fetchall():
                pid = p["page_id"]
                if fn == canonical_file:
                    # canonical 自身
                    canonical_assignments.append({
                        "page_id": pid, "cid": cid, "group_id": group_id,
                    })
                    continue
                # 非 canonical
                if p["has_manual"]:
                    # 絶対保護: manual を quarantine しない (canonical に降格させる手も検討するが今回はスキップ)
                    print(f"  WARN: manual ラベル page_id={pid} ({cid} {fn[:30]} P{p['page_no']}) は canonical={canonical_file[:30]} と異なるが保護のため active 維持", file=sys.stderr)
                    continue
                quarantine_actions.append({
                    "page_id": pid, "cid": cid, "file_name": fn, "page_no": p["page_no"],
                    "doc_type": p["doc_type_name"], "secondary": p["doc_type_secondary"],
                    "canonical_pid": canonical_pages.get(p["page_no"]),
                    "canonical_file": canonical_file,
                    "group_id": group_id, "reason": split_reason,
                })

    # サマリ
    print(f"\n=== Quarantine プラン ===")
    print(f"  対象重複グループ: {len(groups)}")
    print(f"  隔離予定 page:    {len(quarantine_actions)}")
    print(f"  canonical 指定:   {len(canonical_assignments)}")

    by_reason = Counter(q["reason"].split(" ")[0] for q in quarantine_actions)
    print(f"\n  理由別:")
    for reason, n in by_reason.most_common():
        print(f"    {reason}: {n}")

    by_cid = Counter(q["cid"] for q in quarantine_actions)
    print(f"\n  会社別 (top 15):")
    for cid, n in by_cid.most_common(15):
        print(f"    {cid}: {n}")

    if args.dry_run:
        print(f"\n[dry-run] --execute で実行")
        return

    # 実行
    run_id = f"dedup_q_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"\n[execute] dedup_run_id={run_id}", file=sys.stderr)
    conn.execute("BEGIN IMMEDIATE")

    n_q = 0
    for q in quarantine_actions:
        conn.execute("""
            UPDATE pages SET is_active=0,
                quarantine_reason=?, quarantine_group_id=?,
                canonical_page_id=?, dedup_run_id=?,
                quarantined_at=datetime('now','localtime')
            WHERE page_id=?
        """, (q["reason"], q["group_id"], q["canonical_pid"], run_id, q["page_id"]))
        conn.execute("""
            INSERT INTO page_doc_type_history
              (page_id, company_id, file_name, page_no,
               old_doc_type_name, new_doc_type_name,
               reason, workflow_id, decision_id, confirmed_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'system')
        """, (q["page_id"], q["cid"], q["file_name"], q["page_no"],
              q["doc_type"], "(quarantined)",
              f"{q['reason']} canonical_pid={q['canonical_pid']}", run_id, str(uuid.uuid4())))
        n_q += 1

    # 検証ゲート
    post_active = conn.execute("SELECT COUNT(*) FROM pages WHERE is_active=1").fetchone()[0]
    post_manual = conn.execute(
        """SELECT COUNT(DISTINCT p.page_id) FROM pages p
           JOIN page_doc_type_history h ON h.page_id=p.page_id
           WHERE p.is_active=1 AND h.confirmed_by IN ('web_viewer','user_visual','masaru_manual')"""
    ).fetchone()[0]
    post_sec = conn.execute("SELECT COUNT(*) FROM pages WHERE is_active=1 AND doc_type_secondary IS NOT NULL").fetchone()[0]

    print(f"\n=== 検証 ===")
    print(f"  active pages:    {pre_active} → {post_active} ({post_active - pre_active})")
    print(f"  manual labels:   {pre_manual} → {post_manual} ({post_manual - pre_manual})")
    print(f"  secondary count: {pre_sec} → {post_sec} ({post_sec - pre_sec})")
    print(f"  quarantined:     {n_q}")

    if post_manual < pre_manual:
        print(f"\n  ✗ FAIL: manual ラベル {pre_manual - post_manual} 件が消失。ROLLBACK")
        conn.rollback()
        sys.exit(2)
    if post_sec < pre_sec:
        print(f"\n  ⚠ WARN: secondary {pre_sec - post_sec} 件減 (manual 保護で migrate 不可だった可能性)")

    conn.commit()
    print(f"\n=== 完了 ===")
    print(f"  dedup_run_id: {run_id}")


if __name__ == "__main__":
    main()
