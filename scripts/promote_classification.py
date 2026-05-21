"""
Phase 6: shadow 分類器の予測を pages テーブルに promote する。

安全モード (--mode promote_safe):
  - previous_effective_label = 'その他/不明' のみ
  - predicted_label != 'その他/不明'
  - confidence >= 0.85
  - manual_locked = 0
  - workflow_id = fingerprint_promote_<TS>
  - confirmed_by = 'fingerprint_auto'

Usage:
  python scripts/promote_classification.py --run-id <ID> --dry-run --mode promote_safe
  python scripts/promote_classification.py --run-id <ID> --execute --mode promote_safe
  python scripts/promote_classification.py --run-id <ID> --execute --confidence-min 0.90 --mode promote_safe
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
sys.stdout.reconfigure(encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", help="未指定なら最新 shadow run")
    ap.add_argument("--mode", choices=["promote_safe"], default="promote_safe")
    ap.add_argument("--confidence-min", type=float, default=0.85)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    if not args.dry_run and not args.execute:
        print("ERROR: --dry-run か --execute を指定してください", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    if args.run_id:
        run_id = args.run_id
    else:
        r = conn.execute("SELECT run_id FROM classification_runs WHERE mode='shadow' ORDER BY created_at DESC LIMIT 1").fetchone()
        if not r:
            print("ERROR: shadow run が見つかりません", file=sys.stderr)
            sys.exit(1)
        run_id = r["run_id"]

    # safety check: precision/recall
    run_row = conn.execute("SELECT * FROM classification_runs WHERE run_id=?", (run_id,)).fetchone()
    print(f"[init] run_id={run_id}", file=sys.stderr)
    print(f"  pages_processed: {run_row['pages_processed']}", file=sys.stderr)
    print(f"  gold_precision:  {run_row['gold_precision']}", file=sys.stderr)
    print(f"  gold_recall:     {run_row['gold_recall']}", file=sys.stderr)
    print(f"  mode:            {args.mode}", file=sys.stderr)

    # 候補抽出 (evidence_json も取得して text_quality を判定)
    candidates = list(conn.execute(
        """SELECT cc.candidate_id, cc.page_id, cc.predicted_label, cc.confidence,
                  cc.previous_effective_label, cc.evidence_json,
                  p.company_id, p.file_name, p.page_no
           FROM classification_candidates cc
           JOIN pages p USING(page_id)
           WHERE cc.run_id=?
             AND cc.previous_effective_label='その他/不明'
             AND cc.predicted_label != 'その他/不明'
             AND cc.confidence >= ?
             AND cc.manual_locked = 0""",
        (run_id, args.confidence_min)
    ).fetchall())

    # GPT-5.5 推奨 text_quality ゲート + 手動ラベル絶対除外
    import json as _json
    safe = []
    n_low_text_blocked = 0
    n_weak_text_demoted = 0
    n_manual_blocked = 0
    for c in candidates:
        manual = conn.execute(
            """SELECT 1 FROM page_doc_type_history
               WHERE page_id=? AND confirmed_by IN ('web_viewer','user_visual','masaru_manual')
               LIMIT 1""", (c["page_id"],)).fetchone()
        if manual:
            n_manual_blocked += 1
            continue
        try:
            ev = _json.loads(c["evidence_json"] or "{}")
            tq = ev.get("text_quality", "normal")
        except Exception:
            tq = "normal"
        if tq == "low_text":
            n_low_text_blocked += 1
            continue
        if tq == "weak_text" and c["confidence"] < 0.97:
            n_weak_text_demoted += 1
            continue
        safe.append(c)

    print(f"\n=== promote 候補 ({args.mode}, conf>={args.confidence_min}) ===")
    print(f"  candidates: {len(candidates)}")
    print(f"  手動ラベル ブロック:           {n_manual_blocked}")
    print(f"  low_text ブロック:             {n_low_text_blocked}")
    print(f"  weak_text demote (conf<0.97):  {n_weak_text_demoted}")
    print(f"  最終 safe:                    {len(safe)}")

    by_label: dict[str, int] = {}
    for c in safe:
        l = c["predicted_label"]
        by_label[l] = by_label.get(l, 0) + 1
    for l, n in sorted(by_label.items(), key=lambda x: -x[1]):
        print(f"  {l:25s} {n}")

    if args.dry_run:
        print(f"\n[dry-run] 実行するには --execute を指定してください")
        return

    # 実行
    workflow = f"fingerprint_promote_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"\n[execute] workflow_id = {workflow}", file=sys.stderr)
    conn.execute("BEGIN")
    n_updated = 0
    for c in safe:
        conn.execute(
            "UPDATE pages SET doc_type_name=?, confidence=? WHERE page_id=?",
            (c["predicted_label"], c["confidence"], c["page_id"])
        )
        conn.execute(
            """INSERT INTO page_doc_type_history
               (page_id, company_id, file_name, page_no,
                old_doc_type_name, new_doc_type_name,
                reason, workflow_id, decision_id, confirmed_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'fingerprint_auto')""",
            (c["page_id"], c["company_id"], c["file_name"], c["page_no"],
             "その他/不明", c["predicted_label"],
             f"fingerprint_v1 conf={c['confidence']:.3f}",
             workflow, str(uuid.uuid4()))
        )
        n_updated += 1

    # promoted run として更新
    conn.execute(
        "UPDATE classification_runs SET mode='promoted' WHERE run_id=?",
        (run_id,)
    )
    conn.commit()

    print(f"\n=== 完了 ===")
    print(f"  UPDATE pages:         {n_updated} 件")
    print(f"  INSERT history:       {n_updated} 件 (workflow={workflow})")

    # 検算: 手動ラベルが今回 UPDATE で上書きされていないこと
    manual_after_auto = conn.execute(
        """SELECT COUNT(*) FROM page_doc_type_history h1
           WHERE h1.confirmed_by IN ('web_viewer','user_visual','masaru_manual')
             AND EXISTS (
               SELECT 1 FROM page_doc_type_history h2
               WHERE h2.page_id = h1.page_id
                 AND h2.workflow_id = ?
                 AND h2.history_id > h1.history_id
             )""", (workflow,)).fetchone()[0]
    print(f"\n  [安全性検算] 手動ラベル後に自動更新が入った件数: {manual_after_auto} (期待: 0)")

    if manual_after_auto > 0:
        print(f"  ⚠️  ROLLBACK 推奨: 安全性が破られました", file=sys.stderr)


if __name__ == "__main__":
    main()
