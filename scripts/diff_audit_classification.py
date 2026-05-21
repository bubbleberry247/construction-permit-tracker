"""
Phase 5: 差分監査 CSV 出力。

shadow run の予測 vs 現在ラベルを比較し、レビューが必要なページを CSV に書き出す。
GPT-5.5 推奨「filename known -> different known は完全保留、レビュー CSV 出力のみ」を実装。

出力 CSV:
  data/classification_diff_audit_<run_id>_<TS>.csv

カテゴリ:
  - unknown_to_known       : promote_safe で自動昇格対象
  - known_to_same          : 既存ラベルが正しい (検証済み扱い)
  - known_to_different     : 要レビュー (filename ベース誤分類疑い)
  - manual_locked          : 手動ラベルで保護
  - low_text               : OCR 質低、自動 promote 禁止
  - weak_text              : OCR 中程度、conf>=0.97 で promote
  - failed                 : 分類器が決めきれなかった

Usage:
  python scripts/diff_audit_classification.py
  python scripts/diff_audit_classification.py --run-id fp_v1_<TS>
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
sys.stdout.reconfigure(encoding="utf-8")


def categorize(row, manual_locked: bool) -> str:
    cur = row["previous_effective_label"] or ""
    pred = row["predicted_label"] or ""
    conf = row["confidence"] or 0.0
    try:
        ev = json.loads(row["evidence_json"] or "{}")
    except Exception:
        ev = {}
    tq = ev.get("text_quality", "normal")

    if manual_locked:
        return "manual_locked"
    if pred == "その他/不明":
        return "failed"
    if tq == "low_text":
        return "low_text"
    if cur == "その他/不明":
        if tq == "weak_text" and conf < 0.97:
            return "weak_text"
        return "unknown_to_known"
    if cur == pred:
        return "known_to_same"
    return "known_to_different"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", help="未指定なら最新 run")
    ap.add_argument("--top-n", type=int, default=200, help="known_to_different の上位 N 件を CSV 出力")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    if args.run_id:
        run_id = args.run_id
    else:
        r = conn.execute("SELECT run_id FROM classification_runs ORDER BY created_at DESC LIMIT 1").fetchone()
        if not r:
            print("ERROR: classification_runs にレコードなし", file=sys.stderr)
            sys.exit(1)
        run_id = r["run_id"]
    print(f"[init] run_id={run_id}", file=sys.stderr)

    # candidates 取得
    rows = list(conn.execute(
        """SELECT cc.page_id, cc.predicted_label, cc.confidence,
                  cc.second_label, cc.second_confidence,
                  cc.previous_effective_label, cc.evidence_json, cc.manual_locked,
                  p.company_id, p.file_name, p.page_no
           FROM classification_candidates cc
           JOIN pages p USING(page_id)
           WHERE cc.run_id=?
           ORDER BY cc.confidence DESC""",
        (run_id,)
    ).fetchall())

    cat_count: dict[str, int] = {}
    by_change: dict[str, int] = {}
    diff_rows: list[dict] = []
    safe_rows: list[dict] = []

    for r in rows:
        cat = categorize(r, bool(r["manual_locked"]))
        cat_count[cat] = cat_count.get(cat, 0) + 1
        if cat == "known_to_different":
            ck = f'{r["previous_effective_label"]} -> {r["predicted_label"]}'
            by_change[ck] = by_change.get(ck, 0) + 1
        if cat in ("known_to_different", "weak_text"):
            try:
                ev = json.loads(r["evidence_json"] or "{}")
            except Exception:
                ev = {}
            diff_rows.append({
                "page_id": r["page_id"],
                "company_id": r["company_id"],
                "file_name": r["file_name"][:50],
                "page_no": r["page_no"],
                "category": cat,
                "current_label": r["previous_effective_label"],
                "predicted_label": r["predicted_label"],
                "confidence": round(r["confidence"], 3),
                "second_label": r["second_label"] or "",
                "second_conf": round(r["second_confidence"], 3) if r["second_confidence"] else 0,
                "text_quality": ev.get("text_quality", "normal"),
                "text_len": ev.get("text_len_no_ws", 0),
                "evidence": json.dumps(ev.get("evidence", {}), ensure_ascii=False)[:200],
                "approve": "",  # ユーザーが Y を入れたら採用
            })

    print(f"\n=== 分類差分カテゴリ集計 ===")
    for cat, n in sorted(cat_count.items(), key=lambda x: -x[1]):
        print(f"  {cat:25s} {n}")

    if by_change:
        print(f"\n=== known -> different known (top 20) ===")
        for k, n in sorted(by_change.items(), key=lambda x: -x[1])[:20]:
            print(f"  {k:50s} {n}")

    # CSV 出力
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = PROJECT / "data" / f"classification_diff_audit_{run_id}_{ts}.csv"
    if diff_rows:
        with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(diff_rows[0].keys()))
            w.writeheader()
            w.writerows(diff_rows[:args.top_n] if len(diff_rows) > args.top_n else diff_rows)
        print(f"\nレビュー CSV: {out_path}")
        print(f"  ({len(diff_rows)} 件中 {min(args.top_n, len(diff_rows))} を出力、approve=Y で採用候補)")


if __name__ == "__main__":
    main()
