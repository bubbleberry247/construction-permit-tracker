"""
GPT-5.5 推奨: タスク1 (xlsx 10 社目視) と タスク2 (193 件 diff_audit) のオーバーラップ集計。

会社別:
  - xlsx_target: xlsx 対象 10 社か
  - diff_count: その会社の fingerprint 不一致ページ数
  - high_conf_count: confidence >= 0.93 件数
  - main_patterns: 上位 3 つの current→predicted パターン
  - missing_docs: 不足書類 (◎全揃いに足りない種別)
  - complete_gain_potential: もし全 diff を approve したら ◎全揃いになるか

Usage:
  python scripts/diff_overlap_summary.py
  python scripts/diff_overlap_summary.py --run-id fp_v1_<TS>
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
sys.stdout.reconfigure(encoding="utf-8")

XLSX_TARGETS = ["C0029", "C0062", "C0077", "C0079", "C0082", "C0083", "C0118", "C0147", "C0149", "C0150"]
REQUIRED = ["取引申請書", "建設業許可証", "決算書", "会社案内", "工事経歴書", "取引先一覧表",
            "労働安全衛生誓約書", "資格略字一覧", "労働者名簿"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    if args.run_id:
        run_id = args.run_id
    else:
        r = conn.execute("SELECT run_id FROM classification_runs ORDER BY created_at DESC LIMIT 1").fetchone()
        if not r:
            print("ERROR: classification_runs 空", file=sys.stderr)
            sys.exit(1)
        run_id = r["run_id"]
    print(f"[init] run_id={run_id}", file=sys.stderr)

    # diff candidates 集計 (categorize 相当)
    rows = list(conn.execute(
        """SELECT cc.page_id, cc.predicted_label, cc.confidence,
                  cc.previous_effective_label, cc.evidence_json, cc.manual_locked,
                  p.company_id, p.file_name, p.page_no, p.doc_type_name as current_label
           FROM classification_candidates cc
           JOIN pages p USING(page_id)
           WHERE cc.run_id=?""", (run_id,)
    ).fetchall())

    # 会社別 diff
    diff_by_cid: dict[str, list] = defaultdict(list)
    for r in rows:
        if r["manual_locked"]:
            continue
        cur = r["current_label"]
        pred = r["predicted_label"]
        if pred == "その他/不明":
            continue
        if cur == pred:
            continue
        # text_quality
        try:
            ev = json.loads(r["evidence_json"] or "{}")
            tq = ev.get("text_quality", "normal")
        except Exception:
            tq = "normal"
        if tq == "low_text":
            continue
        # known→different のみ
        if cur in ("その他/不明", "空白"):
            continue
        diff_by_cid[r["company_id"]].append({
            "page_id": r["page_id"],
            "page_no": r["page_no"],
            "file_name": r["file_name"],
            "current": cur,
            "predicted": pred,
            "confidence": r["confidence"],
            "text_quality": tq,
        })

    # 各社の現状ラベル分布 + 不足書類
    out_rows = []
    for cid, diffs in sorted(diff_by_cid.items(), key=lambda x: -len(x[1])):
        cmp = conn.execute("SELECT official_name FROM companies WHERE company_id=?", (cid,)).fetchone()
        if not cmp:
            continue
        labels = conn.execute(
            "SELECT doc_type_name, COUNT(*) FROM pages WHERE company_id=? GROUP BY doc_type_name", (cid,)
        ).fetchall()
        found = {l[0] for l in labels}
        missing = [d for d in REQUIRED if d not in found]

        # diff の中で不足書類を埋めるパターン
        gain_patterns = []
        for d in diffs:
            if d["predicted"] in missing and d["confidence"] >= 0.85:
                gain_patterns.append(d["predicted"])
        gain_unique = set(gain_patterns)
        # complete_gain: 不足全部を埋められれば +1 ◎
        complete_gain = "yes" if missing and gain_unique >= set(missing) else ""
        # partial_gain: 一部埋まる
        partial_gain = len(gain_unique & set(missing))

        # 高 conf カウント
        hi = sum(1 for d in diffs if d["confidence"] >= 0.93)
        # 上位パターン
        pat_count = Counter(f'{d["current"]}->{d["predicted"]}' for d in diffs)
        top_pats = "; ".join(f'{p}({n})' for p, n in pat_count.most_common(3))

        out_rows.append({
            "company_id": cid,
            "official_name": cmp["official_name"],
            "xlsx_target": "Y" if cid in XLSX_TARGETS else "",
            "diff_count": len(diffs),
            "high_conf_count": hi,
            "missing_count": len(missing),
            "missing_docs": " / ".join(missing) if missing else "",
            "complete_gain": complete_gain,
            "partial_gain_count": partial_gain,
            "main_patterns": top_pats,
        })

    # ソート: complete_gain > partial_gain > xlsx_target > diff_count
    out_rows.sort(key=lambda x: (
        x["complete_gain"] != "yes",
        -x["partial_gain_count"],
        x["xlsx_target"] != "Y",
        -x["diff_count"]
    ))

    # 表示
    print(f"\n=== diff_audit × xlsx_target × ◎全揃い影響 集計 ===")
    print(f"{'cid':>6} {'xlsx':>4} {'diff':>5} {'hi':>4} {'miss':>4} {'gain':>5} {'partial':>7}  patterns")
    print("-" * 100)
    for r in out_rows[:30]:
        gain = "◎" if r["complete_gain"] == "yes" else ""
        print(f'{r["company_id"]:>6} {r["xlsx_target"]:>4} {r["diff_count"]:>5} {r["high_conf_count"]:>4} {r["missing_count"]:>4} {gain:>5} {r["partial_gain_count"]:>7}  {r["main_patterns"][:60]}')

    # CSV 出力
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = PROJECT / "data" / f"diff_overlap_summary_{ts}.csv"
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)
    print(f"\n出力: {out_path.name}")

    # サマリ
    print(f"\n=== サマリ ===")
    n_xlsx_with_diff = sum(1 for r in out_rows if r["xlsx_target"] == "Y")
    n_complete_gain = sum(1 for r in out_rows if r["complete_gain"] == "yes")
    n_partial_gain = sum(1 for r in out_rows if r["partial_gain_count"] > 0 and r["complete_gain"] != "yes")
    n_hi = sum(r["high_conf_count"] for r in out_rows)
    total_diff = sum(r["diff_count"] for r in out_rows)
    print(f"  diff 持つ会社:                {len(out_rows)}")
    print(f"  うち xlsx 対象:                {n_xlsx_with_diff}")
    print(f"  ◎全揃い改善見込み:            {n_complete_gain} 社")
    print(f"  partial gain (一部不足解消):  {n_partial_gain} 社")
    print(f"  diff 件数合計:                {total_diff}")
    print(f"  high_conf (>=0.93) 合計:      {n_hi}")

    # 上位パターン全社統合
    print(f"\n=== 全社統合 上位パターン ===")
    all_pats = Counter()
    for cid, diffs in diff_by_cid.items():
        for d in diffs:
            all_pats[f'{d["current"]} -> {d["predicted"]}'] += 1
    for p, n in all_pats.most_common(15):
        print(f"  {p:50s} {n}")


if __name__ == "__main__":
    main()
