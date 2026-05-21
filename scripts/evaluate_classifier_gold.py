"""
Phase 4: 手動ラベル (web_viewer/user_visual/masaru_manual) を gold set として
shadow 分類器の precision/recall/F1 を計測。

Usage:
  python scripts/evaluate_classifier_gold.py --run-id fp_v1_<TS>
  python scripts/evaluate_classifier_gold.py  # 直近 run-id
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
sys.stdout.reconfigure(encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", help="未指定なら最新 run")
    ap.add_argument("--manual-types", default="web_viewer,user_visual,masaru_manual",
                    help="gold とみなす confirmed_by")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    if args.run_id:
        run_id = args.run_id
    else:
        r = conn.execute("SELECT run_id FROM classification_runs ORDER BY created_at DESC LIMIT 1").fetchone()
        if not r:
            print("ERROR: classification_runs にレコードなし。先に classify_pages_fingerprint.py --shadow を実行", file=sys.stderr)
            sys.exit(1)
        run_id = r["run_id"]
    print(f"[init] run_id={run_id}", file=sys.stderr)

    manual_types = [m.strip() for m in args.manual_types.split(",") if m.strip()]
    placeholders = ",".join("?" for _ in manual_types)

    # gold set: 各 page_id について、最新の手動 history で確定したラベル
    gold = list(conn.execute(f"""
        SELECT p.page_id, p.company_id, p.file_name, p.page_no, p.doc_type_name as label
        FROM pages p
        WHERE EXISTS (
            SELECT 1 FROM page_doc_type_history h
            WHERE h.page_id = p.page_id AND h.confirmed_by IN ({placeholders})
        )
        ORDER BY p.page_id
    """, manual_types).fetchall())
    print(f"[gold] manual labels: {len(gold)} pages", file=sys.stderr)

    # 予測を JOIN
    pred_by_pid = {}
    for r in conn.execute(
        "SELECT page_id, predicted_label, confidence, second_label, second_confidence, evidence_json "
        "FROM classification_candidates WHERE run_id=?", (run_id,)
    ).fetchall():
        pred_by_pid[r["page_id"]] = r

    # 評価 (テキスト有無で分割)
    by_class = defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0, "support": 0})
    by_class_no_text = defaultdict(lambda: {"support": 0})  # テキストなしページ数 (分類不能)
    confusion: dict = defaultdict(lambda: defaultdict(int))
    fail_cases = []
    no_pred = 0
    n_no_text = 0

    for g in gold:
        pid = g["page_id"]
        true = g["label"] or "その他/不明"
        if true == "その他/不明":
            continue  # gold としては無視
        pred = pred_by_pid.get(pid)
        if not pred:
            no_pred += 1
            continue
        p_label = pred["predicted_label"]
        ev = json.loads(pred["evidence_json"] or "{}")

        # text_too_short / no_class は OCR 不能ページ → 別集計
        is_no_text = (
            isinstance(ev, dict) and ev.get("reason") in ("text_too_short", "no_class_above_min_score")
            and ev.get("len", 0) < 30
        ) if (isinstance(ev, dict) and ev.get("reason") == "text_too_short") else False

        if is_no_text:
            by_class_no_text[true]["support"] += 1
            n_no_text += 1
            continue

        confusion[true][p_label] += 1
        by_class[true]["support"] += 1
        if p_label == true:
            by_class[true]["tp"] += 1
        else:
            by_class[true]["fn"] += 1
            by_class[p_label]["fp"] += 1
            fail_cases.append({
                "page_id": pid,
                "company_id": g["company_id"],
                "file_name": g["file_name"][:50],
                "page_no": g["page_no"],
                "true": true,
                "pred": p_label,
                "confidence": round(pred["confidence"], 3),
                "second": pred["second_label"] or "",
                "evidence": (pred["evidence_json"] or "")[:200],
            })

    # 集計
    print(f"\n=== Gold Set 評価 (run={run_id}) ===")
    n_with_text = sum(c['support'] for c in by_class.values())
    n_no_text_total = sum(c['support'] for c in by_class_no_text.values())
    print(f"  gold total: {n_with_text + n_no_text_total + no_pred} pages")
    print(f"    テキスト有り: {n_with_text} (= 評価対象)")
    print(f"    テキスト無し: {n_no_text_total} (Azure OCR 必要、評価から除外)")
    print(f"    予測なし:     {no_pred}")
    if by_class_no_text:
        print(f"\n  クラス別 テキスト無しページ数:")
        for cls, c in sorted(by_class_no_text.items(), key=lambda x: -x[1]["support"]):
            print(f"    {cls:25s} {c['support']}")
    print()
    print(f"{'class':25s} {'sup':>5} {'TP':>5} {'FP':>5} {'FN':>5} {'prec':>6} {'recall':>7} {'F1':>6}")

    total_tp = total_fp = total_fn = 0
    macro_p = macro_r = macro_f = 0.0
    n_classes = 0

    for cls in sorted(by_class.keys()):
        c = by_class[cls]
        sup = c["support"]
        tp = c["tp"]
        fp = c["fp"]
        fn = c["fn"]
        total_tp += tp
        total_fp += fp
        total_fn += fn
        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec = tp / sup if sup > 0 else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0.0
        if sup > 0:
            macro_p += prec
            macro_r += rec
            macro_f += f1
            n_classes += 1
        print(f"{cls:25s} {sup:>5d} {tp:>5d} {fp:>5d} {fn:>5d} {prec:>6.3f} {rec:>7.3f} {f1:>6.3f}")

    micro_p = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0.0
    micro_r = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0.0
    micro_f = 2 * micro_p * micro_r / (micro_p + micro_r) if (micro_p + micro_r) > 0 else 0.0

    print()
    print(f"{'micro avg':25s} {'':>5} {total_tp:>5d} {total_fp:>5d} {total_fn:>5d} {micro_p:>6.3f} {micro_r:>7.3f} {micro_f:>6.3f}")
    if n_classes > 0:
        print(f"{'macro avg':25s} {'':>5} {'':>5} {'':>5} {'':>5} {macro_p/n_classes:>6.3f} {macro_r/n_classes:>7.3f} {macro_f/n_classes:>6.3f}")

    # 混同行列
    classes = sorted(set(list(by_class.keys()) + [k for v in confusion.values() for k in v.keys()]))
    print(f"\n=== 混同行列 (横=正解, 縦=予測) ===")
    print(f"{'true \\ pred':25s} | " + " ".join(f"{c[:6]:>6s}" for c in classes))
    print("-" * (28 + 7 * len(classes)))
    for t in classes:
        if t not in confusion:
            continue
        row = f"{t:25s} | "
        for p in classes:
            n = confusion[t].get(p, 0)
            row += f"{n:>6d} "
        print(row)

    # CSV 出力
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = PROJECT / "data"
    fail_path = out_dir / f"gold_eval_failures_{run_id}_{ts}.csv"
    with open(fail_path, "w", encoding="utf-8-sig", newline="") as f:
        if fail_cases:
            w = csv.DictWriter(f, fieldnames=list(fail_cases[0].keys()))
            w.writeheader()
            w.writerows(fail_cases)
    print(f"\n失敗ケース CSV: {fail_path}  ({len(fail_cases)} 件)")

    # classification_runs に集計を保存
    conn.execute(
        "UPDATE classification_runs SET gold_precision=?, gold_recall=? WHERE run_id=?",
        (round(micro_p, 4), round(micro_r, 4), run_id)
    )
    conn.commit()

    print(f"\n*** 期待値 ***")
    print(f"  precision >= 0.95 (現在 {micro_p:.3f}): {'OK' if micro_p >= 0.95 else 'NG (要 YAML 改善)'}")
    print(f"  recall    >= 0.85 (現在 {micro_r:.3f}): {'OK' if micro_r >= 0.85 else 'NG (要 YAML 改善)'}")


if __name__ == "__main__":
    main()
