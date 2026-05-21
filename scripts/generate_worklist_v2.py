"""
GPT-5.5 推奨フローに基づく統合 worklist CSV 生成。

タスク1 (xlsx 10 社) + タスク2 (fingerprint 不一致) のオーバーラップを
1 つの CSV にまとめ、優先順位とビューワー直リンクを含める。

優先順位 (GPT-5.5 推奨):
  1. ◎全揃い改善見込みの会社 (complete_gain)
  2. xlsx 対象 10 社
  3. 高 confidence (>=0.93) かつ上位パターン (pattern別 approve 候補)
  4. ◎全揃いに近い会社
  5. 中間 confidence (0.85-0.92) は影響大のみ

出力: data/worklist_v2_<TS>.csv
  - approve 列にユーザーが Y を入れたら採用候補
  - viewer_url 列クリックで即ジャンプ

Usage:
  python scripts/generate_worklist_v2.py
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
from urllib.parse import quote

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
sys.stdout.reconfigure(encoding="utf-8")

XLSX_TARGETS = ["C0029", "C0062", "C0077", "C0079", "C0082", "C0083", "C0118", "C0147", "C0149", "C0150"]
REQUIRED = ["取引申請書", "建設業許可証", "決算書", "会社案内", "工事経歴書", "取引先一覧表",
            "労働安全衛生誓約書", "資格略字一覧", "労働者名簿"]

VIEWER_BASE = "http://127.0.0.1:9000/viewer"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id")
    ap.add_argument("--confidence-min", type=float, default=0.85)
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    if args.run_id:
        run_id = args.run_id
    else:
        # 最大 pages_processed の最新 run を選択
        r = conn.execute(
            "SELECT run_id FROM classification_runs WHERE pages_processed > 1000 ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        run_id = r["run_id"] if r else None
    print(f"[init] run_id={run_id}", file=sys.stderr)

    # 各社の不足書類を計算 (doc_type_secondary も考慮)
    missing_by_cid: dict[str, set] = {}
    for r in conn.execute("SELECT DISTINCT company_id FROM pages").fetchall():
        cid = r["company_id"]
        found: set = set()
        for l in conn.execute(
            "SELECT DISTINCT doc_type_name FROM pages WHERE company_id=?", (cid,)
        ).fetchall():
            if l[0]:
                found.add(l[0])
        for l in conn.execute(
            "SELECT DISTINCT doc_type_secondary FROM pages WHERE company_id=? AND doc_type_secondary IS NOT NULL",
            (cid,)
        ).fetchall():
            if l[0]:
                found.add(l[0])
        missing_by_cid[cid] = set(REQUIRED) - found

    # 不一致候補
    rows = list(conn.execute(
        """SELECT cc.page_id, cc.predicted_label, cc.confidence,
                  cc.second_label, cc.second_confidence, cc.evidence_json,
                  p.company_id, c.official_name, p.file_name, p.page_no,
                  p.doc_type_name AS current_doc_type
           FROM classification_candidates cc
           JOIN pages p USING(page_id)
           JOIN companies c ON c.company_id = p.company_id
           WHERE cc.run_id=?
             AND cc.manual_locked = 0
             AND cc.predicted_label != 'その他/不明'
             AND p.doc_type_name != cc.predicted_label
             AND cc.confidence >= ?
           ORDER BY cc.confidence DESC""",
        (run_id, args.confidence_min)
    ).fetchall())

    # 加工
    items: list[dict] = []
    for r in rows:
        try:
            ev = json.loads(r["evidence_json"] or "{}")
            tq = ev.get("text_quality", "normal")
        except Exception:
            ev = {}
            tq = "normal"
        if tq == "low_text":
            continue
        cur = r["current_doc_type"]
        pred = r["predicted_label"]
        if cur == "その他/不明":
            category = "unknown_to_known"
        elif cur == "空白":
            category = "blank_to_known"
        elif cur == pred:
            continue
        else:
            category = "known_to_different"

        cid = r["company_id"]
        missing = missing_by_cid.get(cid, set())
        # この修正が ◎全揃いに効くか
        helps_complete = pred in missing
        # 優先度スコア (大きいほど優先)
        priority = 0
        if helps_complete and len(missing) <= 2:
            priority = 100  # 1-2 不足 + この修正で埋まる → 最優先
        elif helps_complete:
            priority = 80   # 不足を一部解消
        elif cid in XLSX_TARGETS:
            priority = 60   # xlsx 対象社
        elif r["confidence"] >= 0.93 and category == "known_to_different":
            priority = 40   # 高 conf パターン候補
        else:
            priority = 20

        # viewer URL
        url = f'{VIEWER_BASE}/{cid}?filter=all&focus={r["page_id"]}'

        items.append({
            "priority": priority,
            "category": category,
            "company_id": cid,
            "official_name": r["official_name"],
            "xlsx_target": "Y" if cid in XLSX_TARGETS else "",
            "page_id": r["page_id"],
            "page_no": r["page_no"],
            "file_name": r["file_name"][:50],
            "current": cur,
            "predicted": pred,
            "confidence": round(r["confidence"], 3),
            "text_quality": tq,
            "missing_count": len(missing),
            "missing_docs": " / ".join(sorted(missing)) if missing else "",
            "helps_complete": "Y" if helps_complete else "",
            "second_label": r["second_label"] or "",
            "viewer_url": url,
            "approve": "",  # ユーザーが Y を入れたら採用候補
        })

    # ソート: priority DESC, helps_complete (Y first), confidence DESC
    items.sort(key=lambda x: (-x["priority"], x["helps_complete"] != "Y", -x["confidence"]))

    # 出力
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = PROJECT / "data" / f"worklist_v2_{ts}.csv"
    if items:
        with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(items[0].keys()))
            w.writeheader()
            w.writerows(items)

    # サマリ
    print(f"\n=== Worklist v2 サマリ ===")
    print(f"  全件: {len(items)}")
    by_pri = Counter(i["priority"] for i in items)
    print(f"\n  優先度別:")
    print(f"    100 (◎達成見込み):       {by_pri[100]}")
    print(f"    80 (一部不足解消):       {by_pri[80]}")
    print(f"    60 (xlsx 対象):          {by_pri[60]}")
    print(f"    40 (高 conf パターン):   {by_pri[40]}")
    print(f"    20 (その他):             {by_pri[20]}")

    print(f"\n  カテゴリ別:")
    by_cat = Counter(i["category"] for i in items)
    for c, n in by_cat.most_common():
        print(f"    {c:25s} {n}")

    # 上位パターン
    pat_count = Counter(f'{i["current"]} -> {i["predicted"]}' for i in items if i["category"] == "known_to_different")
    print(f"\n  known→different 上位パターン:")
    for p, n in pat_count.most_common(8):
        print(f"    {p:50s} {n}")

    # ◎全揃い改善見込み社
    complete_gain = [i for i in items if i["priority"] == 100]
    cg_cids = sorted({i["company_id"] for i in complete_gain})
    print(f"\n  ◎全揃い改善見込み 会社: {len(cg_cids)}")
    for cid in cg_cids:
        miss = next((i["missing_docs"] for i in items if i["company_id"] == cid), "")
        n_p = sum(1 for i in items if i["company_id"] == cid and i["priority"] == 100)
        print(f"    {cid} (修正候補 {n_p} 件 / 不足: {miss})")

    print(f"\n出力: {out_path}")
    print(f"\n進め方:")
    print(f"  1. Excel で {out_path.name} を開く")
    print(f"  2. priority 順に viewer_url をクリック → 即ジャンプ")
    print(f"  3. ビューワー右パネルで書類種別を変更 → web_viewer で自動保存")
    print(f"  4. approve 列に Y を入れた行を後で集計可能")


if __name__ == "__main__":
    main()
