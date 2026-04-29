"""
Phase R-C-3: 抽出した EXEMPTION 候補 + 既知 exemption を company_doc_exemptions に登録。

GPT-5.5 Issue 5 反映:
  既知 exemption (三和シヤッター 工事経歴書 提出不可 等) と
  Phase R-C で抽出した EXEMPTION 19 件を機械的に登録し、
  Phase R-E (wb 生成) 時に業務判定をクリーンに算出する。

入力:
  - corrections CSV (Phase R-C-1 出力)
  - 既知 exemption (本スクリプト内に hardcode、後で外出し可能)

処理:
  - 各候補を `company_doc_exemptions` に INSERT (UNIQUE 違反 = skip)
  - confirmed_by に出典 (auto_proposal_R-C / known_policy_C0008 等) を残す

dry-run / --execute 切替。

Usage:
  python scripts/register_exemptions.py [--corrections-csv <CSV>] [--execute]
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "permit_tracker.db"
CORRECTIONS_DEFAULT = PROJECT / "output" / "FDE_MANAGED" / "10_corrections_classified_20260429_phaseRC1_v2.csv"

# 既知の exemption (会社単位で恒久的に成立する免除)
# 出典: 過去藤田さんとの会話/メール
KNOWN_EXEMPTIONS = [
    {
        "company_id": "C0008",
        "document_type": "工事経歴書",
        "exempt_kind": "NOT_APPLICABLE",
        "reason": "三和シヤッター: 工事経歴書は官公庁限定で提出不可 (4/24 吉野様ご回答)",
        "confirmed_by": "known_policy_C0008",
    },
    {
        "company_id": "C0060",
        "document_type": "工事経歴書",
        "exempt_kind": "NOT_APPLICABLE",
        "reason": "井上商会: 工事経歴書お断り (2026-04-29 藤田指摘)",
        "confirmed_by": "known_policy_C0060",
    },
    {"company_id": "C0110", "document_type": "工事経歴書", "exempt_kind": "NOT_APPLICABLE",
     "reason": "北恵 卸業者のため提出不要 (2026-04-29 藤田指摘)", "confirmed_by": "known_policy_C0110"},
    {"company_id": "C0110", "document_type": "資格略字一覧", "exempt_kind": "NOT_APPLICABLE",
     "reason": "北恵 卸業者のため提出不要 (2026-04-29 藤田指摘)", "confirmed_by": "known_policy_C0110"},
    {"company_id": "C0110", "document_type": "労働者名簿", "exempt_kind": "NOT_APPLICABLE",
     "reason": "北恵 卸業者のため提出不要 (2026-04-29 藤田指摘)", "confirmed_by": "known_policy_C0110"},
    {"company_id": "C0132", "document_type": "工事経歴書", "exempt_kind": "NOT_APPLICABLE",
     "reason": "岡崎製材 卸業者のため提出不要 (2026-04-29 藤田指摘)", "confirmed_by": "known_policy_C0132"},
    {"company_id": "C0132", "document_type": "資格略字一覧", "exempt_kind": "NOT_APPLICABLE",
     "reason": "岡崎製材 卸業者のため提出不要 (2026-04-29 藤田指摘)", "confirmed_by": "known_policy_C0132"},
    {"company_id": "C0132", "document_type": "労働者名簿", "exempt_kind": "NOT_APPLICABLE",
     "reason": "岡崎製材 卸業者のため提出不要 (2026-04-29 藤田指摘)", "confirmed_by": "known_policy_C0132"},
    {"company_id": "C0133", "document_type": "工事経歴書", "exempt_kind": "NOT_APPLICABLE",
     "reason": "後藤木材 卸業者のため提出不要 (2026-04-29 藤田指摘)", "confirmed_by": "known_policy_C0133"},
    {"company_id": "C0133", "document_type": "労働安全衛生誓約書", "exempt_kind": "NOT_APPLICABLE",
     "reason": "後藤木材 卸業者のため提出不要 (2026-04-29 藤田指摘)", "confirmed_by": "known_policy_C0133"},
    {"company_id": "C0133", "document_type": "資格略字一覧", "exempt_kind": "NOT_APPLICABLE",
     "reason": "後藤木材 卸業者のため提出不要 (2026-04-29 藤田指摘)", "confirmed_by": "known_policy_C0133"},
    {"company_id": "C0133", "document_type": "労働者名簿", "exempt_kind": "NOT_APPLICABLE",
     "reason": "後藤木材 卸業者のため提出不要 (2026-04-29 藤田指摘)", "confirmed_by": "known_policy_C0133"},
]


def load_corrections(csv_path: Path) -> list[dict]:
    out = []
    with csv_path.open("r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            out.append(row)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corrections-csv", type=Path, default=CORRECTIONS_DEFAULT)
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    print(f"=== Phase R-C-3: exemption 自動登録 ===")
    print(f"  mode: {'EXECUTE' if args.execute else 'DRY-RUN'}")

    if not args.corrections_csv.exists():
        print(f"  WARN: corrections CSV not found, 既知 exemption のみ処理")
        corrections_exempts = []
    else:
        rows = load_corrections(args.corrections_csv)
        corrections_exempts = []
        for r in rows:
            if r.get("category") != "EXEMPTION":
                continue
            cid = r.get("company_id") or ""
            if not cid:
                continue
            doc = r.get("doc_type") or ""
            # doc_type の正規化 (DOC_COLS に揃える)
            DOC_COLS = ["取引申請書", "建設業許可証", "決算書", "工事経歴書",
                         "取引先一覧表", "労働安全衛生誓約書", "資格略字一覧", "労働者名簿"]
            doc_normalized = next((dc for dc in DOC_COLS if dc in doc or doc in dc), doc)
            if doc_normalized not in DOC_COLS:
                # 会社単位の包括 exemption は skip (個別書類対象外)
                continue
            corrections_exempts.append({
                "company_id": cid,
                "document_type": doc_normalized,
                "exempt_kind": r.get("sub_kind") or "NOT_APPLICABLE",
                "reason": f"Phase R-C 自動抽出: {r.get('full_comment', '')[:200]}",
                "confirmed_by": "auto_proposal_R-C",
            })

    all_targets = KNOWN_EXEMPTIONS + corrections_exempts
    print(f"\n登録予定:")
    print(f"  既知 exemption: {len(KNOWN_EXEMPTIONS)} 件")
    print(f"  Phase R-C 抽出: {len(corrections_exempts)} 件")
    print(f"  合計:          {len(all_targets)} 件")

    print(f"\n上位 10:")
    for t in all_targets[:10]:
        print(f"  {t['company_id']} | {t['document_type']:<14} | {t['exempt_kind']:<14} | {t['confirmed_by']}")

    if not args.execute:
        print("\n[DRY-RUN] DB は変更されません。--execute で INSERT。")
        return

    conn = sqlite3.connect(DB_PATH)
    inserted = 0
    skipped_dup = 0
    try:
        conn.execute("BEGIN")
        for t in all_targets:
            try:
                conn.execute(
                    "INSERT INTO company_doc_exemptions "
                    "(company_id, document_type, exempt_kind, reason, confirmed_by) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (t["company_id"], t["document_type"], t["exempt_kind"],
                     t["reason"], t["confirmed_by"]),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                skipped_dup += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"  EXCEPTION: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()

    print(f"\n=== EXECUTE 完了 ===")
    print(f"  INSERT: {inserted} 件")
    print(f"  duplicate skip: {skipped_dup} 件")
    print()
    print("次のステップ:")
    print("  1. python scripts/classify_business_status.py で業務判定再算出")
    print("  2. python scripts/build_editor_workbook_v2.py で wb 再生成")


if __name__ == "__main__":
    main()
