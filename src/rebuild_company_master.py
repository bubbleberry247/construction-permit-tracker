"""
rebuild_company_master.py — メール起点で会社マスタを再構築する。

EMAIL_TO_COMPANY マッピング（手動確定済み）を正として、
既存 company_master.csv にメアドを紐付け＋新規会社を追加する。

Usage:
    python src/rebuild_company_master.py
    python src/rebuild_company_master.py --dry-run
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import tempfile
import unicodedata
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent.parent

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
MASTER_HEADERS: list[str] = [
    "company_id",
    "official_name",
    "name_aliases",
    "corporation_type",
    "sender_emails",
    "contact_person",
    "permit_number",
    "permit_authority",
    "mlit_status",
    "last_confirmed_at",
    "status",
    "created_at",
    "updated_at",
]

EMAIL_MAP_HEADERS: list[str] = [
    "sender_email",
    "company_id",
    "match_source",
    "confidence",
    "created_at",
]

# ---------------------------------------------------------------------------
# Ground truth: email → company name (48 vendors + 1 accountant → しごとラボ)
# ---------------------------------------------------------------------------
EMAIL_TO_COMPANY: dict[str, str] = {
    # --- 26 already matched (HIGH) ---
    "a-satomi@tohoku-kigyo.co.jp": "東北企業株式会社",
    "asano@suzukisangyo.com": "株式会社鈴木産業",
    "chubu-k@voice.ocn.ne.jp": "有限会社中部工芸",
    "fukama-keiri@katch.ne.jp": "福釜電工株式会社",
    "goemon1230@gmail.com": "株式会社メンテック",
    "hkondo@omori-mokuzai.co.jp": "大森木材株式会社",
    "info@r-coating.jp": "株式会社レジンテクニカ",
    "iwata.maki@iwata-glass.co.jp": "岩田硝子株式会社",
    "juna@matusima-kaitai.com": "有限会社松島商店",
    "k_mizuno@d-m-b.co.jp": "大日本木材防腐株式会社",
    "keecompanycoltd@gmail.com": "有限会社キー・カンパニー",
    "keiri-ishida@ishida-setsubi.com": "石田設備株式会社",
    "m-ichikawa@ansei.biz": "安成工業株式会社",
    "moto.hs@aurora.ocn.ne.jp": "株式会社ハジメサービス",
    "n.harima@yokogawa-yess.co.jp": "株式会社横河ブリッジシステム建築",
    "n.masuda@kyowa-el.jp": "協和電気工業株式会社",
    "niimikougyou@live.jp": "株式会社新美興業",
    "rika@nagasaka-kk.co.jp": "長坂建設興業株式会社",
    "soumu@kyouritsubousai.biz": "共立防災工事株式会社",
    "tk-h-72@nifty.com": "株式会社夏目住宅",
    "toyota@yamanishi.co.jp": "株式会社山西",
    "watanabe@kondo-eec.com": "株式会社近藤電工社",
    "y-goto@komaki-tohken.com": "有限会社トーケン",
    "yamada_1@katch.ne.jp": "有限会社山田工場",
    "yamaguchi-nana@kkis.co.jp": "株式会社石田産業",
    "yu_take_home@yahoo.co.jp": "有限会社竹下建築",
    # --- 21 newly identified ---
    "daikou_kentiku@msn.com": "有限会社大功建築",
    "info@yoshidadenki-kk.co.jp": "株式会社吉田電気工事",
    "jimu.toyohashi@sanshin-g.co.jp": "三信建材工業株式会社",
    "k-yamauchi@inoues.co.jp": "株式会社井上商会",
    "k.y@yamajipt.com": "有限会社YAMAJIパーティション",
    "kawaguchi-kiso@outlook.jp": "株式会社川口",
    "kyoko@pal-watanabe.jp": "株式会社パルカンパニー渡辺",
    "murai@chitajyuki.co.jp": "有限会社知多重機",
    "nagoya@meisei-kenzai.co.jp": "名西建材株式会社",
    "nakamitsu@katch.ne.jp": "ナカミツホームテック",
    "okakenokada@ma.medias.ne.jp": "株式会社オカケン",
    "samtec2016@outlook.jp": "サムテック",
    "shiro@4-star.jp": "株式会社フォースター",
    "somu@chiyoda-cic.jp": "千代田コンクリート工業株式会社",
    "takahashi@takagumi.com": "株式会社高橋組",
    "tilt-kazuma@katch.ne.jp": "株式会社チルト",
    "toyotetsu.takeda@gmail.com": "豊鉄工業株式会社",
    "tsuruokay@sip.sanwa-ss.co.jp": "三和シャッター工業株式会社",
    "ymk_maehatakoumuten@yahoo.co.jp": "有限会社前畑工務店",
    "yoshinosh@sip.sanwa-ss.co.jp": "三和シャッター工業株式会社",
    "yui_takahashi@s-thing.co.jp": "株式会社サムシング",
    # --- accountant for しごとラボ ---
    "ito-zeimu713@sunny.ocn.ne.jp": "株式会社しごとラボ",
    # --- 伊藤建設 (既存C0047にメアド紐付け) ---
    "k-itoken@sk2.aitai.ne.jp": "株式会社伊藤建設",
    # --- 非PDF添付メール由来 (ZIP/XLSX/クラウドDL) ---
    "okazaki@taninomiya.co.jp": "谷ノ宮",
    "tomida-y@infratec.co.jp": "株式会社インフラテック",
    "ku-matsumoto@ota-shoji.co.jp": "太田商事株式会社",
    "keiri@hashimoto-denki.jp": "橋本電機株式会社",
    "c-kenso@katch.ne.jp": "中部建装株式会社",
    "h.hayashi@akari-den.com": "株式会社あかり電工社",
    "kouzou.0425@katch.ne.jp": "庭昭",
    "k-kamiya@ootake.co.jp": "株式会社大嶽安城",
    "kiyomi.yokoi@gterior.co.jp": "Gテリア株式会社",
    "n.arimura@suzuki1963.co.jp": "株式会社スズキ",
    "hiroko-i@k-ishihara.net": "株式会社イシハラ",
    "daisuke@ifjp.com": "株式会社イフ",
    "hironn@yk.commufa.jp": "新美総合建設",
}

# Emails to SKIP (not vendors — not forwarded from shinsei.tic)
SKIP_EMAILS: set[str] = {
    "t-yamaguchi@acogroup.co.jp",  # kalimistk直接メール、shinsei.tic転送ではない
}

# ---------------------------------------------------------------------------
# Explicit overrides: email → existing company_id
# For cases where name normalization alone won't find the match.
# ---------------------------------------------------------------------------
EXPLICIT_EMAIL_TO_CID: dict[str, str] = {
    # 「株式会社吉田電気工事」 → C0013「吉田電気工事株式会社」(法人格の位置が違う)
    "info@yoshidadenki-kk.co.jp": "C0013",
    # 「株式会社フォースター」 → C0043「株式会社フォースター」(master has C0036 4スター別, C0043 フォースター)
    "shiro@4-star.jp": "C0043",
    # 「株式会社山西」 → C0048「株式会社山西 豊田店」
    "toyota@yamanishi.co.jp": "C0048",
    # 「三和シャッター工業株式会社」 → C0008「三和シャッター工業株式会社 刈谷統括営業所」
    "tsuruokay@sip.sanwa-ss.co.jp": "C0008",
    "yoshinosh@sip.sanwa-ss.co.jp": "C0008",
    # 「名西建材株式会社」 → C0014
    "nagoya@meisei-kenzai.co.jp": "C0014",
    # 「株式会社しごとラボ」 → C0038
    "ito-zeimu713@sunny.ocn.ne.jp": "C0038",
    # 「株式会社伊藤建設」 → C0047
    "k-itoken@sk2.aitai.ne.jp": "C0047",
    # 「豊鉄工業株式会社」 → C0057「豊錦工業株式会社」(OCR漢字誤り → 修正)
    "toyotetsu.takeda@gmail.com": "C0057",
}

# ---------------------------------------------------------------------------
# Name corrections to apply to existing master before processing
# ---------------------------------------------------------------------------
NAME_CORRECTIONS: dict[str, str] = {
    "C0057": "豊鉄工業株式会社",  # 豊錦 → 豊鉄 (OCR誤り修正)
}

# ---------------------------------------------------------------------------
# Non-vendor companies to mark INACTIVE
# ---------------------------------------------------------------------------
INACTIVE_IDS: dict[str, str] = {
    "C0003": "非建設業（NTPシステム）",
    "C0006": "OCR誤抽出（サムテックのPDFからゴールドトラスト）",
    "C0007": "OCR誤読（三信建材工業→三信建設工業、C0059と同一）",
    "C0009": "代表者名誤認（ナカミツホームテックC0063の中根さん）",
    "C0020": "非建設業（IT協同組合）",
    "C0032": "発注者（東海インプル建設＝自社）",
    "C0033": "グループ会社（アルファホーム）",
    "C0036": "同一会社（C0043フォースター＝4スター）",
    "C0037": "非建設業（PCワールド）",
    "C0039": "OCR誤抽出（オカケンのPDFからカカシ）",
    "C0040": "非建設業（クオカード）",
    "C0041": "OCR誤読（チルト→タイト、C0068と同一）",
    "C0046": "OCR誤読（井上商会→上商会、C0060と同一）",
}


# ---------------------------------------------------------------------------
# Utility functions (same pattern as generate_company_master.py)
# ---------------------------------------------------------------------------
def normalize_name(name: str) -> str:
    """会社名を正規化する（マッチング用）。NFKC + 空白除去 + 法人格略称展開。"""
    if not name:
        return ""
    normalized = unicodedata.normalize("NFKC", name)
    normalized = re.sub(r"\s+", "", normalized)
    normalized = normalized.replace("(株)", "株式会社")
    normalized = normalized.replace("(有)", "有限会社")
    normalized = normalized.replace("(合)", "合同会社")
    return normalized


def classify_corporation(name: str) -> str:
    """法人区分を判定する。"""
    corp_markers = [
        "株式会社", "有限会社", "合同会社", "合資会社", "合名会社",
        "一般社団法人", "一般財団法人", "協同組合",
    ]
    for marker in corp_markers:
        if marker in name:
            return "CORPORATION"
    return "SOLE_PROPRIETOR"


def safe_write_csv(
    path: Path,
    headers: list[str],
    rows: list[dict[str, str]],
) -> None:
    """一時ファイルに書き込み後、原子的にリネームする (UTF-8 BOM)。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        suffix=".csv",
        prefix=path.stem + "_tmp_",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        if path.exists():
            path.unlink()
        Path(tmp_path).rename(path)
    except Exception:
        try:
            Path(tmp_path).unlink()
        except OSError:
            pass
        raise


def load_existing_master(path: Path) -> list[dict[str, str]]:
    """既存の company_master.csv を読み込む。"""
    if not path.exists():
        print(f"  WARNING: {path} not found, starting from empty master")
        return []
    rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


def get_max_company_id(rows: list[dict[str, str]]) -> int:
    """既存マスタから最大の company_id 番号を取得する。"""
    max_num = 0
    for row in rows:
        cid = row.get("company_id", "")
        if cid.startswith("C") and cid[1:].isdigit():
            max_num = max(max_num, int(cid[1:]))
    return max_num


# ---------------------------------------------------------------------------
# Build lookup indices for matching
# ---------------------------------------------------------------------------
def build_name_index(
    rows: list[dict[str, str]],
) -> dict[str, int]:
    """正規化名 → rows内インデックス のマッピングを構築する。

    official_name と name_aliases（pipe区切り）両方を登録。
    """
    index: dict[str, int] = {}
    for i, row in enumerate(rows):
        # official_name
        norm = normalize_name(row.get("official_name", ""))
        if norm:
            index[norm] = i

        # name_aliases (pipe separated)
        aliases = row.get("name_aliases", "")
        if aliases:
            for alias in aliases.split("|"):
                alias_norm = normalize_name(alias.strip())
                if alias_norm:
                    index[alias_norm] = i

    return index


def build_cid_index(rows: list[dict[str, str]]) -> dict[str, int]:
    """company_id → rows内インデックス のマッピング。"""
    return {row["company_id"]: i for i, row in enumerate(rows) if row.get("company_id")}


def add_email_to_company(row: dict[str, str], email: str) -> None:
    """会社行の sender_emails にメアドを追加する（重複なし、pipe区切り）。"""
    existing = row.get("sender_emails", "") or ""
    existing_set = set(e.strip() for e in existing.split("|") if e.strip())
    if email not in existing_set:
        existing_set.add(email)
        row["sender_emails"] = "|".join(sorted(existing_set))


def make_new_company(
    company_id: str,
    official_name: str,
    email: str,
    now_str: str,
) -> dict[str, str]:
    """新規会社行を作成する。"""
    return {
        "company_id": company_id,
        "official_name": official_name,
        "name_aliases": "",
        "corporation_type": classify_corporation(official_name),
        "sender_emails": email,
        "contact_person": "",
        "permit_number": "",
        "permit_authority": "",
        "mlit_status": "NOT_CONFIRMED",
        "last_confirmed_at": "",
        "status": "ACTIVE",
        "created_at": now_str,
        "updated_at": now_str,
    }


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------
def rebuild(master_path: Path, email_map_path: Path, *, dry_run: bool = False) -> None:
    now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # 1. Load existing master
    print(f"[1] Loading existing master: {master_path}")
    rows = load_existing_master(master_path)
    print(f"    Loaded {len(rows)} companies")

    # 2. Apply name corrections
    cid_idx = build_cid_index(rows)
    for cid, corrected_name in NAME_CORRECTIONS.items():
        if cid in cid_idx:
            old_name = rows[cid_idx[cid]]["official_name"]
            rows[cid_idx[cid]]["official_name"] = corrected_name
            rows[cid_idx[cid]]["updated_at"] = now_str
            print(f"    CORRECTED: {cid} '{old_name}' -> '{corrected_name}'")

    # 3. Build lookup indices
    name_idx = build_name_index(rows)
    cid_idx = build_cid_index(rows)  # rebuild after corrections

    # 4. Process each email-company pair
    next_id_num = get_max_company_id(rows) + 1
    email_map_rows: list[dict[str, str]] = []
    stats = {"matched": 0, "new": 0, "skipped": 0, "explicit": 0}

    print(f"\n[2] Processing {len(EMAIL_TO_COMPANY)} email-company mappings...")

    for email, company_name in sorted(EMAIL_TO_COMPANY.items()):
        target_norm = normalize_name(company_name)

        # Check explicit override first
        if email in EXPLICIT_EMAIL_TO_CID:
            target_cid = EXPLICIT_EMAIL_TO_CID[email]
            if target_cid in cid_idx:
                idx = cid_idx[target_cid]
                add_email_to_company(rows[idx], email)
                rows[idx]["updated_at"] = now_str
                # Also add as alias if name is different
                existing_norm = normalize_name(rows[idx]["official_name"])
                if existing_norm != target_norm:
                    existing_aliases = rows[idx].get("name_aliases", "") or ""
                    alias_norms = {normalize_name(a) for a in existing_aliases.split("|") if a.strip()}
                    if target_norm not in alias_norms and target_norm != existing_norm:
                        if existing_aliases:
                            rows[idx]["name_aliases"] = existing_aliases + "|" + company_name
                        else:
                            rows[idx]["name_aliases"] = company_name
                print(f"    EXPLICIT: {email} -> {target_cid} ({rows[idx]['official_name']})")
                email_map_rows.append({
                    "sender_email": email,
                    "company_id": target_cid,
                    "match_source": "manual_verified",
                    "confidence": "HIGH",
                    "created_at": now_str,
                })
                stats["explicit"] += 1
                continue

        # Try normalized name match
        matched_idx = name_idx.get(target_norm)
        if matched_idx is not None:
            row = rows[matched_idx]
            add_email_to_company(row, email)
            row["updated_at"] = now_str
            cid = row["company_id"]
            print(f"    MATCHED:  {email} -> {cid} ({row['official_name']})")
            email_map_rows.append({
                "sender_email": email,
                "company_id": cid,
                "match_source": "manual_verified",
                "confidence": "HIGH",
                "created_at": now_str,
            })
            stats["matched"] += 1
            continue

        # No match → create new company
        new_cid = f"C{next_id_num:04d}"
        next_id_num += 1
        new_row = make_new_company(new_cid, company_name, email, now_str)
        rows.append(new_row)
        # Update indices
        name_idx[target_norm] = len(rows) - 1
        cid_idx[new_cid] = len(rows) - 1
        print(f"    NEW:      {email} -> {new_cid} ({company_name})")
        email_map_rows.append({
            "sender_email": email,
            "company_id": new_cid,
            "match_source": "manual_verified",
            "confidence": "HIGH",
            "created_at": now_str,
        })
        stats["new"] += 1

    # 5. Apply INACTIVE status to non-vendor companies
    for row in rows:
        cid = row.get("company_id", "")
        if cid in INACTIVE_IDS:
            row["status"] = "INACTIVE"

    # 6. Sort rows by company_id
    rows.sort(key=lambda r: r.get("company_id", ""))
    email_map_rows.sort(key=lambda r: r.get("sender_email", ""))

    # 6. Summary
    print(f"\n[3] Summary:")
    print(f"    Total companies:      {len(rows)}")
    print(f"    Existing matched:     {stats['matched']}")
    print(f"    Explicit overrides:   {stats['explicit']}")
    print(f"    New additions:        {stats['new']}")
    print(f"    Email mappings:       {len(email_map_rows)}")

    # List new companies
    if stats["new"] > 0:
        print(f"\n    New companies added:")
        for row in rows:
            if row.get("created_at") == now_str and row["company_id"] >= "C0059":
                print(f"      {row['company_id']}: {row['official_name']}")

    # List companies without email mapping
    no_email = [r for r in rows if not r.get("sender_emails")]
    if no_email:
        print(f"\n    Companies without email mapping ({len(no_email)}):")
        for r in no_email:
            print(f"      {r['company_id']}: {r['official_name']}")

    # 7. Write output
    if dry_run:
        print("\n[DRY RUN] No files written.")
        return

    print(f"\n[4] Writing output...")
    safe_write_csv(master_path, MASTER_HEADERS, rows)
    print(f"    Written: {master_path} ({len(rows)} companies)")

    safe_write_csv(email_map_path, EMAIL_MAP_HEADERS, email_map_rows)
    print(f"    Written: {email_map_path} ({len(email_map_rows)} mappings)")

    print("\nDone.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebuild company_master.csv from email-based ground truth",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without writing files",
    )
    parser.add_argument(
        "--master",
        type=Path,
        default=_PROJECT_ROOT / "output" / "company_master.csv",
        help="Path to company_master.csv",
    )
    parser.add_argument(
        "--email-map",
        type=Path,
        default=_PROJECT_ROOT / "output" / "email_company_map.csv",
        help="Path to email_company_map.csv",
    )
    args = parser.parse_args()
    rebuild(args.master, args.email_map, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
