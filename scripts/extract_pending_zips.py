"""
Phase 1: Gmail で受信済み ZIP 添付 (inbound_log.csv の PENDING) を解凍し、
内包ファイルを company_id 推定 + originals 配置 + files INSERT。
pages テーブルへの INSERT は別フェーズ (Phase 2)。

責務分離:
  - 解凍 + manifest 生成 + originals 配置 + files INSERT のみ
  - pages 登録 / 分類 / xlsx→PDF 変換 は別スクリプト

設計鉄則 (GPT-5.5 レビュー反映):
  - file_hash 主体の冪等性
  - 保存名 {zipstem}__{innerstem}_{hash8}.{ext} で UNIQUE 衝突回避
  - process_status 6 状態 (EXTRACTING/EXTRACTED/PARTIAL_EXTRACTED/FAILED_EXTRACT/FAILED_MATCH/DUPLICATE_SKIPPED)
  - inbound_messages / receipt_events は新規作成しない (Gmail 受信時点で 1 message)
  - ネスト ZIP: depth=2, size 200MB, count 200, path traversal 拒否
  - 手動ラベル絶対保護 (このスクリプトは pages 触らないので自動的に保護)

Usage:
  python scripts/extract_pending_zips.py --dry-run
  python scripts/extract_pending_zips.py --execute
  python scripts/extract_pending_zips.py --execute --zip-name 継続取引申請書.zip
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import uuid
import zipfile
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_fujita_mapping import normalize_steps, fully_normalized

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
INBOX = PROJECT / "data" / "inbox"
ORIGINALS = PROJECT / "data" / "originals"
LOGS = PROJECT / "logs"
INBOUND_LOG = LOGS / "inbound_log.csv"

# 保存先カテゴリ (import_classified_inbox.py から流用)
CATEGORY_FOLDER = {
    "取引申請書": "01_取引申請書",
    "建設業許可証": "02_建設業許可証",
    "決算書": "03_決算書",
    "工事経歴書": "04_工事経歴書",
    "労働安全衛生誓約書": "05_労働安全衛生誓約書",
    "資格略字一覧": "06_資格略字一覧",
    "取引先一覧表": "07_取引先一覧表",
    "労働者名簿": "08_労働者名簿",
    "会社案内": "09_会社案内",
}
DEFAULT_CATEGORY = "99_受領バンドル"

# ファイル名 → doc_type 推定 (import_classified_inbox 参考の単純 keyword match)
KEYWORD_TO_DOCTYPE = [
    ("会社案内", "会社案内"),
    ("会社概要", "会社案内"),
    ("取引先一覧", "取引先一覧表"),
    ("仕入先", "取引先一覧表"),
    ("売上先", "取引先一覧表"),
    ("工事経歴", "工事経歴書"),
    ("施工実績", "工事経歴書"),
    ("建設業許可", "建設業許可証"),
    ("許可通知", "建設業許可証"),
    ("許可証", "建設業許可証"),
    ("決算", "決算書"),
    ("貸借対照表", "決算書"),
    ("損益計算書", "決算書"),
    ("労働者名簿", "労働者名簿"),
    ("作業員名簿", "労働者名簿"),
    ("資格略字", "資格略字一覧"),
    ("資格者名簿", "資格略字一覧"),
    ("労働安全", "労働安全衛生誓約書"),
    ("誓約書", "労働安全衛生誓約書"),
    ("御取引条件", "取引申請書"),
    ("取引条件", "取引申請書"),
    ("継続取引申請", "取引申請書"),
    ("新規取引", "取引申請書"),
    ("取引申請書", "取引申請書"),
]

# ネスト/サイズ制限
# ZIP パスワードはリポジトリへ保存しない。
# 必要時は --password-file または ZIP_PASSWORDS_JSON / ZIP_PASSWORDS_FILE で渡す。
ZIP_PASSWORDS: dict[str, str] = {}

MAX_DEPTH = 2
MAX_TOTAL_SIZE = 200 * 1024 * 1024  # 200MB
MAX_ENTRY_COUNT = 200


def load_zip_passwords(password_file: str | None = None) -> dict[str, str]:
    """ZIP basename -> password をローカルJSONまたは環境変数から読む。"""
    raw = os.environ.get("ZIP_PASSWORDS_JSON", "").strip()
    if raw:
        data = json.loads(raw)
        return {str(k): str(v) for k, v in data.items()}

    path_str = password_file or os.environ.get("ZIP_PASSWORDS_FILE", "").strip()
    if not path_str:
        return {}
    path = Path(path_str)
    data = json.loads(path.read_text(encoding="utf-8"))
    return {str(k): str(v) for k, v in data.items()}


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(65536), b""):
            h.update(c)
    return h.hexdigest()


def load_company_index(conn: sqlite3.Connection) -> dict:
    """companies + 営業所等 alias を _full → company_id, name に index 化"""
    rows = conn.execute(
        "SELECT company_id, official_name, name_aliases FROM companies WHERE status='ACTIVE'"
    ).fetchall()
    idx: dict[str, list[dict]] = {}
    raw: dict[str, list[dict]] = {}
    for r in rows:
        cid = r[0]
        names = [r[1]]
        if r[2]:
            names.extend([s.strip() for s in r[2].split(",") if s.strip()])
        for nm in names:
            full = fully_normalized(nm)
            entry = {"company_id": cid, "name": nm, "_full": full}
            idx.setdefault(full, []).append(entry)
            raw.setdefault(nm, []).append(entry)
    return {"by_full": idx, "by_raw": raw}


def match_company(query: str, idx: dict) -> tuple[str, str, str | None, str | None]:
    """
    return (confidence, method, company_id, matched_name)
    """
    if not query:
        return "UNMATCHED", "empty", None, None
    # 1. raw 完全一致
    if query in idx["by_raw"]:
        cands = idx["by_raw"][query]
        if len(cands) == 1:
            return "HIGH", "exact", cands[0]["company_id"], cands[0]["name"]
        return "MEDIUM", "exact_ambiguous", cands[0]["company_id"], cands[0]["name"]
    # 2. フル正規化一致
    full = fully_normalized(query)
    if full and full in idx["by_full"]:
        cands = idx["by_full"][full]
        if len(cands) == 1:
            return "HIGH", "normalized_full", cands[0]["company_id"], cands[0]["name"]
        return "MEDIUM", "normalized_ambiguous", cands[0]["company_id"], cands[0]["name"]
    # 3. 部分一致 (substring) — 短すぎる query は除外 (誤マッチ防止)
    if len(full) >= 3:
        cands = []
        for k, vs in idx["by_full"].items():
            if not k:
                continue
            if k in full or full in k:
                cands.extend(vs)
        if len(cands) == 1:
            return "LOW", "substring", cands[0]["company_id"], cands[0]["name"]
        if len(cands) > 1:
            # 最長一致を選ぶ
            cands.sort(key=lambda v: -len(v["_full"]))
            return "LOW", "substring_ambiguous", cands[0]["company_id"], cands[0]["name"]
    return "UNMATCHED", "none", None, None


def find_cid_by_email(conn: sqlite3.Connection, email: str) -> str | None:
    """送信者メールアドレスから company_id を取得"""
    if not email or "@" not in email:
        return None
    r = conn.execute(
        "SELECT company_id FROM company_emails WHERE email=? LIMIT 1", (email,)
    ).fetchone()
    if r:
        return r[0]
    # ドメイン部分一致 (mitsui@sora-support.jp → sora-support.jp で会社特定難しいので skip)
    return None


COMMON_PREFIX_RE = re.compile(r"^(御取引条件等説明書|御取引条件等説明書|継続取引申請書|新規・継続取引|新規取引|提出書類|取引申請書|お取引内容確認書|仕入先および売上先)[_＿\s]?")
COMMON_SUFFIX_RE = re.compile(r"\.pdf$|\.xlsx$|\(.*\)$|（.*）$", re.IGNORECASE)


def strip_zip_filename(stem: str) -> str:
    """ZIP ファイル名から共通プレフィックス・サフィックス除去"""
    s = re.sub(r"^\d{8}_\d{6}_?", "", stem)
    s = COMMON_PREFIX_RE.sub("", s)
    s = COMMON_SUFFIX_RE.sub("", s)
    s = s.replace("㈱", "株式会社").replace("㈲", "有限会社")
    return s.strip("_＿ 　")


def estimate_company(
    inner_name: str,
    zip_basename: str,
    sender_email: str,
    conn: sqlite3.Connection,
    idx: dict,
) -> tuple[str, str, str | None, str | None]:
    """3 段階で company_id を推定し最良結果を返す"""
    # 1. 送信者メールから cid 直引き → HIGH
    by_mail = find_cid_by_email(conn, sender_email)
    if by_mail:
        name = conn.execute(
            "SELECT official_name FROM companies WHERE company_id=?", (by_mail,)
        ).fetchone()
        return "HIGH", "sender_email", by_mail, name[0] if name else None
    # 2. ZIP 親ファイル名で会社マッチ (共通プレフィックス除去後)
    zip_stem = Path(zip_basename).stem
    zip_stem_clean = strip_zip_filename(zip_stem)
    conf1, m1, cid1, nm1 = match_company(zip_stem_clean, idx)
    # 3. 内包ファイル名でマッチ
    inner_stem = Path(inner_name).stem
    inner_stem_clean = strip_zip_filename(inner_stem)
    conf2, m2, cid2, nm2 = match_company(inner_stem_clean, idx)
    # 同 cid なら信頼度を昇格 (ZIP 名と内包どちらか一致のみでも採用)
    if cid1 and cid2 and cid1 == cid2:
        # 両方一致 → 信頼度 max
        rank = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "UNMATCHED": 0}
        best_conf = max(conf1, conf2, key=lambda c: rank.get(c, 0))
        # LOW でも両方一致なら MEDIUM に昇格
        if best_conf == "LOW":
            best_conf = "MEDIUM"
        return best_conf, f"both_{m1}_and_{m2}", cid1, nm1
    # ZIP 名から拾えれば優先 (LOW でも採用、後段で警告)
    if cid1:
        return conf1, f"zipname_{m1}", cid1, nm1
    if cid2:
        return conf2, f"inner_{m2}", cid2, nm2
    return "UNMATCHED", "none", None, None


def estimate_doctype(file_name: str) -> str:
    """ファイル名 keyword から doc_type 推定"""
    for kw, dt in KEYWORD_TO_DOCTYPE:
        if kw in file_name:
            return dt
    return ""


def update_inbound_log(zip_log_id: str, new_status: str, error_msg: str = ""):
    """inbound_log.csv の指定 log_id 行の process_status を更新"""
    rows = []
    with INBOUND_LOG.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for r in reader:
            if r.get("log_id") == zip_log_id:
                r["process_status"] = new_status
                r["process_finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if error_msg:
                    r["error_message"] = error_msg[:500]
            rows.append(r)
    with INBOUND_LOG.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def safe_path_check(member: zipfile.ZipInfo) -> bool:
    """path traversal 防止: '..' や絶対パスを拒否"""
    name = member.filename
    if name.startswith("/") or name.startswith("\\"):
        return False
    if ".." in Path(name).parts:
        return False
    if ":" in name:  # Windows 絶対パス
        return False
    return True


def process_zip(
    zip_log_row: dict,
    conn: sqlite3.Connection,
    idx: dict,
    run_id: str,
    dry_run: bool,
    depth: int = 0,
) -> dict:
    """1 ZIP の解凍・配置を実行し、サマリを返す"""
    log_id = zip_log_row["log_id"]
    zip_path = Path(zip_log_row["saved_path"])
    sender_email = zip_log_row.get("original_sender_email", "") or ""
    zip_basename = zip_path.name
    msg_id = zip_log_row.get("message_id", "")

    summary = {
        "zip_log_id": log_id,
        "zip_path": str(zip_path),
        "sender": sender_email,
        "depth": depth,
        "entries": [],
        "status": "EXTRACTING",
    }

    if not zip_path.exists():
        summary["status"] = "FAILED_EXTRACT"
        summary["error"] = "zip not found"
        return summary

    # パスワード検索 (file_name 一致)
    pwd = ZIP_PASSWORDS.get(zip_basename) or ZIP_PASSWORDS.get(zip_basename.lower())
    pwd_bytes = pwd.encode() if pwd else None

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            if pwd_bytes:
                zf.setpassword(pwd_bytes)
            members = zf.namelist()
            # 検証
            if len(members) > MAX_ENTRY_COUNT:
                summary["status"] = "FAILED_EXTRACT"
                summary["error"] = f"too many entries: {len(members)}>{MAX_ENTRY_COUNT}"
                return summary
            total_size = sum(zi.file_size for zi in zf.infolist())
            if total_size > MAX_TOTAL_SIZE:
                summary["status"] = "FAILED_EXTRACT"
                summary["error"] = f"too large: {total_size}>{MAX_TOTAL_SIZE}"
                return summary

            # path traversal チェック
            unsafe = [m for m in zf.infolist() if not safe_path_check(m)]
            if unsafe:
                summary["status"] = "FAILED_EXTRACT"
                summary["error"] = f"unsafe path: {[m.filename for m in unsafe[:3]]}"
                return summary

            # tmp 展開
            tmpdir = Path(tempfile.mkdtemp(prefix="zipext_"))
            try:
                zf.extractall(tmpdir)

                ok_count = 0
                fail_count = 0
                # 内包ファイルごとに処理
                for member in zf.infolist():
                    if member.is_dir():
                        continue
                    inner = tmpdir / member.filename
                    if not inner.is_file():
                        continue
                    inner_size = inner.stat().st_size
                    if inner_size == 0:
                        continue
                    inner_name = Path(member.filename).name
                    # ネスト ZIP
                    if inner_name.lower().endswith(".zip"):
                        if depth + 1 >= MAX_DEPTH:
                            entry = {
                                "inner_name": inner_name,
                                "size": inner_size,
                                "status": "FAILED_EXTRACT_NESTED_LIMIT",
                                "doc_type": "",
                                "company_id": None,
                            }
                            summary["entries"].append(entry)
                            fail_count += 1
                            continue
                        # 簡略化: ネスト ZIP は 99_受領バンドル に置くだけ (後で手動展開)
                        entry = {
                            "inner_name": inner_name,
                            "size": inner_size,
                            "status": "NESTED_ZIP_BUNDLE",
                            "doc_type": "",
                            "company_id": None,
                        }
                        summary["entries"].append(entry)
                        fail_count += 1
                        continue

                    # 会社推定
                    conf, method, cid, cname = estimate_company(
                        inner_name, zip_basename, sender_email, conn, idx
                    )
                    if conf in ("UNMATCHED",):
                        # LOW は不確定なのでバンドルに残置
                        entry = {
                            "inner_name": inner_name,
                            "size": inner_size,
                            "status": "FAILED_MATCH" if conf == "UNMATCHED" else "LOW_CONFIDENCE",
                            "match_confidence": conf,
                            "match_method": method,
                            "doc_type": "",
                            "company_id": cid,
                        }
                        summary["entries"].append(entry)
                        fail_count += 1
                        continue

                    # ハッシュ計算
                    fhash = sha256_file(inner)

                    # 既存 file_hash 重複チェック (active のみ対象)
                    existing = conn.execute(
                        "SELECT f.file_id, f.company_id, f.new_path FROM files f "
                        "WHERE f.file_hash=? LIMIT 1",
                        (fhash,),
                    ).fetchone()
                    if existing:
                        entry = {
                            "inner_name": inner_name,
                            "size": inner_size,
                            "file_hash": fhash[:12],
                            "status": "DUPLICATE_SKIPPED",
                            "match_confidence": conf,
                            "company_id": cid,
                            "existing_file_id": existing[0],
                            "existing_path": existing[2],
                        }
                        summary["entries"].append(entry)
                        # 重複は ok 扱い (不正状態でない)
                        continue

                    # doc_type 推定
                    doc_type = estimate_doctype(inner_name)
                    folder = CATEGORY_FOLDER.get(doc_type, DEFAULT_CATEGORY)

                    # 保存先
                    cdir = ORIGINALS / f"{cid}_{cname}"
                    sub = cdir / folder / "2026"
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    zip_stem = Path(zip_basename).stem
                    inner_stem = Path(inner_name).stem
                    ext = Path(inner_name).suffix
                    safe_zip_stem = re.sub(r'[\\/:*?"<>|]', '_', zip_stem)[:60]
                    safe_inner_stem = re.sub(r'[\\/:*?"<>|]', '_', inner_stem)[:60]
                    new_name = f"{ts}_{safe_zip_stem}__{safe_inner_stem}_{fhash[:8]}{ext}"
                    dst = sub / new_name

                    entry = {
                        "inner_name": inner_name,
                        "size": inner_size,
                        "file_hash": fhash[:12],
                        "match_confidence": conf,
                        "match_method": method,
                        "company_id": cid,
                        "company_name": cname,
                        "doc_type": doc_type,
                        "folder": folder,
                        "dst_path": str(dst),
                        "status": "EXTRACTED",
                    }

                    if not dry_run:
                        sub.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(inner), str(dst))
                        # files INSERT
                        cur = conn.execute(
                            "INSERT INTO files (message_id, company_id, file_name, file_hash, "
                            "file_size_bytes, saved_path, new_filename, new_path, created_at) "
                            "VALUES (?,?,?,?,?,?,?,?,?)",
                            (
                                msg_id, cid, inner_name, fhash, inner_size,
                                str(zip_path), new_name, str(dst),
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            ),
                        )
                        entry["file_id"] = cur.lastrowid

                    summary["entries"].append(entry)
                    ok_count += 1

                # ZIP 単位サマリ
                if ok_count > 0 and fail_count == 0:
                    summary["status"] = "EXTRACTED"
                elif ok_count > 0 and fail_count > 0:
                    summary["status"] = "PARTIAL_EXTRACTED"
                elif ok_count == 0 and any(e["status"] == "DUPLICATE_SKIPPED" for e in summary["entries"]):
                    summary["status"] = "DUPLICATE_SKIPPED"
                else:
                    summary["status"] = "FAILED_MATCH"
                summary["ok_count"] = ok_count
                summary["fail_count"] = fail_count
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)

    except zipfile.BadZipFile as e:
        summary["status"] = "FAILED_EXTRACT"
        summary["error"] = f"BadZipFile: {e}"
    except Exception as e:
        summary["status"] = "FAILED_EXTRACT"
        summary["error"] = f"{type(e).__name__}: {e}"

    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--zip-name", help="特定 ZIP のみ処理 (basename)")
    ap.add_argument("--password-file", help="ZIP パスワードJSON (basename -> password)")
    args = ap.parse_args()
    if not args.dry_run and not args.execute:
        print("ERROR: --dry-run か --execute", file=sys.stderr)
        sys.exit(1)

    ZIP_PASSWORDS.update(load_zip_passwords(args.password_file))

    run_id = f"extract_zips_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    print(f"run_id: {run_id}")
    print(f"mode: {'DRY-RUN' if args.dry_run else 'EXECUTE'}")
    print(f"zip_passwords: {len(ZIP_PASSWORDS)} 件")

    # PENDING ZIP 一覧
    with INBOUND_LOG.open(encoding="utf-8-sig", newline="") as f:
        all_rows = list(csv.DictReader(f))
    zip_rows = [
        r for r in all_rows
        if r["file_name"].lower().endswith(".zip")
        and r["process_status"] == "PENDING"
    ]
    if args.zip_name:
        zip_rows = [r for r in zip_rows if Path(r["saved_path"]).name == args.zip_name]
    print(f"PENDING ZIP: {len(zip_rows)} 件")

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    idx = load_company_index(conn)

    manifest_path = LOGS / f"extracted_manifest_{run_id}.jsonl"
    summaries = []

    if args.execute:
        conn.execute("BEGIN IMMEDIATE")
    try:
        for i, zr in enumerate(zip_rows, 1):
            print(f"\n[{i}/{len(zip_rows)}] {Path(zr['saved_path']).name[:60]}", file=sys.stderr)
            print(f"  sender: {zr.get('original_sender_email','')[:40]}", file=sys.stderr)
            s = process_zip(zr, conn, idx, run_id, args.dry_run)
            print(f"  -> {s['status']} ok={s.get('ok_count',0)} fail={s.get('fail_count',0)}", file=sys.stderr)
            for e in s.get("entries", [])[:10]:
                cid = e.get("company_id") or "-"
                doc = e.get("doc_type") or ""
                conf = e.get("match_confidence", "")
                st = e["status"]
                print(f"     [{st:<22}] {cid:<6} {conf:<6} {doc[:10]:<10} {e['inner_name'][:50]}", file=sys.stderr)
            if len(s.get("entries", [])) > 10:
                print(f"     ... +{len(s['entries'])-10} more", file=sys.stderr)
            summaries.append(s)
            if args.execute:
                update_inbound_log(zr["log_id"], s["status"], s.get("error", ""))
        if args.execute:
            conn.commit()
    except Exception as e:
        if args.execute:
            conn.rollback()
        print(f"ERROR: {e}", file=sys.stderr)
        raise

    # manifest 出力
    if args.execute:
        with manifest_path.open("w", encoding="utf-8") as f:
            for s in summaries:
                f.write(json.dumps(s, ensure_ascii=False) + "\n")
        print(f"\nmanifest: {manifest_path}")

    # サマリ
    print(f"\n=== {'DRY-RUN' if args.dry_run else 'EXECUTE'} 結果 ===")
    from collections import Counter
    zip_cnt = Counter(s["status"] for s in summaries)
    for st, n in zip_cnt.most_common():
        print(f"  ZIP {st}: {n}")
    entry_cnt = Counter(e["status"] for s in summaries for e in s.get("entries", []))
    print()
    for st, n in entry_cnt.most_common():
        print(f"  entry {st}: {n}")

    if args.dry_run:
        print(f"\n[dry-run] --execute で実行")
    else:
        print(f"\n次: Phase 2 (xlsx→PDF + register_pages_from_files)")


if __name__ == "__main__":
    main()
