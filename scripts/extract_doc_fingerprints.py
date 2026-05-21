"""
Phase 1: 書類カテゴリーを横断的に観察し、共通キー (識別力ある特徴) を統計的に抽出する。

出力:
  - data/fingerprint_analysis_<TS>.json  : 全体統計（クラス内 TF + log-odds スコア）
  - data/fingerprint_analysis_<TS>.md    : 人間レビュー用レポート
  - config/doc_fingerprints_v2_auto.yaml : 自動生成 YAML 候補（v1 と diff 取って手動マージ）

ロジック:
  1. seed = 高信頼度ページ (confidence >= 0.85) + 手動ラベル
  2. 各カテゴリで OCR テキスト取得 + 字間スペース正規化
  3. 各カテゴリで:
     - 含有率 (含むページ/総ページ): クラス特徴語抽出
     - log-odds (このクラス vs 他クラス): 識別力スコア
  4. 抽出対象:
     - headings: 上位 200 字内に出現する短いフレーズ (4-15 文字)
     - required_keywords: クラス内含有率 ≥ 70%
     - support_keywords: 含有率 30-70%
     - distinctive_keywords (log-odds 上位 30): 他クラスにほぼ出現しない語
     - regex pattern: 数字・年号・許可番号

Usage:
  python scripts/extract_doc_fingerprints.py
  python scripts/extract_doc_fingerprints.py --min-samples 10
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import pymupdf as fitz

PROJECT = Path(__file__).resolve().parent.parent
DB = PROJECT / "data" / "permit_tracker.db"
OCR_CACHE = PROJECT / "data" / "ocr_cache"
ORIGINALS = PROJECT / "data" / "originals"
sys.stdout.reconfigure(encoding="utf-8")

VALID_LABELS = [
    "建設業許可証", "決算書", "工事経歴書", "労働者名簿",
    "労働安全衛生誓約書", "資格略字一覧", "取引先一覧表",
    "会社案内", "取引申請書"
]

_JP_CHAR = r'[ぁ-んァ-ヴー一-龯々]'
_pdf_hash_cache: dict[str, str] = {}


def normalize(t: str) -> str:
    if not t:
        return ""
    t = re.sub(rf'({_JP_CHAR})\s+(?={_JP_CHAR})', r'\1', t)
    t = re.sub(r'(\d)\s*,\s*(\d)', r'\1,\2', t)
    t = re.sub(r'[ \t]+', ' ', t)
    return t


def find_pdf(file_name: str, cid: str) -> Path | None:
    if file_name.lower().endswith(".xlsx"):
        alt = file_name[:-5] + ".pdf"
        for c in ORIGINALS.glob(f"{cid}_*"):
            for p in c.rglob(alt):
                if p.is_file():
                    return p
    for c in ORIGINALS.glob(f"{cid}_*"):
        for p in c.rglob(file_name):
            if p.is_file():
                return p
        for p in c.rglob(f"*{file_name}"):
            if p.is_file():
                return p
    return None


def pdf_sha256(pdf: Path) -> str:
    k = str(pdf.resolve())
    if k in _pdf_hash_cache:
        return _pdf_hash_cache[k]
    h = hashlib.sha256()
    with pdf.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    _pdf_hash_cache[k] = h.hexdigest()
    return _pdf_hash_cache[k]


def get_text(pdf: Path, page_no: int) -> str:
    text = ""
    try:
        doc = fitz.open(pdf)
        if page_no <= len(doc):
            text = (doc[page_no - 1].get_text() or "").strip()
        doc.close()
    except Exception:
        text = ""

    if len(text) >= 30:
        return text

    try:
        h = pdf_sha256(pdf)
        per_page = OCR_CACHE / f"page_{h}_p{page_no}.json"
        if per_page.exists():
            d = json.loads(per_page.read_text(encoding="utf-8"))
            cached = d.get("text", "") or ""
            if len(cached) > len(text):
                return cached
    except Exception:
        pass

    try:
        h = pdf_sha256(pdf)
        full = OCR_CACHE / f"{h}.json"
        if full.exists():
            arr = json.loads(full.read_text(encoding="utf-8"))
            if isinstance(arr, list) and 1 <= page_no <= len(arr):
                cached = arr[page_no - 1] or ""
                if len(cached) > len(text):
                    return cached
    except Exception:
        pass

    return text


# ---------------------------------------------------------------------------
# n-gram extraction (2-8 chars, 日本語のみ)
# ---------------------------------------------------------------------------
def extract_ngrams(t: str, n_min=2, n_max=8) -> set[str]:
    """日本語連続文字列から n-gram を抽出。"""
    grams = set()
    # 日本語連続部分を切り出す
    runs = re.findall(rf'{_JP_CHAR}+', t)
    for run in runs:
        for n in range(n_min, n_max + 1):
            for i in range(len(run) - n + 1):
                gram = run[i:i+n]
                grams.add(gram)
    return grams


# 評価用 regex
REGEXES = {
    "permit_number": r'(?:般|特)[–\-―—ー\s]*\d+\)?\s*第\s*\d+\s*号',
    "amount_comma":  r'\d{1,3}(?:,\d{3}){2,}',
    "era_date":      r'(?:令和|平成|昭和)\s*\d+\s*年\s*\d+\s*月',
    "slash_date":    r'\d{4}/\d{1,2}/\d{1,2}',
    "postal_code":   r'\d{3}-\d{4}',
    "company_form":  r'(?:株式会社|有限会社|\(株\)|㈱|\(有\)|㈲)',
    "period_n":      r'第\s*\d+\s*期',
    "thousand_yen":  r'\d{1,5},\d{3}\s*千円',
}


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-samples", type=int, default=8)
    ap.add_argument("--min-confidence", type=float, default=0.80)
    ap.add_argument("--max-pages-per-class", type=int, default=200, help="クラスごと最大サンプル数（処理時間制限）")
    args = ap.parse_args()

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # seed: 手動ラベル + 高信頼度
    samples_by_class: dict[str, list] = defaultdict(list)
    for label in VALID_LABELS:
        # 手動ラベル優先
        manual = conn.execute(
            """SELECT DISTINCT p.page_id, p.company_id, p.file_name, p.page_no
               FROM pages p
               JOIN page_doc_type_history h USING(page_id)
               WHERE p.doc_type_name = ?
                 AND h.confirmed_by IN ('web_viewer','user_visual','masaru_manual')
               LIMIT ?""",
            (label, args.max_pages_per_class)
        ).fetchall()
        # 不足分は高信頼度
        if len(manual) < args.max_pages_per_class:
            need = args.max_pages_per_class - len(manual)
            existing_ids = {r["page_id"] for r in manual}
            high = conn.execute(
                """SELECT page_id, company_id, file_name, page_no FROM pages
                   WHERE doc_type_name=? AND COALESCE(confidence, 0) >= ?
                   ORDER BY confidence DESC LIMIT ?""",
                (label, args.min_confidence, need * 2)
            ).fetchall()
            high = [r for r in high if r["page_id"] not in existing_ids][:need]
            samples_by_class[label] = list(manual) + list(high)
        else:
            samples_by_class[label] = list(manual)

        print(f"[seed] {label:25s} = {len(samples_by_class[label])} sample (manual={len(manual)})", file=sys.stderr)

    # OCR テキスト取得 + 正規化
    texts_by_class: dict[str, list[str]] = defaultdict(list)
    pdf_cache: dict[str, Path | None] = {}
    n_skip_no_pdf = 0
    n_skip_no_text = 0

    for label, rows in samples_by_class.items():
        for r in rows:
            ck = f'{r["company_id"]}/{r["file_name"]}'
            if ck in pdf_cache:
                pdf = pdf_cache[ck]
            else:
                pdf = find_pdf(r["file_name"], r["company_id"])
                pdf_cache[ck] = pdf
            if pdf is None:
                n_skip_no_pdf += 1
                continue
            text = get_text(pdf, r["page_no"])
            if not text or len(text) < 20:
                n_skip_no_text += 1
                continue
            texts_by_class[label].append(normalize(text))

    print(f"[ocr] no_pdf={n_skip_no_pdf}, no_text={n_skip_no_text}", file=sys.stderr)
    for l in VALID_LABELS:
        print(f"[ocr] {l:25s} = {len(texts_by_class.get(l, []))} pages", file=sys.stderr)

    # 各クラスで n-gram の document frequency を計算 (1 page = 1 doc)
    # df_by_class[class][gram] = page count containing the gram
    df_by_class: dict[str, Counter] = defaultdict(Counter)
    n_pages_by_class: dict[str, int] = {}

    for label, texts in texts_by_class.items():
        n_pages_by_class[label] = len(texts)
        if len(texts) < args.min_samples:
            print(f"[warn] {label}: サンプル {len(texts)} < min_samples {args.min_samples}", file=sys.stderr)
        for t in texts:
            grams = extract_ngrams(t, 2, 8)
            for g in grams:
                df_by_class[label][g] += 1

    # 全クラス合計の DF
    df_total: Counter = Counter()
    n_total = 0
    for l, df in df_by_class.items():
        n_total += n_pages_by_class.get(l, 0)
        for g, c in df.items():
            df_total[g] += c

    # 解析結果
    analysis: dict = {
        "version": "v2_auto",
        "generated_at": datetime.now().isoformat(),
        "n_total_pages": n_total,
        "documents": {}
    }

    for label in VALID_LABELS:
        nP = n_pages_by_class.get(label, 0)
        if nP < args.min_samples:
            analysis["documents"][label] = {
                "n_pages": nP,
                "skipped": "insufficient_samples"
            }
            continue
        df = df_by_class[label]

        # log-odds (Dirichlet smoothed) - 識別力ある語
        # logodds(g) = log(p_in / (1 - p_in)) - log(p_out / (1 - p_out))
        # ただし n_in / nP_class, n_out / nP_other で計算
        nO = n_total - nP  # 他クラスのページ数

        scored = []
        for g, c_in in df.items():
            if c_in < 3:  # 最低 3 ページに出現
                continue
            if len(g) < 2:
                continue
            c_out = df_total[g] - c_in
            # 含有率
            rate_in = c_in / nP
            rate_out = c_out / max(nO, 1)
            # log-odds (smoothing α=0.5)
            num = (c_in + 0.5) / (nP - c_in + 0.5)
            den = (c_out + 0.5) / (nO - c_out + 0.5)
            logodds = math.log(num) - math.log(den)
            # 標準誤差
            se = math.sqrt(1/(c_in+0.5) + 1/(c_out+0.5) + 1/(nP-c_in+0.5) + 1/(nO-c_out+0.5))
            z = logodds / se
            scored.append((g, c_in, rate_in, rate_out, logodds, z))

        # z-score で並べ替え (識別力高い + 統計的に有意)
        scored.sort(key=lambda x: x[5], reverse=True)

        # 高含有率（≥70%）の必須語候補
        required = [(g, c, ri) for g, c, ri, ro, lo, z in scored if ri >= 0.70 and ro < 0.30]
        required.sort(key=lambda x: x[2], reverse=True)
        # 含有率 30-70% の補助語
        support = [(g, c, ri, lo, z) for g, c, ri, ro, lo, z in scored if 0.30 <= ri < 0.70 and z > 2.0]
        support.sort(key=lambda x: x[4], reverse=True)
        # 識別力上位 (log-odds 高、含有率 ≥ 20%)
        distinctive = [(g, c, ri, lo, z) for g, c, ri, ro, lo, z in scored if ri >= 0.20 and z > 3.0]
        distinctive.sort(key=lambda x: x[4], reverse=True)

        # サブシーケンス重複除去 (longest-first prefer)
        def dedup_substrings(words: list[str]) -> list[str]:
            sw = sorted(words, key=lambda x: -len(x))
            kept = []
            for w in sw:
                if not any(w in k or k in w for k in kept if w != k):
                    kept.append(w)
                elif len(w) > 2 and not any(w in k for k in kept):
                    kept.append(w)
            # remove prefix-of-existing
            out = []
            for w in kept:
                # if w is substring of a longer kept, drop it
                if any(w in k and w != k for k in kept):
                    continue
                out.append(w)
            return out

        required_words = dedup_substrings([r[0] for r in required[:50]])[:20]
        support_words = dedup_substrings([r[0] for r in support[:80]])[:30]
        distinctive_words = dedup_substrings([r[0] for r in distinctive[:60]])[:20]

        # regex hit rate
        regex_rates = {}
        for name, pat in REGEXES.items():
            cre = re.compile(pat)
            hits = sum(1 for t in texts_by_class[label] if cre.search(t))
            rate_in = hits / nP
            # 他クラス
            other_hits = 0
            other_n = 0
            for ol, otexts in texts_by_class.items():
                if ol == label:
                    continue
                other_hits += sum(1 for t in otexts if cre.search(t))
                other_n += len(otexts)
            rate_out = other_hits / max(other_n, 1)
            regex_rates[name] = {
                "rate_in": round(rate_in, 3),
                "rate_out": round(rate_out, 3),
                "discriminative": rate_in - rate_out
            }

        # heading candidates: 上位 200 字に頻出する 4-12 字の連続日本語
        heading_counts: Counter = Counter()
        for t in texts_by_class[label]:
            top = t[:200]
            for run in re.findall(rf'{_JP_CHAR}+', top):
                for n in range(4, 13):
                    for i in range(len(run) - n + 1):
                        gram = run[i:i+n]
                        heading_counts[gram] += 1
        # 上位 200 字に 50%+ で出る語
        heading_thr = max(3, int(nP * 0.4))
        headings = [(g, c) for g, c in heading_counts.most_common(40) if c >= heading_thr]
        headings_dedup = dedup_substrings([h[0] for h in headings[:30]])[:10]

        analysis["documents"][label] = {
            "n_pages": nP,
            "headings_top200": [{"gram": g, "count": c, "rate": round(c/nP, 3)} for g, c in headings[:20]],
            "headings_dedup": headings_dedup,
            "required_candidates": [{"gram": g, "count": c, "rate": round(ri, 3)} for g, c, ri in required[:30]],
            "support_candidates": [{"gram": g, "count": c, "rate": round(ri, 3), "z": round(z, 2)} for g, c, ri, lo, z in support[:30]],
            "distinctive_top": [{"gram": g, "count": c, "rate": round(ri, 3), "logodds": round(lo, 2), "z": round(z, 2)} for g, c, ri, lo, z in distinctive[:30]],
            "required_words": required_words,
            "support_words": support_words,
            "distinctive_words": distinctive_words,
            "regex_hit_rates": regex_rates,
        }

    # 出力
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = PROJECT / "data" / f"fingerprint_analysis_{ts}.json"
    md_path = PROJECT / "data" / f"fingerprint_analysis_{ts}.md"

    json_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")

    # MD レポート
    lines = [
        f"# 書類フィンガープリント横断分析 v2 ({ts})",
        f"",
        f"- サンプル: 高信頼度 (conf≥{args.min_confidence}) + 手動ラベル",
        f"- 全 {n_total} ページ",
        f"- min_samples: {args.min_samples}",
        f"",
    ]
    for label in VALID_LABELS:
        d = analysis["documents"].get(label, {})
        if d.get("skipped"):
            lines.append(f"## {label} - **スキップ** (samples={d['n_pages']})\n")
            continue
        lines.append(f"## {label} (n={d['n_pages']})\n")
        lines.append(f"### 見出し候補 (上位 200 字内 ≥40%)")
        for h in d["headings_top200"][:10]:
            lines.append(f"- `{h['gram']}` rate={h['rate']:.2f} ({h['count']})")
        lines.append("")
        lines.append(f"### 必須キーワード候補 (含有率 ≥70%、他クラス <30%)")
        for r in d["required_candidates"][:15]:
            lines.append(f"- `{r['gram']}` rate={r['rate']:.2f} ({r['count']} pages)")
        lines.append("")
        lines.append(f"### 識別力上位 (log-odds, z>3, rate≥20%)")
        for r in d["distinctive_top"][:15]:
            lines.append(f"- `{r['gram']}` rate={r['rate']:.2f} z={r['z']} logodds={r['logodds']}")
        lines.append("")
        lines.append(f"### Regex ヒット率")
        for name, rr in d["regex_hit_rates"].items():
            lines.append(f"- `{name}`: in={rr['rate_in']:.2f}, out={rr['rate_out']:.2f}, disc={rr['discriminative']:.2f}")
        lines.append("")

    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"\n出力:")
    print(f"  JSON: {json_path}")
    print(f"  MD:   {md_path}")
    print(f"\n次のステップ: MD レポートを確認 → config/doc_fingerprints_v1.yaml を更新")


if __name__ == "__main__":
    main()
