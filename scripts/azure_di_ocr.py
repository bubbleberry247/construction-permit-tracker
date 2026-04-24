"""
Azure Document Intelligence (prebuilt-layout) 単体OCR スクリプト。
GPT-4o/5.4 Vision を使わず、Azure DI のみでテキスト抽出。

Usage:
    # credentials は 1Password から op run で注入
    op run --env-file /dev/stdin -- python scripts/azure_di_ocr.py --pdf <path> <<< "AZURE_DI_ENDPOINT=op://Personal/Azure エンドポイント/URL
AZURE_DI_KEY=op://Personal/Azure キー1/password"

    # または環境変数を事前にセット
    export AZURE_DI_ENDPOINT=...
    export AZURE_DI_KEY=...
    python scripts/azure_di_ocr.py --pdf data/staging/C0149_有限会社西春金属/20260424_101824_取引先.pdf
"""
import argparse
import json
import os
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")


def run_layout_ocr(pdf_path: Path) -> dict:
    """Azure DI prebuilt-layout を呼び出し、生OCR結果を返す。"""
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
    from azure.core.credentials import AzureKeyCredential

    endpoint = os.environ.get("AZURE_DI_ENDPOINT")
    key = os.environ.get("AZURE_DI_KEY")
    if not endpoint or not key:
        raise SystemExit("AZURE_DI_ENDPOINT / AZURE_DI_KEY 未設定")

    client = DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))
    with pdf_path.open("rb") as f:
        poller = client.begin_analyze_document(
            "prebuilt-layout",
            AnalyzeDocumentRequest(bytes_source=f.read()),
        )
    result = poller.result()
    return result


def summarize(result, pdf_path: Path) -> dict:
    """結果をJSON整形。テキスト、ページ数、表の有無のみ抽出。"""
    pages = []
    for page in getattr(result, "pages", []) or []:
        lines = [line.content for line in (page.lines or [])]
        pages.append({
            "page_no": page.page_number,
            "width": page.width,
            "height": page.height,
            "angle": getattr(page, "angle", None),
            "line_count": len(lines),
            "lines": lines,
        })
    tables = []
    for tbl in getattr(result, "tables", []) or []:
        tables.append({
            "rows": tbl.row_count,
            "cols": tbl.column_count,
            "cells_sample": [c.content for c in (tbl.cells or [])[:10]],
        })
    return {
        "pdf": pdf_path.name,
        "page_count": len(pages),
        "pages": pages,
        "table_count": len(tables),
        "tables": tables,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True, help="対象PDFパス")
    ap.add_argument("--output", help="結果JSON出力先（省略時はstdout）")
    args = ap.parse_args()

    pdf_path = Path(args.pdf)
    if not pdf_path.exists():
        raise SystemExit(f"PDFが存在しません: {pdf_path}")

    print(f"Azure DI OCR実行中: {pdf_path.name}", file=sys.stderr)
    result = run_layout_ocr(pdf_path)
    summary = summarize(result, pdf_path)

    out_json = json.dumps(summary, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(out_json, encoding="utf-8")
        print(f"結果: {args.output}", file=sys.stderr)
    else:
        print(out_json)


if __name__ == "__main__":
    main()
