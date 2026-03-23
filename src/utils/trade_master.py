"""
建設業許可 29業種マスタ + OCR表記ゆれ正規化
建設業法 別表第一（2016年解体工事業追加後）
"""
from __future__ import annotations

# 29業種マスタ（正規名称）
TRADE_CATEGORIES: list[str] = [
    # 一式工事（2業種）
    "土木工事業",
    "建築工事業",
    # 専門工事（27業種）
    "大工工事業",
    "左官工事業",
    "とび・土工工事業",
    "石工事業",
    "屋根工事業",
    "電気工事業",
    "管工事業",
    "タイル・れんが・ブロック工事業",
    "鋼構造物工事業",
    "鉄筋工事業",
    "舗装工事業",
    "しゅんせつ工事業",
    "板金工事業",
    "ガラス工事業",
    "塗装工事業",
    "防水工事業",
    "内装仕上工事業",
    "機械器具設置工事業",
    "熱絶縁工事業",
    "電気通信工事業",
    "造園工事業",
    "さく井工事業",
    "建具工事業",
    "水道施設工事業",
    "消防施設工事業",
    "清掃施設工事業",
    "解体工事業",
]

# 指定建設業7業種（特定建設業で1級資格必須）
DESIGNATED_TRADES: set[str] = {
    "土木工事業", "建築工事業", "電気工事業",
    "管工事業", "鋼構造物工事業", "舗装工事業", "造園工事業"
}

# OCR表記ゆれ → 正規名称マッピング
TRADE_ALIASES: dict[str, str] = {
    # とび・土工工事業
    "とび・土工・コンクリート工事業": "とび・土工工事業",
    "とび土工工事業": "とび・土工工事業",
    "とび・土工コンクリート工事業": "とび・土工工事業",
    "鳶・土工工事業": "とび・土工工事業",
    # タイル・れんが・ブロック工事業
    "タイル・煉瓦・ブロック工事業": "タイル・れんが・ブロック工事業",
    "タイル・レンガ・ブロック工事業": "タイル・れんが・ブロック工事業",
    "タイルれんがブロック工事業": "タイル・れんが・ブロック工事業",
    # 内装仕上工事業
    "内装仕上げ工事業": "内装仕上工事業",
    "内装工事業": "内装仕上工事業",
    # しゅんせつ工事業
    "浚渫工事業": "しゅんせつ工事業",
    "竣渫工事業": "しゅんせつ工事業",
    # 機械器具設置工事業
    "機械器具設置": "機械器具設置工事業",
    # 電気通信工事業
    "電気通信": "電気通信工事業",
}


def normalize_trade(raw: str) -> str | None:
    """
    OCRで取得した業種名を正規名称に変換する。
    正規名称にマッチしなければNoneを返す。

    Args:
        raw: OCR取得の業種名（部分一致・表記ゆれあり）

    Returns:
        正規化後の業種名。マッチしなければNone。
    """
    # Step 1: 正規名称に完全一致
    if raw in TRADE_CATEGORIES:
        return raw

    # Step 2: TRADE_ALIASES で変換
    if raw in TRADE_ALIASES:
        return TRADE_ALIASES[raw]

    # Step 3: 正規名称に raw が含まれるか部分一致（例: 「電気工事」→「電気工事業」）
    for canonical in TRADE_CATEGORIES:
        if raw in canonical:
            return canonical

    # Step 4: 全て失敗
    return None


def normalize_trade_list(raw_list: list[str]) -> tuple[list[str], list[str]]:
    """
    複数業種を一括正規化。

    Returns:
        (normalized: 正規化成功リスト, failed: 正規化失敗リスト)
    """
    normalized: list[str] = []
    failed: list[str] = []

    for raw in raw_list:
        result = normalize_trade(raw)
        if result is not None:
            normalized.append(result)
        else:
            failed.append(raw)

    return normalized, failed


def is_valid_trade(trade: str) -> bool:
    """正規名称かどうか確認"""
    return trade in TRADE_CATEGORIES
