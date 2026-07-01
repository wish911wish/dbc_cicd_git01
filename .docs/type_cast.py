"""
業務要件を含まない、汎用の型変換・妥当性判定ユーティリティ。
 
STRING カラムを目標型へ変換し、変換失敗を quarantine 判定する。
- 型名指定           : try_cast(col AS <型>)         例 "INT", "DECIMAL(12,2)"
- フォーマット指定   : try_to_date / try_to_timestamp 例 {"type": "DATE", "format": "yyyyMMdd"}
 
判定ルール（業務非依存）:
  無効 = ソースが非NULL かつ 変換結果が NULL（= その型/形式として解釈不能）
  ソースが NULL の場合は正規の欠損として有効（NULL→NULL）
 
前提:
  ANSIモード有効を想定（try_cast/try_to_* は失敗時に例外ではなく NULL を返す）。
  日付/時刻パーサは既定（spark.sql.legacy.timeParserPolicy = CORRECTED 相当）を想定し、
  存在しない日付・不正月日は NULL となることに依存する。
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, concat_ws, when, lit
 
 
# 型名指定で受け付ける目標型（業務に依存しない定型キャストのみ）
ALLOWED_TARGET_TYPES = {
    "INT", "BIGINT", "SMALLINT", "TINYINT",
    "DOUBLE", "FLOAT", "BOOLEAN",
    "DATE", "TIMESTAMP",
}
 
# フォーマット指定で許可するパターン（式へ埋め込むためホワイトリスト管理）
ALLOWED_FORMATS = {
    "yyyy-MM-dd", "yyyy/MM/dd", "yyyy/M/d", "yyyyMMdd",
    "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "yyyyMMddHHmmss",
}
 
 
def _is_decimal(t: str) -> bool:
    return t.upper().startswith("DECIMAL(")
 
 
def _validate_target_type(t: str) -> None:
    if t.upper() not in ALLOWED_TARGET_TYPES and not _is_decimal(t):
        raise ValueError(f"未対応の目標型です: {t}")
 
 
def _validate_format(fmt: str) -> None:
    if fmt not in ALLOWED_FORMATS:
        raise ValueError(f"未許可のフォーマットです: {fmt}")
 
 
def _build_cast_expr(src_col: str, spec) -> str:
    """
    1カラム分のキャスト SQL 式を組み立てて返す。
 
    spec が str  -> 型名として try_cast
    spec が dict -> {"type": "DATE"|"TIMESTAMP", "format": "<pattern>"} として
                    try_to_date / try_to_timestamp をフォーマット指定で使用
    """
    col = f"`{src_col}`"
 
    if isinstance(spec, str):
        _validate_target_type(spec)
        return f"try_cast({col} AS {spec})"
 
    if isinstance(spec, dict):
        t = spec.get("type", "").upper()
        fmt = spec.get("format")
        if not t or not fmt:
            raise ValueError(f"format指定には type と format が必要です: {spec}")
        _validate_format(fmt)
        if t == "DATE":
            return f"try_to_date({col}, '{fmt}')"
        if t == "TIMESTAMP":
            return f"try_to_timestamp({col}, '{fmt}')"
        raise ValueError(f"format指定は DATE / TIMESTAMP のみ対応です: {t}")
 
    raise ValueError(f"不正なマッピング指定です: {src_col} -> {spec}")
 
 
def cast_columns(df: DataFrame, mapping: dict) -> DataFrame:
    """
    mapping: {ソースカラム名(STRING): spec}
        spec = 型名文字列（例 "INT", "DECIMAL(12,2)"）
             | {"type": "DATE"|"TIMESTAMP", "format": "<pattern>"}
 
    返り値: 型付け列 `{col}_t` + quarantine_reason + _is_valid を付与した DataFrame
    """
    if not mapping:
        raise ValueError("mapping が空です")
 
    result = df
    reason_parts = []
 
    for src_col, spec in mapping.items():
        typed_col = f"{src_col}_t"
        cast_sql = _build_cast_expr(src_col, spec)
 
        # 変換（失敗は NULL）
        result = result.withColumn(typed_col, expr(cast_sql))
 
        # 「ソース非NULL かつ 変換結果NULL」を失敗として理由化
        reason_parts.append(
            when(
                expr(f"`{src_col}` IS NOT NULL AND `{typed_col}` IS NULL"),
                lit(f"{src_col}:cast_failed"),
            )
        )
 
    result = result.withColumn("quarantine_reason", concat_ws("; ", *reason_parts))
    result = result.withColumn("_is_valid", expr("quarantine_reason = ''"))
    return result
 
 
def split_valid_quarantine(df: DataFrame):
    """
    cast_columns 適用後の DataFrame を有効/無効に分割して返す。
    返り値: (valid_df, quarantine_df)
    """
    if "_is_valid" not in df.columns:
        raise ValueError("cast_columns を先に適用してください（_is_valid 列がありません）")
    return df.filter("_is_valid"), df.filter("NOT _is_valid")
