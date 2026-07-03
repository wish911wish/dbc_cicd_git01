# common/type_cast.py
CAST_PREFIX = "_cast_"                        # 型変換結果カラムのプレフィックス
CAST_FAILED_COLUMNS = "_cast_failed_columns"  # 変換失敗カラム名の連結
CAST_PASSED = "_cast_passed"                  # 全カラム変換成功なら True

# common/expectations.py
EXPECT_FAILED_RULES = "_expect_failed_rules"  # 違反ルール名の連結
EXPECT_PASSED = "_expect_passed"              # 違反ルールなしなら True


# common/expectations.py
"""
名前付きの Expectations 条件を行ごとに評価する汎用ユーティリティ。
NULL の扱いは関数では判断せず、rules の条件式側で担保する。
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, concat_ws, when, lit

FAILED_COL = "failed_expectations"   # 違反ルール名の連結列
PASSED_FLAG = "_passed"              # 違反なし = True


def apply_expectations(df: DataFrame, rules: dict) -> DataFrame:
    """
    rules: {ルール名: SQLブール条件}
        例 {"qty_positive": "qty > 0",
            "qty_present":  "qty IS NOT NULL AND qty > 0"}  # NULLも弾きたい場合は条件側で表現

    各ルールについて条件が真でない行に ルール名 を FAILED_COL へ記録する。
    条件が NULL の行は真ではないが、三値論理により NOT(条件) も NULL となり違反に含めない。
    NULL を違反として扱いたい場合は、条件式側で明示的に判定すること。
    """
    if not rules:
        raise ValueError("rules が空です")

    violation_parts = []
    for rule_name, condition in rules.items():
        if not isinstance(condition, str) or not condition.strip():
            raise ValueError(f"ルール条件は非空の文字列で指定してください: {rule_name}")
        violation_parts.append(
            when(expr(f"NOT ({condition})"), lit(rule_name))
        )

    result = df.withColumn(FAILED_COL, concat_ws(", ", *violation_parts))
    result = result.withColumn(PASSED_FLAG, expr(f"`{FAILED_COL}` = ''"))
    return result


def split_by_expectations(df: DataFrame):
    """
    apply_expectations 適用後の DataFrame を 合格/不合格 に分割して返す。
    返り値: (passed_df, failed_df)
    """
    if PASSED_FLAG not in df.columns:
        raise ValueError("apply_expectations を先に適用してください")
    return df.filter(f"`{PASSED_FLAG}`"), df.filter(f"NOT `{PASSED_FLAG}`")
