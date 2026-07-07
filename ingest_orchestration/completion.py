"""Bronzeジョブの完了実績を制御テーブルに記録する共有ユーティリティ。

各Bronzeジョブの最終タスクから呼ぶ。共通wheel等に含め、Bronze側から import する想定。
"""
from __future__ import annotations

import datetime as dt

from pyspark.sql import SparkSession


def record_bronze_completion(
    spark: SparkSession,
    control_table: str,
    *,
    job_name: str,
    table_name: str,
    batch_date: str,            # 業務日 'YYYY-MM-DD'
    status: str,                # 'success' | 'failed'
    row_count: int | None = None,
    source_version: int | None = None,
    run_id: str | None = None,
) -> None:
    """(batch_date, job_name) をキーに MERGE(upsert) する。

    同一キーでの再実行は上書きになるため冪等。成功・失敗のいずれも記録することで、
    ゲートキーパーが status='success' で突合でき、失敗はSLA判定・観測に使える。
    """
    bd = dt.date.fromisoformat(batch_date)
    src = spark.createDataFrame(
        [
            (
                bd,
                job_name,
                table_name,
                status,
                row_count,
                source_version,
                run_id,
                dt.datetime.now(dt.timezone.utc),
            )
        ],
        schema=(
            "batch_date date, job_name string, table_name string, status string, "
            "row_count long, source_version long, run_id string, "
            "completed_at timestamp"
        ),
    )
    src.createOrReplaceTempView("_bronze_completion_src")
    spark.sql(
        f"""
        MERGE INTO {control_table} AS t
        USING _bronze_completion_src AS s
          ON t.batch_date = s.batch_date AND t.job_name = s.job_name AND t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


# 使用例（Bronzeジョブの最終タスク内）:
#
#   from common.completion import record_bronze_completion
#
#   CTL = f"{catalog}.{ops_schema}.bronze_ingestion_control"
#   try:
#       n = run_bronze_ingestion(...)              # 実際の取り込み処理
#       record_bronze_completion(
#           spark, CTL, job_name="bronze_orders", table_name="orders", batch_date=batch_date,
#           status="success", row_count=n, run_id=run_id,
#       )
#   except Exception:
#       record_bronze_completion(
#           spark, CTL, job_name="bronze_orders", table_name="orders", batch_date=batch_date,
#           status="failed", run_id=run_id,
#       )
#       raise
