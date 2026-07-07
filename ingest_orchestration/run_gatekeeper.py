"""Bronze→Silver ゲートキーパー本体（spark_python_task）。

制御テーブル bronze_ingestion_control の更新（table_update, ANY_UPDATED）で起動され、
Silverジョブごとに「必要なBronzeが全て成功済みか」を業務日(batch_date)基準で判定し、
揃っていれば Silver を run_now で起動する。SLA超過は例外を投げてジョブ失敗通知に載せる。

job_id の解決（B案）:
  - マニフェストは job_id を持たない。
  - DABが ${resources.jobs.<key>.id} を埋めたJSONを --silver-job-ids で受け取り、
    論理名 -> job_id を解決する。dev/prod で自動的に正しいIDが入る。

冪等性・安全性:
  - 起動実績は silver_trigger_control に記録し、二重起動を防ぐ。
  - このテーブルは監視対象(bronze_ingestion_control)とは別なので、記録の書き込みで
    自分自身を再トリガーしない。
  - ジョブは max_concurrent_runs=1 前提（判定〜起動〜記録の競合を排除）。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys

# spark_python_task では実行ファイルのディレクトリを import パスに追加しておく
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from databricks.sdk import WorkspaceClient  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from dependencies import SILVER_DEPENDENCIES, SilverDependency  # noqa: E402

JST = dt.timezone(dt.timedelta(hours=9))


def _quote_list(names) -> str:
    return ", ".join(f"'{n}'" for n in names)


def successful_bronze(spark, control_table, batch_date, required) -> set[str]:
    """指定業務日に status='success' で完了している required 内のBronzeジョブ名集合。"""
    rows = spark.sql(
        f"""
        SELECT job_name
        FROM {control_table}
        WHERE batch_date = DATE'{batch_date}'
          AND status = 'success'
          AND job_name IN ({_quote_list(required)})
        """
    ).collect()
    return {r["job_name"] for r in rows}


def already_triggered(spark, trigger_ctl, silver_job, batch_date) -> bool:
    return (
        spark.sql(
            f"""
            SELECT 1 FROM {trigger_ctl}
            WHERE silver_job = '{silver_job}' AND batch_date = DATE'{batch_date}'
            LIMIT 1
            """
        ).count()
        > 0
    )


def mark_triggered(spark, trigger_ctl, silver_job, batch_date, run_id) -> None:
    spark.sql(
        f"""
        INSERT INTO {trigger_ctl} (silver_job, batch_date, triggered_at, trigger_run_id)
        VALUES ('{silver_job}', DATE'{batch_date}', current_timestamp(), '{run_id}')
        """
    )


def candidate_batches(spark, control_table, lookback_days) -> list[dt.date]:
    """直近 lookback_days に完了実績がある業務日（=処理対象になり得るバッチ）。"""
    rows = spark.sql(
        f"""
        SELECT DISTINCT batch_date
        FROM {control_table}
        WHERE batch_date >= current_date() - INTERVAL {lookback_days} DAYS
        ORDER BY batch_date
        """
    ).collect()
    return [r["batch_date"] for r in rows]


def sla_deadline(batch_date: dt.date, dep: SilverDependency) -> dt.datetime:
    """業務日の「翌日」朝 sla_hour_jst:sla_minute_jst(JST) を締切とする。"""
    base = dt.datetime(
        batch_date.year, batch_date.month, batch_date.day,
        dep.sla_hour_jst, dep.sla_minute_jst, tzinfo=JST,
    )
    return base + dt.timedelta(days=1)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--ops-schema", required=True)
    p.add_argument("--lookback-days", type=int, default=3)
    # DABが ${resources.jobs.<key>.id} を埋めたJSON文字列（論理名 -> job_id）
    p.add_argument("--silver-job-ids", required=True)
    args = p.parse_args()

    silver_job_ids: dict[str, int] = {
        k: int(v) for k, v in json.loads(args.silver_job_ids).items()
    }

    # マニフェストとIDマップの整合性チェック（設定漏れを早期に検出）
    missing_ids = sorted(set(SILVER_DEPENDENCIES) - set(silver_job_ids))
    if missing_ids:
        raise KeyError(f"--silver-job-ids に job_id が無い Silver: {missing_ids}")

    control_table = f"{args.catalog}.{args.ops_schema}.bronze_ingestion_control"
    trigger_ctl = f"{args.catalog}.{args.ops_schema}.silver_trigger_control"

    spark = SparkSession.builder.getOrCreate()
    w = WorkspaceClient()  # ジョブ実行コンテキストの既定認証を使用
    now = dt.datetime.now(JST)

    batches = candidate_batches(spark, control_table, args.lookback_days)
    breaches: list[str] = []

    for silver_job, dep in SILVER_DEPENDENCIES.items():
        for batch_date in batches:
            if already_triggered(spark, trigger_ctl, silver_job, batch_date):
                continue

            done = successful_bronze(spark, control_table, batch_date, dep.requires)
            missing = sorted(set(dep.requires) - done)

            if not missing:
                # 全依存Bronzeが成功 → Silverを起動し、実績を記録（冪等ガード）
                run = w.jobs.run_now(
                    job_id=silver_job_ids[silver_job],
                    job_parameters={"batch_date": batch_date.isoformat()},
                )
                # run.run_id はSDKバージョンで参照経路が異なる場合があるので適宜調整
                mark_triggered(spark, trigger_ctl, silver_job, batch_date, str(run.run_id))
                print(f"[triggered] {silver_job} batch={batch_date} run_id={run.run_id}")
            elif now > sla_deadline(batch_date, dep):
                # 締切超過でも未達 → 不完全データでSilverは起動せず、SLA違反として記録
                breaches.append(f"{silver_job} batch={batch_date} missing={missing}")

    if breaches:
        # ジョブを失敗させ、email_notifications.on_failure で通知させる
        raise RuntimeError("SLA breach (Bronze未達): " + "; ".join(breaches))

    print("gatekeeper: 判定完了（起動対象なし、または起動済み）")


if __name__ == "__main__":
    main()
