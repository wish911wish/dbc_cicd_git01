-- 制御テーブル（ops スキーマ想定）。${catalog}.${ops_schema} は環境に合わせて置換する。
-- 実行方法の例: SQLタスクのパラメータ、USE CATALOG / USE SCHEMA、または初期化ノートブック。

-- 1) Bronze取り込み完了実績（table_update トリガーの監視対象テーブル）
CREATE TABLE IF NOT EXISTS ${catalog}.${ops_schema}.bronze_ingestion_control (
  batch_date      DATE      NOT NULL,   -- 業務日（壁時計でなく論理日で突合する）
  job_name        STRING    NOT NULL,   -- ジョブ名
  table_name      STRING    NOT NULL,   -- 対象テーブル名
  status          STRING    NOT NULL,   -- 'success' | 'failed'
  row_count       BIGINT,               -- 取り込み件数（任意・観測用）
  source_version  BIGINT,               -- 取り込んだソースのコミットバージョン（任意）
  run_id          STRING,               -- Bronze側 run_id（トレース用）
  completed_at    TIMESTAMP NOT NULL,
  CONSTRAINT pk_bronze_ingestion_control PRIMARY KEY (batch_date, job_name)
) USING DELTA;

-- 2) Silver起動実績（冪等性の要）。
--    監視対象(bronze_ingestion_control)とは「別テーブル」にすることが重要。
--    ゲートキーパーの記録書き込みで自分自身を再トリガーしないため。
CREATE TABLE IF NOT EXISTS ${catalog}.${ops_schema}.silver_trigger_control (
  silver_job     STRING    NOT NULL,    -- 起動したSilverジョブ（論理名）
  batch_date     DATE      NOT NULL,
  triggered_at   TIMESTAMP NOT NULL,
  trigger_run_id STRING,                -- 起動したSilver側 run_id
  CONSTRAINT pk_silver_trigger_control PRIMARY KEY (silver_job, batch_date)
) USING DELTA;

-- 注: Unity Catalog の PRIMARY KEY は情報制約（既定では非強制）。
--     意図の明示とオプティマイザ利用が目的で、一意性はアプリ側で担保する。
