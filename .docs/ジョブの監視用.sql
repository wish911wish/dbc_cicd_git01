SELECT
  jrt.job_id,
  j.name AS job_name,
  jrt.run_id,
  MIN(jrt.period_start_time) AS run_start_time,
  MAX(jrt.period_end_time) AS run_end_time,
  MAX(jrt.result_state) AS result_state,
  'ALERT: Job not finished by 03:00' AS alert_message
FROM system.lakeflow.job_run_timeline jrt
JOIN system.lakeflow.jobs j
  ON jrt.job_id = j.job_id AND jrt.workspace_id = j.workspace_id
GROUP BY jrt.job_id, jrt.run_id, j.name
-- HAVING MAX(jrt.result_state) IS NULL -- 未完了のJobを対象とする場合は、この条件を有効にする
  -- OR MAX(jrt.period_end_time) > DATE_TRUNC('DAY', CURRENT_TIMESTAMP()) + INTERVAL 3 HOURS
