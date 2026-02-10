# ログ_Jobの実行結果をauditログから取得する

Auditログから必要なデータを取得する[https://docs.databricks.com/aws/en/admin/account-settings/audit-logs#job-events](https://docs.databricks.com/aws/en/admin/account-settings/audit-logs#job-events)

```
select
  event_time,
  action_name,
  request_params.jobId,
  request_params.taskKey,
  response.error_message
from system.access.audit
where service_name = 'jobs'
  and action_name in ('runStart', 'runSucceeded', 'runFailed') ;


```


![auditログからJobのログを取得する01](/.docs/images/auditログからJobのログを取得する01.png)

