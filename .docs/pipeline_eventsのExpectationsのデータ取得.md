


```

from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()

# Job実行結果からpipeline_idを辿る
run_result = ws.jobs.get_run(run_id=run_id)

# JobのタスクからDLT pipeline IDを取得
pipeline_id = next(
    task.pipeline_task.pipeline_id
    for task in run_result.tasks
    if task.pipeline_task is not None
)

# Pipeline eventsからExpectations統計を取得
events = ws.pipelines.list_pipeline_events(pipeline_id=pipeline_id)

expectation_events = [
    e for e in events
    if e.event_type == "flow_progress"
    and e.details is not None
    and "expectations" in (e.details or {})
]


```

eventのdetails
```

{
  "flow_progress": {
    "metrics": {
      "num_output_rows": 980
    },
    "data_quality": {
      "dropped_records": 20,
      "expectations": [
        {
          "name": "valid_amount",
          "dataset": "silver_orders",
          "passed_records": 980,
          "failed_records": 20
        }
      ]
    }
  }
}

```

```

for event in expectation_events:
    dq = event.details.get("flow_progress", {}).get("data_quality", {})
    dropped = dq.get("dropped_records", 0)
    for exp in dq.get("expectations", []):
        print(f"{exp['name']}: failed={exp['failed_records']}, passed={exp['passed_records']}")

```
