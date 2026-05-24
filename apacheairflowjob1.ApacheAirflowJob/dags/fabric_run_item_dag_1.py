from airflow import DAG
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunJobOperator
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricPipelineJobParameters
from datetime import datetime, timedelta

with DAG(
  dag_id="weekday_pipeline_orchestration",
  start_date=datetime(2026, 5, 24),
  # CRON explanation: 21:30 (30 21), Every Day (* *), Monday-Friday (1-5)
  schedule_interval="30 21 * * 1-5",
  catchup=False,
) as dag:

  # Pipeline
  run_item = MSFabricRunJobOperator(
    task_id="runPipelineTask",
    fabric_conn_id="fabric-integration",
    workspace_id="4db246e1-0a39-4ba9-a437-cf43cb1ec668",  # Stock price watermark pipeline
    item_id="7c04d7f2-ab5f-4194-8719-fe7ea7a32ee9",  # pl_stock_incremental_load
    job_type="Pipeline",
    timeout=1200,
    deferrable=True,
    job_params= MSFabricPipelineJobParameters()
        .set_parameter("p_load_mode", "INCREMENTAL")
        .set_parameter("p_full_load_start_date", "2023-01-01")
        .set_parameter("p_api_key", "DJ77SC1NH6YDCP6Y")
        .to_json()
  )

  # Task will run when DAG is triggered