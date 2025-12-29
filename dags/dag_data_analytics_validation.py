# =================== dag_data_analytics_validation.py ===============================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from etl_utils import run_analytics_validation

with DAG(
    dag_id="dag_data_analytics_validation",
    start_date=datetime(2025, 1, 1),
    schedule=None,            # or set a cron expression
    catchup=False,
    max_active_runs=1,
    tags=["analytics"]
) as dag:

    t_run_validation = PythonOperator(
        task_id="run_analytics_validation",
        python_callable=run_analytics_validation,
        op_kwargs={"save_to_file": True}  # ensure outputs are saved
    )

    t_run_validation
