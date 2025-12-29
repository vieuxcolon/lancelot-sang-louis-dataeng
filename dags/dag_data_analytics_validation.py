# =================== dag_data_analytics_validation.py ===============================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from etl_utils import run_analytics_validation

# ----------------------------
# DAG Definition
# ----------------------------
with DAG(
    dag_id="dag_data_analytics_validation",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["analytics"]
) as dag:

    # Standardized task name: t_ + python function name
    t_run_analytics_validation = PythonOperator(
        task_id="run_analytics_validation",
        python_callable=run_analytics_validation
    )

# No dependencies required, single task

