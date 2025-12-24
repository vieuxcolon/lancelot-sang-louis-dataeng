# =================== dag_data_analyze.py ===============================


from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from etl_utils import (
    create_star_schema,
    min_test_star_schema,
    full_test_star_schema
)

with DAG(
    dag_id="dag_data_analyze",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["analyze"]
) as dag:

    t1 = PythonOperator(
        task_id="create_star_schema",
        python_callable=create_star_schema
    )

    t2 = PythonOperator(
        task_id="min_test_star_schema",
        python_callable=min_test_star_schema
    )

    t3 = PythonOperator(
        task_id="full_test_star_schema",
        python_callable=full_test_star_schema
    )

    trigger_validation = TriggerDagRunOperator(
        task_id="trigger_data_analytics_validation",
        trigger_dag_id="dag_data_analytics_validation",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    t1 >> t2 >> t3 >> trigger_validation
