# =================== dag_data_analyze.py =================================================================
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from etl_utils import create_dimensions_and_fact, min_test_star_schema, full_test_star_schema

with DAG(
    dag_id="dag_data_analyze",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # -----------------------------
    # ETL / Star Schema Tasks
    # -----------------------------
    t1_star_schema = PythonOperator(
        task_id="create_star_schema",
        python_callable=create_dimensions_and_fact
    )

    t2_min_test = PythonOperator(
        task_id="min_test_star_schema",
        python_callable=min_test_star_schema
    )

    t3_full_test = PythonOperator(
        task_id="full_test_star_schema",
        python_callable=full_test_star_schema
    )

    # -----------------------------
    # Trigger Analytics DAG
    # -----------------------------
    t4_trigger_analytics = TriggerDagRunOperator(
        task_id="trigger_analytics_validation",
        trigger_dag_id="dag_analytics_validation",  # DAG ID of your analytics validation DAG
        wait_for_completion=True,  # optional: wait for the analytics DAG to finish
        poke_interval=60
    )

    # -----------------------------
    # Task Dependencies
    # -----------------------------
    t1_star_schema >> t2_min_test >> t3_full_test >> t4_trigger_analytics
