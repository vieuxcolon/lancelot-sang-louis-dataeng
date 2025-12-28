Purpose:
    This DAG performs the full ETL and validation process for the star schema
    used in the analytics pipeline. It builds dimension and fact tables from
    preprocessed datasets, performs minimal and full schema tests, and triggers
    the downstream analytics validation DAG.

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from etl_utils import (
    drop_dimensions,
    drop_fact,
    create_dimensions,
    populate_dimensions,
    create_fact,
    populate_fact,
    min_test_star_schema,
    full_test_star_schema,
    log_and_count  # <-- centralized helper in etl_utils
)

with DAG(
    dag_id="dag_data_analyze",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["analyze"]
) as dag:

    # =======================
    # Star Schema Build TaskGroup
    # =======================
    with TaskGroup("star_schema_build") as star_schema_group:

        t_drop_dimensions = PythonOperator(
            task_id="drop_dimensions",
            python_callable=log_and_count(drop_dimensions, "Drop Dimensions")
        )

        t_drop_fact = PythonOperator(
            task_id="drop_fact",
            python_callable=log_and_count(drop_fact, "Drop Fact Table")
        )

        t_create_dimensions = PythonOperator(
            task_id="create_dimensions",
            python_callable=log_and_count(create_dimensions, "Create Dimension Tables")
        )

        t_populate_dimensions = PythonOperator(
            task_id="populate_dimensions",
            python_callable=log_and_count(populate_dimensions, "Populate Dimension Tables")
        )

        t_create_fact = PythonOperator(
            task_id="create_fact",
            python_callable=log_and_count(create_fact, "Create Fact Table")
        )

        t_populate_fact = PythonOperator(
            task_id="populate_fact",
            python_callable=log_and_count(populate_fact, "Populate Fact Table", table_name="fact_accidents")
        )

        # Explicit ordering inside TaskGroup
        t_drop_dimensions >> t_drop_fact
        t_drop_fact >> t_create_dimensions >> t_populate_dimensions
        t_populate_dimensions >> t_create_fact >> t_populate_fact

    # =======================
    # Tests
    # =======================
    t_min_test = PythonOperator(
        task_id="min_test_star_schema",
        python_callable=log_and_count(min_test_star_schema, "Minimal Star Schema Test")
    )

    t_full_test = PythonOperator(
        task_id="full_test_star_schema",
        python_callable=log_and_count(full_test_star_schema, "Full Star Schema Test")
    )

    # =======================
    # Trigger validation DAG
    # =======================
    trigger_validation = TriggerDagRunOperator(
        task_id="trigger_data_analytics_validation",
        trigger_dag_id="dag_data_analytics_validation",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    # =======================
    # Full DAG ordering
    # =======================
    star_schema_group >> t_min_test >> t_full_test >> trigger_validation
