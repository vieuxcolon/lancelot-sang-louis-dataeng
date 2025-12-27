# =================== dag_data_clean.py ===============================================================================================================
# DAG responsible for processing, cleaning, transforming and standardizing raw data from the landing zone
# It uses utility functions defined in etl_utils.py to perform the various tasks of data cleaning and transformation
# Resulting clean datasets include ariadb_clean, workaccidents_clean, and fatalities_clean which are loaded into clean database tables
# =====================================================================================================================================================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.baseoperator import chain
from datetime import datetime

from etl_utils import (
    create_ariadb_clean,
    create_workaccidents_clean,
    create_fatalities_clean
)

with DAG(
    dag_id="dag_data_clean",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["clean"]
) as dag:

    # Task 1: Clean ARIADB (loads raw CSV internally, creates ariadb_clean table)
    t1 = PythonOperator(
        task_id="clean_ariadb",
        python_callable=create_ariadb_clean
    )

    # Task 2: Clean Workaccidents (loads raw CSV internally, creates workaccidents_clean table)
    t2 = PythonOperator(
        task_id="clean_workaccidents",
        python_callable=create_workaccidents_clean
    )

    # Task 3: Clean Fatalities (no raw table exists, cleans and creates fatalities_clean table)
    t3 = PythonOperator(
        task_id="clean_fatalities",
        python_callable=create_fatalities_clean
    )

    # Trigger next DAG: dag_data_prep
    trigger_prep = TriggerDagRunOperator(
        task_id="trigger_data_prep",
        trigger_dag_id="dag_data_prep",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    # Define dependencies
    chain([t1, t2, t3], trigger_prep)
