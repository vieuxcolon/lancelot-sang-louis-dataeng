# =================== dag_data_prep.py ===============================================================================================================
# DAG responsible for processing, transforming and standardizing clean data into star-schema ready data from the staging zone
# It uses utility functions defined in etl_utils.py to perform the various tasks of data normalization and standardization
# Resulting prep datasets include ariadb_prep, workaccidents_prep, and fatalities_prep which are used as source for the star schema (dim and fact tables)
# =====================================================================================================================================================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from etl_utils import (
    create_ariadb_prep,
    create_workaccidents_prep,
    create_fatalities_prep
)

with DAG(
    dag_id="dag_data_cprep",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["prep"]
) as dag:

    t1 = PythonOperator(
        task_id="create_ariadb_prep",
        python_callable=create_ariadb_prep
    )

    t2 = PythonOperator(
        task_id="create_workaccidents_prep",
        python_callable=create_workaccidents_prep
    )

    t3 = PythonOperator(
        task_id="create_fatalities_prep",
        python_callable=create_fatalities_prep
    )

    trigger_analyze = TriggerDagRunOperator(
        task_id="trigger_data_analyze",
        trigger_dag_id="dag_data_analyze",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    [t1, t2, t3] >> trigger_analyze
