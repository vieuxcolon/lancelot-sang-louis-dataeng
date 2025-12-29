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

    t_clean_ariadb = PythonOperator(
        task_id="t_clean_ariadb",
        python_callable=create_ariadb_clean
    )

    t_clean_workaccidents = PythonOperator(
        task_id="t_clean_workaccidents",
        python_callable=create_workaccidents_clean
    )

    t_clean_fatalities = PythonOperator(
        task_id="t_clean_fatalities",
        python_callable=create_fatalities_clean
    )

    t_trigger_prep = TriggerDagRunOperator(
        task_id="t_trigger_data_prep",
        trigger_dag_id="dag_data_bprep",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    [t_clean_ariadb, t_clean_workaccidents, t_clean_workaccidents] >> t_trigger_prep
