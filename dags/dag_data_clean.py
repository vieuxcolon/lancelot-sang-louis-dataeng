# =================== dag_data_clean.py ===============================================================================================================
# DAG responsible for processing, cleaning, transforming and standardizing raw data from the landing zone
# into the staging zone. Clean datasets include ariadb_clean, workaccidents_clean, and fatalities_clean.
# =====================================================================================================================================================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.baseoperator import chain
from datetime import datetime
import os

from etl_utils import (
    download_ariadb_via_mongo,
    create_ariadb_clean,
    create_workaccidents_clean,
    create_fatalities_clean,
    DATA_DIR
)

# ------------------------- DAG definition -------------------------
with DAG(
    dag_id="dag_data_clean",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["clean"]
) as dag:

    # ------------------------- Task: Download ARIADB from Mongo -------------------------
    download_ariadb = PythonOperator(
        task_id="download_ariadb_via_mongo",
        python_callable=lambda: download_ariadb_via_mongo(
            url="https://example.com/ariadb_source.csv",
            output_filename=os.path.join(DATA_DIR, "ariadb_raw.csv")
        )
    )

    # ------------------------- Task: Clean ARIADB using CSV -------------------------
    clean_ariadb = PythonOperator(
        task_id="clean_ariadb",
        python_callable=create_ariadb_clean
    )

    # ------------------------- Other clean tasks -------------------------
    clean_workaccidents = PythonOperator(
        task_id="clean_workaccidents",
        python_callable=create_workaccidents_clean
    )

    clean_fatalities = PythonOperator(
        task_id="clean_fatalities",
        python_callable=create_fatalities_clean
    )

    # ------------------------- Trigger downstream DAG -------------------------
    trigger_prep = TriggerDagRunOperator(
        task_id="trigger_data_prep",
        trigger_dag_id="dag_data_prep",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    # ------------------------- Task dependencies -------------------------
    # ARIADB flow: download → clean → downstream
    download_ariadb >> clean_ariadb

    # Other flows
    chain(clean_ariadb, clean_workaccidents, clean_fatalities, trigger_prep)
