# =================== dag_data_download.py ===============================================================================================================
# DAG responsible for downloading raw data files from source URLs or external systems
# into the landing zone for subsequent ETL and cleaning.
# =====================================================================================================================================================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.baseoperator import chain
from datetime import datetime
import os

from etl_utils import (
    download_ariadb_via_mongo,
    download_workaccidents_csv,
    download_fatalities_csv,
    DATA_DIR
)

with DAG(
    dag_id="dag_data_download",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["download"]
) as dag:

    # ------------------------- Task: Download ARIADB -------------------------
    download_ariadb = PythonOperator(
        task_id="download_ariadb_via_mongo",
        python_callable=lambda: download_ariadb_via_mongo(
            url="https://example.com/ariadb_source.csv",
            output_filename=os.path.join(DATA_DIR, "ariadb_raw.csv")
        )
    )

    # ------------------------- Task: Download Work Accidents -------------------------
    download_workaccidents = PythonOperator(
        task_id="download_workaccidents",
        python_callable=download_workaccidents_csv
    )

    # ------------------------- Task: Download Fatalities -------------------------
    download_fatalities = PythonOperator(
        task_id="download_fatalities",
        python_callable=download_fatalities_csv
    )

    # ------------------------- Trigger downstream DAG -------------------------
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_data_clean",
        trigger_dag_id="dag_data_clean",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    # ------------------------- Task dependencies -------------------------
    # Ensure downloads finish before triggering cleaning
    chain(
        [download_ariadb, download_workaccidents, download_fatalities],
        trigger_clean
    )
