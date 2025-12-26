# =================== dag_data_download.py ====================================================================
# This DAG is responsible for downloading source datasets into the landing zone.
#
# Flow:
#   - Ariadb: Source URL → MongoDB → CSV (DATA_DIR/ariadb.csv)
#   - Fatalities: Source → CSV
#   - Workaccidents: Source ZIP → extracted CSV
#
# On successful completion, triggers dag_data_clean.
# ===========================================================================================================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from etl_utils import (
    download_ariadb_via_mongo,
    download_all_fatalities,
    download_and_extract_zip,
    CSV_URL,
)

# ---------------------------------------------------------------------
# Task wrappers (debug-friendly)
# ---------------------------------------------------------------------
def download_ariadb():
    """
    Ariadb ingestion flow:
        Source URL → MongoDB → CSV (DATA_DIR/ariadb.csv)
    """
    print("[INFO] Starting Ariadb ingestion via MongoDB")
    download_ariadb_via_mongo(CSV_URL, "ariadb.csv")
    print("[INFO] Ariadb ingestion completed successfully")

def download_fatalities():
    print("[INFO] Starting fatalities download")
    download_all_fatalities()
    print("[INFO] Fatalities download completed successfully")

def download_workaccidents():
    print("[INFO] Starting workaccidents download")
    download_and_extract_zip()
    print("[INFO] Workaccidents download completed successfully")

# ---------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------
with DAG(
    dag_id="dag_data_download",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["download"],
) as dag:

    t1_download_ariadb = PythonOperator(
        task_id="download_ariadb",
        python_callable=download_ariadb,
    )

    t2_download_fatalities = PythonOperator(
        task_id="download_fatalities",
        python_callable=download_fatalities,
    )

    t3_download_workaccidents = PythonOperator(
        task_id="download_workaccidents",
        python_callable=download_workaccidents,
    )

    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_data_clean",
        trigger_dag_id="dag_data_clean",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # All downloads must complete before cleaning starts
    [t1_download_ariadb, t2_download_fatalities, t3_download_workaccidents] >> trigger_clean
