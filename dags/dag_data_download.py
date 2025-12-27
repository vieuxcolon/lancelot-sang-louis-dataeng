# =================== dag_data_download.py ====================================================================
# This DAG is responsible for downloading ARIADB, Workaccidents, and Fatalities data into the landing zone.
# No direct loading to Postgres or cleaning is done here; this DAG just ensures the raw data files exist on disk.
# ===========================================================================================================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from etl_utils import (
    download_csv,
    download_all_fatalities,
    download_and_extract_zip,
    CSV_URL,
    DATA_DIR
)

def task_download_ariadb():
    """
    Download ARIADB CSV and save it to disk ($DATA_DIR/ariadb.csv)
    """
    csv_path = download_csv(CSV_URL, "ariadb.csv", skiprows=7)
    print(f"✔ ARIADB CSV downloaded to {csv_path}")

def task_download_fatalities():
    """
    Download all fatalities files and save them to disk
    """
    files = download_all_fatalities()
    for f in files:
        print(f"✔ Fatalities file downloaded: {f}")

def task_download_workaccidents():
    """
    Download and extract workaccidents ZIP file to disk
    """
    csv_path = download_and_extract_zip()
    print(f"✔ Workaccidents CSV downloaded/extracted to {csv_path}")

with DAG(
    dag_id="dag_data_download",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # Parallel download tasks
    t1_ariadb = PythonOperator(task_id="download_ariadb", python_callable=task_download_ariadb)
    t2_fatalities = PythonOperator(task_id="download_fatalities", python_callable=task_download_fatalities)
    t3_workaccidents = PythonOperator(task_id="download_workaccidents", python_callable=task_download_workaccidents)

    # Trigger cleaning DAG after all downloads are complete
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_data_clean",
        trigger_dag_id="dag_data_clean",
        wait_for_completion=True
    )

    [t1_ariadb, t2_fatalities, t3_workaccidents] >> trigger_clean
