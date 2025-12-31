# =================== dag_data_download.py ====================================================================
# This DAG is responsible for downloading ARIADB, Workaccidents, and Fatalities data into the landing zone.
# No direct loading to Postgres or cleaning is done here; this DAG just ensures the raw data files exist on disk.
# All raw files are written to $DATA_DIR. Loading/cleaning into Postgres is now handled in dag_data_clean.
# ===========================================================================================================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import os
from etl_utils import (
    download_ariadb_via_mongo,
    download_all_fatalities,
    download_and_extract_zip,
    CSV_URL,
    ZIP_URL,
    DATA_DIR
)

def task_download_ariadb():
    print(f"[INFO] Downloading ARIADB to {os.path.join(DATA_DIR, 'ariadb.csv')} via MongoDB")
    download_ariadb_via_mongo(CSV_URL, batch_size=5000)
    print(f"✔ ARIADB CSV ready at {os.path.join(DATA_DIR, 'ariadb.csv')}")

def task_download_fatalities():
    files = download_all_fatalities()
    for f in files:
        print(f"✔ Fatalities file downloaded: {f}")

def task_download_workaccidents():
    csv_path = download_and_extract_zip(ZIP_URL)
    print(f"✔ Workaccidents CSV downloaded/extracted to {csv_path}")

with DAG(
    dag_id="dag_data_download",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    t_download_ariadb = PythonOperator(task_id="t_download_ariadb", python_callable=task_download_ariadb)
    t_download_fatalities = PythonOperator(task_id="t_download_fatalities", python_callable=task_download_fatalities)
    t_download_workaccidents = PythonOperator(task_id="t_download_workaccidents", python_callable=task_download_workaccidents)

    t_trigger_clean = TriggerDagRunOperator(
        task_id="t_trigger_data_clean",
        trigger_dag_id="dag_data_clean",
        wait_for_completion=True
    )

    [t_download_ariadb, t_download_fatalities, t_download_workaccidents] >> t_trigger_clean
