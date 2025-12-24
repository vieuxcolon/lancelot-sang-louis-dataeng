# =================== dag_data_download.py ===============================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from etl_utils import (
    download_csv,
    download_all_fatalities,
    download_and_extract_zip,
    CSV_URL
)

def download_ariadb():
    download_csv(CSV_URL, "ariadb.csv")

def download_fatalities():
    download_all_fatalities()

def download_workaccidents():
    download_and_extract_zip()

with DAG(
    dag_id="dag_data_download",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["download", "landing"]
) as dag:

    t1 = PythonOperator(
        task_id="download_ariadb",
        python_callable=download_ariadb
    )

    t2 = PythonOperator(
        task_id="download_fatalities",
        python_callable=download_fatalities
    )

    t3 = PythonOperator(
        task_id="download_workaccidents",
        python_callable=download_workaccidents
    )

    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_data_clean",
        trigger_dag_id="dag_data_clean",
        wait_for_completion=False
    )

    [t1, t2, t3] >> trigger_clean
