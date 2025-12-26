# =================== dag_data_download.py ====================================================================
# This is the dag responsible for downloading ariab, workaccidents and fatalities data into the landing zone
# It uses utility functions defined in etl_utils.py to perform tasks
# such as downloading data, unzipping data, and loading it into a PostgreSQL database
# ===========================================================================================================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from etl_utils import ( download_ariadb_via_mongo, download_all_fatalities, download_and_extract_zip, CSV_URL )

def download_ariadb():
    """
    Source → MongoDB → CSV (DATA_DIR/ariadb.csv)
    """
    download_ariadb_via_mongo(CSV_URL, "ariadb.csv")

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
    tags=["download"]
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
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    [t1, t2, t3] >> trigger_clean
