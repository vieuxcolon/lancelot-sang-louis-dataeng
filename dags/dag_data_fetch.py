# =================== dag_data_fetch.py ====================================================================
# This is the dag responsible for downloading ariab, workaccidents and fatalities data into the landing zone
# It uses utility functions defined in etl_utils.py to perform tasks
# such as downloading data, unzipping data, and loading it into a PostgreSQL database
# ===========================================================================================================

# =================== dag_data_fetch.py ====================================================================
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from etl_utils import (
    download_csv, download_all_fatalities, download_and_extract_zip,
    load_to_postgres, DB_CONFIG, CSV_URL, DATA_DIR
)

# -------------------- TASK FUNCTIONS --------------------
def task_load_ariadb():
    csv_text = download_csv(CSV_URL, "ariadb.csv")
    load_to_postgres(csv_text, table_name=DB_CONFIG["ariadb_table"], skiprows=7, sep=";")

def task_download_fatalities():
    files = download_all_fatalities()
    for f in files:
        print(f"Downloaded {f}")

def task_load_workaccidents():
    csv_text = download_and_extract_zip()
    load_to_postgres(csv_text, table_name=DB_CONFIG["workaccidents_table"], sep=",")

# -------------------- DAG DEFINITION --------------------
with DAG(
    dag_id="dag_data_fetch",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # Load raw datasets (parallel)
    t1_load_ariadb = PythonOperator(task_id="load_ariadb", python_callable=task_load_ariadb)
    t2_download_fatalities = PythonOperator(task_id="download_fatalities", python_callable=task_download_fatalities)
    t3_load_workaccidents = PythonOperator(task_id="load_workaccidents", python_callable=task_load_workaccidents)

    # Trigger next DAG: dag_data_clean synchronously
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_data_clean",
        trigger_dag_id="dag_data_clean",
        wait_for_completion=True
    )

    # -------------------- DEPENDENCIES --------------------
    # All fetch tasks can run in parallel before triggering the clean DAG
    [t1_load_ariadb, t2_download_fatalities, t3_load_workaccidents] >> trigger_clean
