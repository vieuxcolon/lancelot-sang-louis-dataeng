# =================== dag_data_fetch.py ====================================================================
# This is the dag responsible for downloading ariab, workaccidents and fatalities data into the landing zone
# It uses utility functions defined in etl_utils.py to perform tasks
# such as downloading data, unzipping data, and loading it into a PostgreSQL database
# ===========================================================================================================
# ETL Master: dag_etl_master.py
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from etl_utils import download_csv, download_all_fatalities, download_and_extract_zip, DB_CONFIG, CSV_URL, CSV_URLS_FATALITIES, load_to_postgres, append_to_fatalities

def task_load_ariadb():
    csv_text = download_csv(CSV_URL, "ariadb.csv")
    load_to_postgres(csv_text, table_name=DB_CONFIG["ariadb_table"], skiprows=7, sep=";")

def task_download_fatalities():
    files = download_all_fatalities()
    for f in files:
        print(f"Downloaded {f}")

def task_load_fatalities(url, idx):
    csv_text = download_csv(url, f"fatalities_{idx}.csv")
    append_to_fatalities(csv_text)

def task_load_workaccidents():
    csv_text = download_and_extract_zip()
    load_to_postgres(csv_text, table_name=DB_CONFIG["workaccidents_table"], sep=",")

with DAG(
    dag_id="dag_data_fetch",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    t1_load_ariadb = PythonOperator(task_id="load_ariadb", python_callable=task_load_ariadb)
    t2_download_fatalities = PythonOperator(task_id="download_fatalities", python_callable=task_download_fatalities)
    t3_fatalities = [PythonOperator(task_id=f"load_fatalities_{i}", python_callable=lambda u=url, idx=i: task_load_fatalities(u, idx))
                     for i, url in enumerate(CSV_URLS_FATALITIES, start=1)]
    t4_load_workaccidents = PythonOperator(task_id="load_workaccidents", python_callable=task_load_workaccidents)

    t1_load_ariadb >> t2_download_fatalities
    t2_download_fatalities >> t3_fatalities[0]
    for i in range(len(t3_fatalities)-1):
        t3_fatalities[i] >> t3_fatalities[i+1]
    t3_fatalities[-1] >> t4_load_workaccidents
