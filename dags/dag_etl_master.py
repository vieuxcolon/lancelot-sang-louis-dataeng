# =================== dag_etl_master.py =================================
# This is the main ETL DAG that orchestrates the entire ETL process
# It uses utility functions defined in etl_utils.py to perform tasks
# such as downloading data, cleaning it, and loading it into a PostgreSQL database
# ========================================================================
# ETL Master: dag_etl_master.py

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from etl_utils import (
    download_csv, append_to_fatalities, load_to_postgres,
    create_ariadb_clean, create_fatalities_clean, create_workaccidents_clean,
    create_dimensions_and_fact, min_test_star_schema, full_test_star_schema,
    DB_CONFIG, CSV_URL, CSV_URLS_FATALITIES, download_and_extract_zip,
    drop_fatalities_table, download_all_fatalities
)

# ----------------------------------------------------------------------
# Safe wrapper functions
# ----------------------------------------------------------------------

def task_load_ariadb():
    print("=== Loading ARIADB ===")
    csv_text = download_csv(CSV_URL, "ariadb.csv")
    load_to_postgres(
        csv_content=csv_text,
        table_name=DB_CONFIG["ariadb_table"],
        skiprows=7,
        sep=";"
    )
    print("=== ARIADB loaded ===")

def task_create_ariadb_clean():
    print("=== Creating ARIADB_CLEAN ===")
    create_ariadb_clean()
    print("=== ARIADB_CLEAN done ===")

def task_download_fatalities():
    print("=== Downloading all fatalities CSVs ===")
    files = download_all_fatalities()
    print("✔ Downloaded fatalities files:")
    for f in files:
        print(f"  - {f}")
    print("=== Fatalities download done ===")

def task_drop_fatalities():
    print("=== Dropping fatalities table if exists ===")
    drop_fatalities_table()
    print("=== Drop completed ===")

def task_load_fatalities(url, idx):
    print(f"=== Loading fatalities part {idx} ===")
    csv_text = download_csv(url, f"fatalities_{idx}.csv")
    append_to_fatalities(csv_text, sep=",")
    print(f"=== fatalities_{idx} appended ===")

def task_create_fatalities_clean():
    print("=== Creating FATALITIES_CLEAN ===")
    create_fatalities_clean()
    print("=== FATALITIES_CLEAN done ===")

def task_load_workaccidents():
    print("=== Loading Workaccidents ===")
    csv_text = download_and_extract_zip()
    load_to_postgres(
        csv_content=csv_text,
        table_name=DB_CONFIG["workaccidents_table"],
        sep=","
    )
    print("=== WORKACCIDENTS loaded ===")

def task_create_workaccidents_clean():
    print("=== Creating WORKACCIDENTS_CLEAN ===")
    create_workaccidents_clean()
    print("=== WORKACCIDENTS_CLEAN done ===")

# ----------------------------------------------------------------------
# DAG
# ----------------------------------------------------------------------

with DAG(
    dag_id="dag_etl_master",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # --- ARIADB ---
    t1_load_ariadb = PythonOperator(
        task_id="load_ariadb",
        python_callable=task_load_ariadb
    )
    t2_ariadb_clean = PythonOperator(
        task_id="ariadb_clean",
        python_callable=task_create_ariadb_clean
    )

    # --- Download Fatalities ---
    t_download_fatalities = PythonOperator(
        task_id="download_fatalities",
        python_callable=task_download_fatalities
    )

    # --- Fatalities Load (existing tasks unchanged) ---
    t0_drop_fatalities = PythonOperator(
        task_id="drop_fatalities",
        python_callable=task_drop_fatalities
    )

    t3_fatalities = []
    for i, url in enumerate(CSV_URLS_FATALITIES, start=1):
        t = PythonOperator(
            task_id=f"load_fatalities_{i}",
            python_callable=lambda u=url, idx=i: task_load_fatalities(u, idx)
        )
        t3_fatalities.append(t)

    t4_fatalities_clean = PythonOperator(
        task_id="fatalities_clean",
        python_callable=task_create_fatalities_clean
    )

    # --- Workaccidents ---
    t5_load_workaccidents = PythonOperator(
        task_id="load_workaccidents",
        python_callable=task_load_workaccidents
    )
    t6_workaccidents_clean = PythonOperator(
        task_id="workaccidents_clean",
        python_callable=task_create_workaccidents_clean
    )

    # --- Star Schema ---
    t7_star_schema = PythonOperator(
        task_id="create_star_schema",
        python_callable=create_dimensions_and_fact
    )
    t8_min_test = PythonOperator(
        task_id="min_test_star_schema",
        python_callable=min_test_star_schema
    )
    t9_full_test = PythonOperator(
        task_id="full_test_star_schema",
        python_callable=full_test_star_schema
    )

    # -----------------------
    # DAG dependencies
    # -----------------------

    # ARIADB
    t1_load_ariadb >> t2_ariadb_clean

    # Fatalities: download → drop → load_1 → ... → clean
    t2_ariadb_clean >> t_download_fatalities >> t0_drop_fatalities
    t0_drop_fatalities >> t3_fatalities[0]

    for i in range(len(t3_fatalities) - 1):
        t3_fatalities[i] >> t3_fatalities[i + 1]

    t3_fatalities[-1] >> t4_fatalities_clean

    # Workaccidents chain
    t5_load_workaccidents >> t6_workaccidents_clean

    # Star schema waits for all clean tables
    [t2_ariadb_clean, t4_fatalities_clean, t6_workaccidents_clean] >> t7_star_schema

    # Tests
    t7_star_schema >> t8_min_test >> t9_full_test
