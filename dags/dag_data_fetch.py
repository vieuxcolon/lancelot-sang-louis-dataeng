# =================== dag_data_fetch.py ====================================================================
# This is the dag responsible for downloading ariab, workaccidents and fatalities data into the landing zone
# It uses utility functions defined in etl_utils.py to perform tasks
# such as downloading data, unzipping data, and loading it into a PostgreSQL database
# ===========================================================================================================

# =================== dag_data_fetch.py ===================
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from etl_utils import (
    create_database_and_set_config,
    create_ariadb_prep,
    create_workaccidents_prep,
    create_fatalities_prep,
    create_star_schema,
    DB_CONFIG
)

# -------------------------------------------------------------------------
# Task: create_db_dataengdb
# -------------------------------------------------------------------------
def task_create_project_db():
    """Create 'dataengdb' if it does not exist and update DB_CONFIG to point to it."""
    create_database_and_set_config("dataengdb")

# -------------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------------
with DAG(
    dag_id="dag_data_fetch",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["dataeng"]
) as dag:

    # 0️⃣ Create database if it doesn't exist
    t0_create_db = PythonOperator(
        task_id="create_db_dataengdb",
        python_callable=task_create_project_db
    )

    # 1️⃣ Create ariadb prep table
    t1_create_ariadb_prep = PythonOperator(
        task_id="create_ariadb_prep",
        python_callable=create_ariadb_prep
    )

    # 2️⃣ Create workaccidents prep table
    t2_create_workaccidents_prep = PythonOperator(
        task_id="create_workaccidents_prep",
        python_callable=create_workaccidents_prep
    )

    # 3️⃣ Create fatalities prep table
    t3_create_fatalities_prep = PythonOperator(
        task_id="create_fatalities_prep",
        python_callable=create_fatalities_prep
    )

    # 4️⃣ Create star schema (dimensions + fact)
    t4_create_star_schema = PythonOperator(
        task_id="create_star_schema",
        python_callable=create_star_schema
    )

    # ---------------------------------------------------------------------
    # Define DAG dependencies
    # ---------------------------------------------------------------------
    t0_create_db >> [t1_create_ariadb_prep, t2_create_workaccidents_prep, t3_create_fatalities_prep] >> t4_create_star_schema

