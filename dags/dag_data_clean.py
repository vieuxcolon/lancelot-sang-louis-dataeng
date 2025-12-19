# =================== dag_data_clean.py ===============================================================================================================
# This is the dag responsible for processing, cleaning, transforming and standardizing raw data from the landing zone and move it into the staging zone
# It uses utility functions defined in etl_utils.py to perform the various tasks of data cleaning and transformation
# the resulting clean datasets include ariadb_clean, workaccidents_clean and fatalities_clean which are loaded into clean database tables
# =====================================================================================================================================================
# dag_data_clean.py

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from etl_utils import create_ariadb_clean, create_fatalities_clean, create_workaccidents_clean

with DAG(
    dag_id="dag_data_clean",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    t1_ariadb_clean = PythonOperator(task_id="ariadb_clean", python_callable=create_ariadb_clean)
    t2_fatalities_clean = PythonOperator(task_id="fatalities_clean", python_callable=create_fatalities_clean)
    t3_workaccidents_clean = PythonOperator(task_id="workaccidents_clean", python_callable=create_workaccidents_clean)

    t1_ariadb_clean >> t2_fatalities_clean >> t3_workaccidents_clean
