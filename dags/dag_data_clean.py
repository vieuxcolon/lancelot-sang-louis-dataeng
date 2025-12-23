# =================== dag_data_clean.py ===============================================================================================================
# This is the dag responsible for processing, cleaning, transforming and standardizing raw data from the landing zone and move it into the staging zone
# It uses utility functions defined in etl_utils.py to perform the various tasks of data cleaning and transformation
# the resulting clean datasets include ariadb_clean, workaccidents_clean and fatalities_clean which are loaded into clean database tables
# =====================================================================================================================================================

# =================== dag_data_clean.py =====================================================================
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from etl_utils import (
    create_ariadb_clean, create_workaccidents_clean, create_fatalities_clean,
    create_ariadb_prep, create_workaccidents_prep, create_fatalities_prep
)

with DAG(
    dag_id="dag_data_clean",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # -------------------- CLEAN TASKS --------------------
    t_clean_ariadb = PythonOperator(
        task_id="clean_ariadb",
        python_callable=create_ariadb_clean
    )

    t_clean_work = PythonOperator(
        task_id="clean_workaccidents",
        python_callable=create_workaccidents_clean
    )

    t_clean_fatalities = PythonOperator(
        task_id="clean_fatalities",
        python_callable=create_fatalities_clean
    )

    # -------------------- PREP TASKS --------------------
    t_prep_ariadb = PythonOperator(
        task_id="prep_ariadb",
        python_callable=create_ariadb_prep
    )

    t_prep_work = PythonOperator(
        task_id="prep_workaccidents",
        python_callable=create_workaccidents_prep
    )

    t_prep_fatalities = PythonOperator(
        task_id="prep_fatalities",
        python_callable=create_fatalities_prep
    )

    # -------------------- DAG TRIGGER --------------------
    trigger_analyze = TriggerDagRunOperator(
        task_id="trigger_data_analyze",
        trigger_dag_id="dag_data_analyze",
        wait_for_completion=True
    )

    # -------------------- DEPENDENCIES --------------------
    # Clean tasks run in parallel
    [t_clean_ariadb, t_clean_work, t_clean_fatalities] >> [t_prep_ariadb, t_prep_work, t_prep_fatalities]
    
    # Each prep depends only on its corresponding clean
    t_clean_ariadb >> t_prep_ariadb
    t_clean_work >> t_prep_work
    t_clean_fatalities >> t_prep_fatalities

    # Trigger analyze DAG after all prep tasks complete
    [t_prep_ariadb, t_prep_work, t_prep_fatalities] >> trigger_analyze
