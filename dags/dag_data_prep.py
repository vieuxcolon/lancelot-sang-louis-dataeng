# =================== dag_data_prep.py ===============================

# =================== dag_data_prep.py ===================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from etl_utils import (
    create_database_and_set_config,
    create_ariadb_prep,
    create_workaccidents_prep,
    create_fatalities_prep,
    create_star_schema
)

def create_project_db():
    create_database_and_set_config("airflow")

with DAG(
    dag_id="dag_data_prep",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["prep"]
) as dag:

    t0 = PythonOperator(
        task_id="create_db",
        python_callable=create_project_db
    )

    t1 = PythonOperator(
        task_id="create_ariadb_prep",
        python_callable=create_ariadb_prep
    )

    t2 = PythonOperator(
        task_id="create_workaccidents_prep",
        python_callable=create_workaccidents_prep
    )

    t3 = PythonOperator(
        task_id="create_fatalities_prep",
        python_callable=create_fatalities_prep
    )

    t4 = PythonOperator(
        task_id="create_star_schema",
        python_callable=create_star_schema
    )

    trigger_analyze = TriggerDagRunOperator(
        task_id="trigger_data_analyze",
        trigger_dag_id="dag_data_analyze",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"]
    )

    t0 >> [t1, t2, t3] >> t4 >> trigger_analyze
