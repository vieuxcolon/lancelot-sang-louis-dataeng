# =================== dag_data_analyze.py =============================================================================================================
# This dag is responsible for processing clean data moving it from the staging zone to the production zone.
# It creates dimensions tables, the star schemas, perform a minimal and full test of the star schema using utilities defined in etl_utils.py
# The created star schema in the production zone is ready for further analytical workloads  
# =====================================================================================================================================================
# dag_data_analyze.py

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from etl_utils import create_dimensions_and_fact, min_test_star_schema, full_test_star_schema

with DAG(
    dag_id="dag_data_analyze",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    t1_star_schema = PythonOperator(task_id="create_star_schema", python_callable=create_dimensions_and_fact)
    t2_min_test = PythonOperator(task_id="min_test_star_schema", python_callable=min_test_star_schema)
    t3_full_test = PythonOperator(task_id="full_test_star_schema", python_callable=full_test_star_schema)

    t1_star_schema >> t2_min_test >> t3_full_test
