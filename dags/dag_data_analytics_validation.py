# =================== dag_data_analytics_validation.py ================================================================================================
# It uses utility functions defined the run_analytics_validation of etl_utils.py to execute the following 9 analytical queries:
# 1. Fatalities by Year, 2. Fatalities by Country, 3. Fatalities by Industry, 4. Evolution of fatalities France vs USA (last decade),
# 5. Top 10 countries by fatalities last year, 6. Top 10 industries by fatalities last year, 7. Countries with zero fatalities last year (top 10)
# 8. Fatalities trend by top 5 countries (last 5 years), 9. Fatalities per country per month (last year, top 10 records) 
# =====================================================================================================================================================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from etl_utils import run_analytics_validation

# ----------------------------
# DAG Definition
# ----------------------------
with DAG(
    dag_id="dag_data_analytics_validation",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["analytics"]
) as dag:

    # Standardized task name: t_ + python function name
    t_run_analytics_validation = PythonOperator(
        task_id="run_analytics_validation",
        python_callable=run_analytics_validation
    )

# No dependencies required, single task

