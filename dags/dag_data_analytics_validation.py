# =================== dag_data_analytics_validation.py ===============================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from etl_utils import pg_connect

def print_section(title, df):
    print("\n" + "="*80)
    print(f"== {title.upper()} ==")
    print("="*80 + "\n")
    print(df.to_string(index=False))

# ----------------------------
# Fatalities by Year
# ----------------------------
def fatalities_by_year():
    conn = pg_connect()
    df = pd.read_sql("""
        SELECT d.date AS year, COUNT(*) AS total_fatalities
        FROM fact_accidents f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.date
        ORDER BY d.date
    """, conn)
    conn.close()
    print_section("Fatalities by Year", df)

# ----------------------------
# Fatalities by Country
# ----------------------------
def fatalities_by_country():
    conn = pg_connect()
    df = pd.read_sql("""
        SELECT c.country_name AS country, COUNT(*) AS total_fatalities
        FROM fact_accidents f
        JOIN dim_country c ON f.country_id = c.country_id
        GROUP BY c.country_name
        ORDER BY total_fatalities DESC
    """, conn)
    conn.close()
    print_section("Fatalities by Country", df)

# ----------------------------
# Fatalities by Industry
# ----------------------------
def fatalities_by_industry():
    conn = pg_connect()
    df = pd.read_sql("""
        SELECT i.industry_code AS industry, COUNT(*) AS total_fatalities
        FROM fact_accidents f
        JOIN dim_industry i ON f.industry_id = i.industry_id
        GROUP BY i.industry_code
        ORDER BY total_fatalities DESC
    """, conn)
    conn.close()
    print_section("Fatalities by Industry", df)

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

    t1 = PythonOperator(
        task_id="fatalities_by_year",
        python_callable=fatalities_by_year
    )

    t2 = PythonOperator(
        task_id="fatalities_by_country",
        python_callable=fatalities_by_country
    )

    t3 = PythonOperator(
        task_id="fatalities_by_industry",
        python_callable=fatalities_by_industry
    )

    [t1, t2, t3]
