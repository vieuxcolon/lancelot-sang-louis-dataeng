# ===================== dag_data_analytics_validation.py =====================
# This DAG is responsible for validating the analytics correctness of the star schema.
# It runs queries against the fact and dimension tables and prints summary results.
# ============================================================================

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from etl_utils import pg_connect  # Reuse the same connection helper

# -------------------------------------------------------------------------
# Helper for formatted printing
# -------------------------------------------------------------------------
def print_section(title, df):
    print("\n" + "="*80)
    print(f"== {title.upper()} ==")
    print("="*80 + "\n")
    print(df.to_string(index=False))
    print("\n" + "="*80 + "\n")

# -------------------------------------------------------------------------
# Query functions
# -------------------------------------------------------------------------
def fatalities_by_year():
    conn = pg_connect()
    df = pd.read_sql(
        """
        SELECT d.year, COUNT(*) AS total_fatalities
        FROM fact_accidents f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.year
        ORDER BY d.year
        """,
        conn,
    )
    conn.close()
    print_section("Fatalities by Year", df)
    return df

def fatalities_by_country():
    conn = pg_connect()
    df = pd.read_sql(
        """
        SELECT c.country_name, COUNT(*) AS total_fatalities
        FROM fact_accidents f
        JOIN dim_location l ON f.location_id = l.location_id
        JOIN dim_country c ON l.country_id = c.country_id
        GROUP BY c.country_name
        ORDER BY total_fatalities DESC
        """,
        conn,
    )
    conn.close()
    print_section("Fatalities by Country", df)
    return df

def top_locations(n=10):
    conn = pg_connect()
    df = pd.read_sql(
        f"""
        SELECT l.municipality, l.department, c.country_name, COUNT(*) AS fatalities
        FROM fact_accidents f
        JOIN dim_location l ON f.location_id = l.location_id
        JOIN dim_country c ON l.country_id = c.country_id
        GROUP BY l.municipality, l.department, c.country_name
        ORDER BY fatalities DESC
        LIMIT {n}
        """,
        conn,
    )
    conn.close()
    print_section(f"Top {n} Locations by Fatalities", df)
    return df

def fatalities_by_industry():
    conn = pg_connect()
    df = pd.read_sql(
        """
        SELECT i.industry_code, COUNT(*) AS total_fatalities
        FROM fact_accidents f
        JOIN dim_industry i ON f.industry_id = i.industry_id
        GROUP BY i.industry_code
        ORDER BY total_fatalities DESC
        """,
        conn,
    )
    conn.close()
    print_section("Fatalities by Industry", df)
    return df

def full_validation_report():
    print("\n" + "="*100)
    print("FULL ANALYTICS VALIDATION REPORT".center(100))
    print("="*100 + "\n")
    fatalities_by_year()
    fatalities_by_country()
    top_locations()
    fatalities_by_industry()
    print("\n" + "="*100)
    print("VALIDATION COMPLETE".center(100))
    print("="*100 + "\n")

# -------------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------------
with DAG(
    dag_id="dag_data_analytics_validation",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Can be scheduled daily or weekly if needed
    catchup=False,
    max_active_runs=1,
) as dag:

    t1_year = PythonOperator(
        task_id="fatalities_by_year",
        python_callable=fatalities_by_year
    )

    t2_country = PythonOperator(
        task_id="fatalities_by_country",
        python_callable=fatalities_by_country
    )

    t3_top_locations = PythonOperator(
        task_id="top_locations",
        python_callable=top_locations
    )

    t4_industry = PythonOperator(
        task_id="fatalities_by_industry",
        python_callable=fatalities_by_industry
    )

    t5_full_report = PythonOperator(
        task_id="full_validation_report",
        python_callable=full_validation_report
    )

    # Task dependencies: individual queries first, then full report
    [t1_year, t2_country, t3_top_locations, t4_industry] >> t5_full_report
