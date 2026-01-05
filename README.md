# Report: Occupational Accidents Analytics Platform

![Insalogo](./images/logo-insa_0.png)

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

Students:

-   Loe Louis-Marie (louis-marie.loe@insa-lyon.fr)
-   Lancelot Tariot Camille (lancelot.tariot-camille@insa-lyon.fr)
-   Nguyen Sang (sang.nguyen@insa-lyon.fr)

## Table of contents

-   [Introduction](#introduction)
-   [Data sources](#data-sources)
-   [Pipeline](#pipeline)
    -   [Ingestion](#ingestion)
    -   [Staging](#staging)
        -   [Cleansing](#cleansing)
        -   [Transformations](#transformations)
        -   [Enrichments](#enrichments)
    -   [Production](#production)
        -   [Star schema](#star-schema)
        -   [Queries](#queries)
-   [Environment](#environment)
-   [How to run](#how-to-run)
    -   [Automatic](#automatic)
    -   [Manual](#manual)
-   [Validation and monitoring](#validation-and-monitoring)
-   [Future developments](#future-developments)
-   [Project submission checklist](#project-submission-checklist)
-   [License](#license)

## Introduction

This project implements an end-to-end, containerized data pipeline that ingests public occupational accident datasets (ARIA and OSHA), cleans and standardizes them, and builds a PostgreSQL star schema for reproducible analytics. The report you are reading is the full project report, including the pipeline design, run instructions, and analysis queries.

## Data sources

-   **ARIADB (France)**: [Accidents industriels et technologiques (ARIA)](https://www.data.gouv.fr/api/1/datasets/r/e4a3ad9a-cc9d-40c6-8d1a-aebdf75ded7b)
-   **OSHA Work Accident Case Files (USA)**: [January 2015 to March 2025 accident case files](https://www.osha.gov/sites/default/files/January2015toMarch2025.zip)
-   **OSHA Fatality Summaries (USA)**: nine CSVs published by OSHA for fiscal years 2009-2017 (`CSV_URLS_FATALITIES` in `dags/etl_utils.py`)

## Pipeline

![Pipeline overview](./images/pipeline-overview.jpeg)

The Airflow orchestration chain is:

1. `dag_data_download` (download raw datasets to `/opt/airflow/data`)
2. `dag_data_clean` (cleaning + standardized tables)
3. `dag_data_bprep` (prep tables ready for star schema)
4. `dag_data_analyze` (dimensions, fact table, tests)
5. `dag_data_analytics_validation` (automated validation queries)

### Ingestion

-   ARIADB is downloaded via a MongoDB staging step to enforce schema-safe ingestion before exporting to CSV and loading to Postgres.
-   OSHA work accidents are downloaded as a ZIP file and extracted to a single CSV.
-   OSHA fatalities are downloaded as nine CSV files with retries and local fallback.

### Staging

#### Cleansing

-   Column names are normalized (lowercase snake_case, accents removed).
-   Dates are standardized to ISO format.
-   Duplicates are removed on primary identifiers (e.g., `aria_id`, `upa`).
-   Countries are normalized (e.g., USA/US/United States -> `USA`).

#### Transformations

-   Raw tables are filtered to the required columns for analytics.
-   Clean tables are reshaped into `*_prep` tables to align with star schema keys.
-   Types are enforced for numeric identifiers and metrics.

#### Enrichments

-   Country codes are derived for `dim_country`.
-   Employer names and hazard classes are standardized before dimension loading.

### Production

The production phase builds a star schema in `data_db` and runs validation queries.

#### Star schema

![Star schema](./images/star-schema.png)

-   Dimensions: `dim_date`, `dim_country`, `dim_industry`, `dim_accident_type`, `dim_hazard`, `dim_employer`, `dim_fatality`
-   Fact table: `fact_accidents` (one record per standardized accident)

#### Queries

The following 10 queries are the curated analysis set for manual execution in `data_db` once all DAGs finish.

1. Fatalities by country, year, and industry

```sql
SELECT
  c.country_name,
  EXTRACT(YEAR FROM d.date) AS year,
  i.industry_code,
  SUM(fa.no_of_fatality) AS total_fatalities
FROM fact_accidents fa
JOIN dim_country c ON fa.country_id = c.country_id
JOIN dim_date d ON fa.date_id = d.date_id
JOIN dim_industry i ON fa.industry_id = i.industry_id
GROUP BY c.country_name, year, i.industry_code
ORDER BY total_fatalities DESC;
```

2. Most dangerous hazard classes by accident type

```sql
SELECT
  h.hazard_class,
  at.accident_type,
  COUNT(*) AS accident_count,
  SUM(fa.no_of_fatality) AS total_fatalities
FROM fact_accidents fa
JOIN dim_hazard h ON fa.hazard_id = h.hazard_id
JOIN dim_accident_type at ON fa.accident_type_id = at.accident_type_id
GROUP BY h.hazard_class, at.accident_type
ORDER BY total_fatalities DESC;
```

3. Employers with the highest fatality counts

```sql
SELECT
  e.employer,
  SUM(fa.no_of_fatality) AS total_fatalities,
  COUNT(*) AS accident_count
FROM fact_accidents fa
JOIN dim_employer e ON fa.employer_id = e.employer_id
GROUP BY e.employer
HAVING SUM(fa.no_of_fatality) > 0
ORDER BY total_fatalities DESC;
```

4. Fatality severity distribution by industry

```sql
SELECT
  i.industry_code,
  f.fatality_label,
  COUNT(*) AS accident_count
FROM fact_accidents fa
JOIN dim_industry i ON fa.industry_id = i.industry_id
JOIN dim_fatality f ON fa.fatality_id = f.fatality_id
GROUP BY i.industry_code, f.fatality_label
ORDER BY i.industry_code, accident_count DESC;
```

5. Fatalities over time by hazard class

```sql
SELECT
  d.date,
  h.hazard_class,
  SUM(fa.no_of_fatality) AS total_fatalities
FROM fact_accidents fa
JOIN dim_date d ON fa.date_id = d.date_id
JOIN dim_hazard h ON fa.hazard_id = h.hazard_id
GROUP BY d.date, h.hazard_class
ORDER BY d.date;
```

6. Country risk profile by accident type

```sql
SELECT
  c.country_name,
  at.accident_type,
  COUNT(*) AS accident_count,
  SUM(fa.no_of_fatality) AS total_fatalities
FROM fact_accidents fa
JOIN dim_country c ON fa.country_id = c.country_id
JOIN dim_accident_type at ON fa.accident_type_id = at.accident_type_id
GROUP BY c.country_name, at.accident_type
ORDER BY c.country_name, total_fatalities DESC;
```

7. Average fatalities per accident by industry

```sql
SELECT
  i.industry_code,
  AVG(fa.no_of_fatality * 1.0) AS avg_fatalities_per_accident
FROM fact_accidents fa
JOIN dim_industry i ON fa.industry_id = i.industry_id
GROUP BY i.industry_code
ORDER BY avg_fatalities_per_accident DESC;
```

8. Employer-hazard fatality matrix (LEFT JOIN used to avoid data loss)

```sql
SELECT
  e.employer,
  h.hazard_class,
  SUM(fa.no_of_fatality) AS total_fatalities
FROM fact_accidents fa
LEFT JOIN dim_employer e ON fa.employer_id = e.employer_id
LEFT JOIN dim_hazard h ON fa.hazard_id = h.hazard_id
GROUP BY e.employer, h.hazard_class
HAVING SUM(fa.no_of_fatality) > 0
ORDER BY total_fatalities DESC;
```

9. Fatalities by country and year (trend-ready)

```sql
SELECT
  c.country_name,
  EXTRACT(YEAR FROM d.date) AS year,
  SUM(fa.no_of_fatality) AS total_fatalities
FROM fact_accidents fa
JOIN dim_country c ON fa.country_id = c.country_id
JOIN dim_date d ON fa.date_id = d.date_id
GROUP BY c.country_name, year
ORDER BY c.country_name, year;
```

10. Full analytical view (denormalized, safe for BI)

```sql
SELECT
  d.date,
  c.country_name,
  i.industry_code,
  e.employer,
  at.accident_type,
  h.hazard_class,
  f.fatality_label,
  fa.no_of_fatality
FROM fact_accidents fa
LEFT JOIN dim_date d ON fa.date_id = d.date_id
LEFT JOIN dim_country c ON fa.country_id = c.country_id
LEFT JOIN dim_industry i ON fa.industry_id = i.industry_id
LEFT JOIN dim_employer e ON fa.employer_id = e.employer_id
LEFT JOIN dim_accident_type at ON fa.accident_type_id = at.accident_type_id
LEFT JOIN dim_hazard h ON fa.hazard_id = h.hazard_id
LEFT JOIN dim_fatality f ON fa.fatality_id = f.fatality_id;
```

## Environment

### Stack

-   Apache Airflow 3.1 (Celery Executor + Redis)
-   PostgreSQL 16 (`data_db` for project tables)
-   MongoDB 7 (ARIADB staging)

### Services

| Service           | URL                   | Default credentials        | Notes                                                                     |
| ----------------- | --------------------- | -------------------------- | ------------------------------------------------------------------------- |
| Airflow UI / API  | http://localhost:8080 | `airflow` / `airflow`      | Unpause and trigger DAGs, inspect task logs, clear runs.                  |
| pgAdmin           | http://localhost:5050 | `admin@admin.com` / `root` | Use for database browsing. Default connection is available under Servers. |
| Mongo Express     | http://localhost:8085 | `admin` / `admin`          | Inspect the temporary ARIADB collection after download.                   |
| Redis             | n/a                   | n/a                        | Used internally by Airflow Celery, no UI exposed.                         |

PgAdmin connection details (if you create a new server):

-   Hostname: `postgres`
-   Maintenance database: `postgres`
-   Username: `data_user`
-   Password: `root`

Databases in Postgres:

-   `airflow` for Airflow metadata only
-   `postgres` as the maintenance database
-   `data_db` for all project tables

Python dependencies are installed from `requirements.txt`. It includes only the runtime dependencies required by the ETL (Airflow, pandas, pymongo, psycopg2-binary).

## How to run

### Automatic

```powershell
docker compose up --build -d
docker compose ps
```

### Manual

1. Review `.env`, update ports or credentials if needed.
2. Start the stack: `docker compose up --build -d`.
3. Open Airflow at http://localhost:8080 and trigger `dag_data_download`.
4. The DAGs will chain automatically until analytics validation completes.
5. Query results are in Postgres `data_db` and validation outputs are in `./data/analytics_results`.

Notes:

-   The ETL is idempotent and deterministic. Each run drops and recreates the same set of tables from fixed sources.
-   Do not run analytical queries while DAGs are running. Run queries either before launching a new run or after all DAGs finish successfully.

## Validation and monitoring

-   `dag_data_analyze` runs `min_test_star_schema` and `full_test_star_schema` before analytics.
-   `dag_data_analytics_validation` fails fast if required dimensions are empty and stores SQL + results for auditing.
-   Airflow logs are mounted under `./logs`; raw and analytics outputs are in `./data`.

## Future developments

-   Provide offline sample datasets to allow full testing without network access.
-   Extend the star schema with additional dimensions (e.g., region or employer sector).

## Project submission checklist

-   [x] Repository with the code, well documented
-   [x] Docker-compose file to run the environment
-   [x] Detailed description of the various steps
-   [x] Report in the README with project design steps divided per area
-   [ ] Example dataset for offline testing (sample data not yet included)
-   [ ] Slides for the project poster (add `poster.md` or `slides.md`)
-   [x] Airflow + pandas + MongoDB + Postgres used in the pipeline
-   [x] Star schema built in Postgres

## License

This project is released under the [CC0 1.0 Universal](LICENSE) license.
