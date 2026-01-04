# DataEng 2024 - Occupational Accidents Analytics Platform

![Insalogo](./images/logo-insa_0.png)

Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).

Students:

-   Loe Louis-Marie (louis-marie.loe@insa-lyon.fr)
-   Lancelot Tariot Camille (lancelot.tariot-camille@insa-lyon.fr)
-   Nguyen Sang (sang.nguyen@insa-lyon.fr)

## Abstract

This repository implements an end-to-end, fully containerized ELT platform for analysing occupational accidents reported in Europe and the United States between 2009 and 2025. Apache Airflow orchestrates every step, from downloading the public datasets to running reproducible analytics on a dimensional model hosted in PostgreSQL.

Key capabilities include:

-   Automated ingestion of the ARIADB catalog (France), OSHA work accident case files (USA), and OSHA fatality summaries (USA) with resilient retry and fallback strategies.
-   Deterministic cleaning, deduplication, and enrichment logic that standardizes dates, locations, industries, hazards, and employers before persisting data in dedicated raw/clean/prep schemas.
-   Creation of a narrow star schema (six dimensions plus one fact table) that unlocks multi-dimensional analytics on top of occupational safety KPIs.
-   Built-in data quality gates (unit tests and analytics validation) plus GitHub Actions automation that converts the README into a submission-ready PDF after every `master` push.

## Architecture Overview

### Components

-   **Apache Airflow 3.1** (Celery Executor + Redis) orchestrates the ETL. All DAGs live in `dags/`, and `etl_utils.py` hosts the shared logic.
-   **PostgreSQL 16** stores raw data, clean/prep tables, and the star schema inside the `data_db` database. Initialization scripts are located under `postgres/init/`.
-   **MongoDB 7** (plus Mongo Express) temporarily stages the ARIADB feed to guarantee schema-safe ingestion before exporting to CSV/Postgres.
-   **Apache Druid 28** is provisioned for optional near-real-time analytics. Credentials and metadata settings live in `.env` and `postgres/init/02_init_druid.sql`.
-   **Redis 7.2**, **pgAdmin**, bind-mounted `./logs` and `./data` folders, and health checks defined in `docker-compose.yml` round out the stack.
-   **GitHub Actions** workflows in `.github/workflows/` provide a simple CI placeholder (`blank.yml`) and the markdown-to-PDF pipeline used during submission (`2pdf.yml`).

### DAG lineage

1. `dag_data_download`: downloads ARIADB (via Mongo), nine OSHA fatality CSVs, and the OSHA work-accident ZIP into `/opt/airflow/data`.
2. `dag_data_clean`: normalizes, deduplicates, and loads the raw files into `ariadb_clean`, `workaccidents_clean`, and `fatalities_clean` tables.
3. `dag_data_bprep`: reshapes the clean tables into schema-ready prep tables (`*_prep`), harmonizing keys, enforcing PK/FK integrity, and casting numeric fields.
4. `dag_data_analyze`: builds and populates all dimension and fact tables, then runs `min_test_star_schema` and `full_test_star_schema` before triggering downstream analytics.
5. `dag_data_analytics_validation`: executes the automated validation queries, persisting every result set (SQL + output) under `/opt/airflow/data/analytics_results`.

## Dataset Description

### 1. ARIADB (France)

-   **Source**: [Accidents industriels et technologiques (ARIA)](https://www.data.gouv.fr/api/1/datasets/r/e4a3ad9a-cc9d-40c6-8d1a-aebdf75ded7b).
-   **What it contains**: detailed reports about industrial accidents worldwide, including identifiers, dates, publication types, NAF/industry codes, departments, and narrative event classifications.
-   **Processing highlights**: data is ingested through MongoDB to guarantee resilient CSV parsing, column names are sanitized (lowercase snake_case without accents), dates are converted to ISO strings, and duplicates are removed by `aria_id`. Country names are standardized through `normalize_country`, and final columns feed `ariadb_clean` and `ariadb_prep`.

### 2. OSHA Work Accident Case Files (USA)

-   **Source**: [January 2015 to March 2025 accident case files](https://www.osha.gov/sites/default/files/January2015toMarch2025.zip).
-   **What it contains**: US OSHA investigation case records with employer names, NAICS codes, event narratives, geospatial coordinates, hospitalization flags, and enforcement metadata.
-   **Processing highlights**: the ZIP is downloaded once and unpacked to `workaccidents.csv`. After loading to Postgres (`workaccidents`), wide text columns (`final_narrative`) are dropped, dates are normalized to `accident_date`, the dataset is flagged as USA-only, and records are deduplicated on `upa`. The curated subset powers `workaccidents_clean` and `workaccidents_prep`.

### 3. OSHA Fatality Summaries (USA)

-   **Source**: Nine CSVs published by OSHA for fiscal years 2009-2017 (see `CSV_URLS_FATALITIES` in `etl_utils.py`).
-   **What it contains**: short descriptions of fatal incidents, including employer references, victims, hazards, and investigation dates.
-   **Processing highlights**: each file has a dedicated cleaner (column renaming, redundant-row removal, and per-row `no_of_fatalities=1`). All cleaners write intermediate `_clean` files that are concatenated into `fatalities_clean.csv` and stored in Postgres (`fatalities_clean`, `fatalities_prep`) with ISO dates and consistent country tags.

## Star Schema Output

The prep tables converge into a compact star schema built inside `data_db`:

-   `dim_date` (unique calendar dates) and `dim_country` (country names plus ISO-like codes) provide the temporal and geographic axes.
-   `dim_industry`, `dim_accident_type`, `dim_hazard`, and `dim_employer` expose categorical drill-downs populated from all prep sources with conflict-free inserts.
-   `dim_fatality` stores the fatality magnitude dictionary (currently a single "ACCIDENT" entry, but designed for extensibility).
-   `fact_accidents` captures one row per standardized accident, referencing all dimension keys plus `no_of_fatality`.

## Analytics Queries

`dag_data_analytics_validation` runs the automated checks defined in `dags/etl_utils.py`. The following 10 queries are the curated analysis set for manual execution in `data_db` once all DAGs finish:

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

## Repository Layout

-   `dags/`: Airflow DAG definitions (`dag_data_*.py`) and the shared ETL utilities in `etl_utils.py`.
-   `.github/workflows/`: CI placeholders plus the markdown-to-PDF workflow used for submissions.
-   `postgres/init/`: SQL scripts that provision `data_db`/`data_user` (plus a Druid metadata database) on container start.
-   `docker-compose.yml`: multi-service stack (Airflow, Postgres, Redis, Mongo, Druid, pgAdmin, Mongo Express) with bind mounts for `./logs` and `./data`.
-   `requirements.txt` and `Dockerfile`: pin the Python environment installed inside Airflow images.
-   `images/` and `pdfs/`: assets reused by the README and by the GitHub Action output artifacts.

## Requirements

-   Docker Desktop 4.x (or a compatible Linux Docker Engine) with the Compose v2 plugin (>= 2.20).
-   At least 4 CPU cores, 16 GB RAM, and 20 GB of free disk space (Postgres + Druid use persistent volumes).
-   Internet egress to download public datasets the first time each DAG runs.
-   Optional: an AWS EC2 instance (x86_64) with the same specs for remote deployment.
-   Git for cloning the repository and a shell capable of running the commands below.

Python dependencies are installed from `requirements.txt`. It now includes only the runtime dependencies required by the ETL (Airflow, pandas, pymongo, psycopg2-binary).

## Setup & Usage

### 1. Clone and configure the environment

```powershell
git clone https://github.com/<your-org>/lancelot-sang-louis-dataeng.git
cd lancelot-sang-louis-dataeng
```

Review `.env`, update passwords/ports if needed, and keep sensitive overrides out of version control whenever you deploy to shared infrastructure.

### 2. Start the stack

```powershell
docker compose up --build -d
docker compose ps
```

The first run builds the Airflow image (installing `requirements.txt`) and initializes the databases defined under `postgres/init/`.

### 3. Access the services

| Service           | URL                   | Default credentials        | Notes                                                                       |
| ----------------- | --------------------- | -------------------------- | --------------------------------------------------------------------------- |
| Airflow UI / API  | http://localhost:8080 | `airflow` / `airflow`      | Unpause and trigger DAGs, inspect task logs, clear runs.                    |
| pgAdmin           | http://localhost:5050 | `admin@admin.com` / `root` | Use for database browsing. Default connection is available under Servers.   |
| Mongo Express     | http://localhost:8085 | `admin` / `admin`          | Inspect the temporary ARIADB collection after download.                     |
| Druid Coordinator | http://localhost:8081 | `druid` / `druid`          | Optional; validates that metadata storage is reachable.                     |
| Redis             | n/a                   | n/a                        | Used internally by Airflow Celery, no UI exposed.                           |

PgAdmin connection details (if you create a new server):
- Hostname: `postgres`
- Maintenance database: `postgres`
- Username: `data_user`
- Password: `root`

Databases in Postgres:
- `airflow` for Airflow metadata only
- `druid` for Druid metadata only
- `postgres` as the maintenance database
- `data_db` for all project tables

### 4. Run the pipeline in Airflow

1. Unpause `dag_data_download` and trigger it once. The downstream DAGs (`dag_data_clean`, `dag_data_bprep`, `dag_data_analyze`, `dag_data_analytics_validation`) are chained through `TriggerDagRunOperator` and will run in order.
2. Alternatively, trigger from the CLI: `docker compose exec airflow-worker airflow dags trigger dag_data_download`.
3. Each DAG writes raw files to `./data` (host) / `/opt/airflow/data` (containers) and persists curated tables into Postgres `data_db`.
4. Inspect the analytics outputs or rerun any DAG on demand to refresh the data lake.

Notes:
- The ETL is idempotent and deterministic. Each run drops and recreates the same set of tables from fixed sources.
- Do not run analytical queries while DAGs are running. Run queries either before launching a new run or after all DAGs finish successfully.

### 5. Retrieve outputs

-   Query the dimensional model with any SQL client that connects to Postgres on `localhost:5432` using `data_user` / `root`.
-   Analytics validation artifacts are text files located at `./data/analytics_results/*.txt`. Example:

```powershell
docker compose exec airflow-worker ls -lh /opt/airflow/data/analytics_results
```

### 6. Stop and clean up

```powershell
docker compose down
# Remove persistent volumes if you need a clean slate
docker compose down -v
```

## Validation, Testing, and Monitoring

-   `dag_data_analyze` automatically runs `min_test_star_schema` (FK sanity check) and `full_test_star_schema` (joined preview) before any analytics job runs.
-   `dag_data_analytics_validation` fails fast if required dimensions are empty and stores both SQL text and results for auditing.
-   Airflow logs are mounted under `./logs`, while raw/clean data and analytics outputs live under `./data` for easy inspection.
-   Docker health checks (Redis, Postgres, Airflow API) raise failures early; use `docker compose logs -f <service>` for troubleshooting.

## License

This project is released under the [CC0 1.0 Universal](LICENSE) license.
