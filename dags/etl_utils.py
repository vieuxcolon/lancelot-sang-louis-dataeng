# ==================== etl_utils.py =================================
# The utilities contained therein are used by dag_etl_master.py
# This module provides utility functions for ETL processes
# specifically for downloading, cleaning, and loading datasets

# etl_utils.py well formatted and self-documented.

import os
import pandas as pd
import psycopg2
import requests
from io import StringIO, BytesIO
from zipfile import ZipFile
from datetime import datetime
import warnings
import logging
from time import sleep

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


def pg_connect():
    """Safe Postgres connection for ETL functions"""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


# =====================================================================================
# DATA DIRECTORY (Docker)
# =====================================================================================
DATA_DIR = "/opt/airflow/data"
os.makedirs(DATA_DIR, exist_ok=True)

# =====================================================================================
# CONFIGURATION
# =====================================================================================

CSV_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/e4a3ad9a-cc9d-40c6-8d1a-aebdf75ded7b"
)


ZIP_URL = "https://www.osha.gov/sites/default/files/January2015toMarch2025.zip"

# Docker Postgres connection
DB_CONFIG = {
    "host": "postgres",  # Important: Docker hostname
    "port": 5432,
    "database": "airflow",
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "ariadb_table": "ariadb",
    "ariadb_clean_table": "ariadb_clean",
    "workaccidents_table": "workaccidents",
    "workaccidents_clean_table": "workaccidents_clean",
    "fatalities_table": "fatalities",
    "fatalities_clean_table": "fatalities_clean",
}


# ============================================================
# Download ALL fatalities CSV files (robust, Code-1 + fallback)
# ============================================================

CSV_URLS_FATALITIES = [
    "https://www.osha.gov/sites/default/files/fy17_federal-state_summaries.csv",
    "https://www.osha.gov/sites/default/files/fy16_federal-state_summaries.csv",
    "https://www.osha.gov/sites/default/files/fy15_federal-state_summaries.csv",
    "https://www.osha.gov/sites/default/files/fy14_federal-state_summaries.csv",
    "https://www.osha.gov/sites/default/files/fy13_federal-state_summaries.csv",
    "https://www.osha.gov/sites/default/files/FatalitiesFY12.csv",
    "https://www.osha.gov/sites/default/files/FatalitiesFY11.csv",
    "https://www.osha.gov/sites/default/files/FatalitiesFY10.csv",
    "https://www.osha.gov/sites/default/files/FatalitiesFY09.csv",
]


def download_all_fatalities():
    """
    Downloads all 9 OSHA fatality CSVs using a robust method.
    Saves into DATA_DIR as fatalities_1.csv ... fatalities_9.csv.
    Falls back to existing local file if download fails.
    Returns list of file paths.
    """

    os.makedirs(DATA_DIR, exist_ok=True)
    saved_files = []

    headers = {"User-Agent": "Mozilla/5.0 (ETLBot/1.0)", "Accept": "text/csv,*/*;q=0.8"}

    for i, url in enumerate(CSV_URLS_FATALITIES, start=1):
        filename = f"fatalities_{i}.csv"
        local_path = os.path.join(DATA_DIR, filename)

        logger.info(f"📥 Downloading fatalities file {i}: {url}")

        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()

            with open(local_path, "wb") as f:
                f.write(r.content)

            logger.info(f"✅ Saved {filename} ({len(r.content)} bytes)")

        except Exception as e:
            logger.warning(
                f"⚠️ Failed to download {url}: {e}. Trying local fallback..."
            )

            if not os.path.exists(local_path):
                raise FileNotFoundError(f"❌ No local fallback available for {filename}")
            else:
                logger.info(f"📄 Using existing local copy: {local_path}")

        saved_files.append(local_path)
        sleep(1)  # politeness delay toward OSHA servers

    logger.info("✔ All fatalities files obtained (downloaded or fallback).")
    return saved_files


# =====================================================================================
# CSV READING
# =====================================================================================


def read_csv_robust(csv_content, sep=",", skiprows=0, dtype=str):
    encodings = ["utf-8", "latin1"]
    for enc in encodings:
        try:
            return pd.read_csv(
                StringIO(csv_content),
                sep=sep,
                skiprows=skiprows,
                dtype=dtype,
                encoding=enc,
                low_memory=False,
            )
        except Exception:
            continue
    raise ValueError("Failed to read CSV with UTF-8 or Latin-1 encoding")


# =====================================================================================
# COLUMN CLEANING
# =====================================================================================


def clean_column_names(df):
    df.columns = [
        c.strip()
        .lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("#", "")
        .rstrip("_")
        for c in df.columns
    ]
    return df


# =====================================================================================
# DOWNLOAD CSV
# =====================================================================================


def download_csv(URL, filename):
    filepath = os.path.join(DATA_DIR, filename)
    print(f"📥 Trying download: {URL}")

    try:
        r = requests.get(URL, timeout=60)
        r.raise_for_status()

        with open(filepath, "wb") as f:
            f.write(r.content)

        print(f"✅ Download OK ({len(r.content)} bytes)")
        print(f"💾 Saved to {filepath}")

        return r.text

    except Exception as e:
        print(f"⚠️ Download error: {e}, checking local copy.")

        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()

        raise FileNotFoundError(f"❌ Neither URL nor local file available: {URL}")


# =====================================================================================
# ZIP DOWNLOAD
# =====================================================================================

# ==================== etl_utils.py ====================


def download_and_extract_zip(zip_url=None):
    url = zip_url or ZIP_URL
    filename = os.path.basename(url)
    filepath = os.path.join(DATA_DIR, filename)

    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(r.content)
        zip_bytes = BytesIO(r.content)
        print(f"✅ ZIP download OK ({len(r.content)} bytes)")
    except Exception:
        if not os.path.exists(filepath):
            raise FileNotFoundError("❌ ZIP missing online and local")
        zip_bytes = open(filepath, "rb")

    with ZipFile(zip_bytes) as zf:
        csv_files = [f for f in zf.namelist() if f.lower().endswith(".csv")]
        target = csv_files[0]
        with zf.open(target) as f:
            return f.read().decode("utf-8", errors="ignore")


# =====================================================================================
# LOAD TO POSTGRES (FULL REPLACE)
# =====================================================================================


def load_to_postgres(csv_content, table_name, skiprows=0, sep=";"):
    conn = pg_connect()
    cursor = conn.cursor()

    df = read_csv_robust(csv_content, sep=sep, skiprows=skiprows, dtype=str)
    df = clean_column_names(df)

    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')

    col_defs = ", ".join([f'"{c}" TEXT' for c in df.columns])
    cursor.execute(f'CREATE TABLE "{table_name}" ({col_defs});')

    insert_sql = f"""
        INSERT INTO "{table_name}"
        ({", ".join([f'"{c}"' for c in df.columns])})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """

    for _, row in df.iterrows():
        cursor.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✔ Loaded table: {table_name} ({len(df)} rows)")


# ============================================================
# LOAD FATALITIES HELPERS
# ============================================================


def load_fatalities_first(csv_text, sep=","):
    """Drop & recreate fatalities table before loading fatalities_1."""
    return load_to_postgres(
        csv_content=csv_text,
        table_name=DB_CONFIG["fatalities_table"],
        sep=sep,
        skiprows=0,  # fatalities have no metadata rows
        drop_if_exists=True,  # FORCE DROP
    )


def append_fatalities_next(csv_text, sep=","):
    """Append fatalities_2, fatalities_3, etc."""
    return append_to_fatalities(csv_text, sep=sep)


# ============================================================
# Safe DROP fatalities table
# ============================================================


def drop_fatalities_table():
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS fatalities;")
        conn.commit()
        logger.info("🗑️ Dropped table fatalities (if existed).")
    except Exception as e:
        logger.error(f"⚠️ Error dropping fatalities table: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


"""
def drop_fatalities_table():
    
    Safely drop the fatalities table if it exists.
    Never throws an error if the table is missing.
    
    conn = pg_connect()
    cur = conn.cursor()
    try:
        cur.execute(f'DROP TABLE IF EXISTS "{DB_CONFIG["fatalities_table"]}"')
        conn.commit()
        print("✔ fatalities table dropped (if existed)")
    finally:
        conn.close()
"""

# ============================================================
# Load all fatalities sequentially:
#   - drop table
#   - load fatalities_1 as CREATE
#   - append fatalities_2..9 as APPEND
# ============================================================


def load_all_fatalities():
    """
    Master function:
    - downloads all CSVs (with fallback)
    - drops fatalities table
    - loads fatalities_1 as new table
    - appends fatalities_2..9
    """
    files = download_all_fatalities()
    drop_fatalities_table()

    for i, path in enumerate(files, start=1):
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            csv_text = f.read()

        if i == 1:
            logger.info("📌 Creating fatalities table using part 1")
            append_to_fatalities(csv_text, sep=",", create_if_missing=True)
        else:
            logger.info(f"📌 Appending fatalities part {i}")
            append_to_fatalities(csv_text, sep=",", create_if_missing=False)

    logger.info("✔ Fatalities table fully rebuilt from 9 CSVs.")


def append_to_fatalities(csv_content, table_name=None, sep=","):
    table = table_name or DB_CONFIG["fatalities_table"]

    conn = pg_connect()
    cursor = conn.cursor()

    df = read_csv_robust(csv_content, sep=sep)
    df = clean_column_names(df)

    # Ensure table exists
    col_defs = ", ".join([f'"{c}" TEXT' for c in df.columns])
    cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs});')

    # Add missing columns dynamically
    cursor.execute(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
    )
    existing = {r[0] for r in cursor.fetchall()}

    for col in df.columns:
        if col not in existing:
            cursor.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT;')
            print(f"🟡 Added new column: {col}")

    insert_sql = f"""
        INSERT INTO "{table}"
        ({", ".join([f'"{c}"' for c in df.columns])})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """

    for _, row in df.iterrows():
        cursor.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✔ Appended to {table}: {len(df)} rows")


# =====================================================================================
# ADDRESS PARSER (unchanged)
# =====================================================================================


def parse_address(raw):
    if raw is None or not isinstance(raw, str):
        return pd.Series([None] * 5)

    raw = raw.replace("  ", " ").strip()

    if "," in raw:
        employer, rest = raw.split(",", 1)
        employer = employer.strip()
        rest = rest.strip()
    else:
        return pd.Series([raw, None, None, None, None])

    tokens = rest.split()
    if len(tokens) < 3:
        return pd.Series([employer, None, None, None, None])

    zip_code = tokens[-1] if tokens[-1].isdigit() else None
    state = tokens[-2]
    city = tokens[-3]
    address = " ".join(tokens[:-3]) if len(tokens) > 3 else None

    return pd.Series([employer, address, zip_code, city, state])


# =====================================================================================
# CREATE FATALITIES CLEAN  (FINAL, GUARDED, PRODUCTION-SAFE)
# =====================================================================================

from psycopg2.extras import execute_batch


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def read_csv_safe(path):
    return pd.read_csv(
        path,
        sep=",",
        quotechar='"',
        encoding="latin1",
        engine="python",
    )


def finalize(df):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()]
    df["no_of_fatalities"] = 1
    df["country"] = "USA"
    return df[["date", "no_of_fatalities", "country"]]


# ------------------------------------------------------------------
# Fatalities cleaners
# ------------------------------------------------------------------

def clean_fatalities_1_2(file, out):
    df = read_csv_safe(file)
    df = df.iloc[1:]
    df.columns = ["date", "employer", "victim", "hazard", "fatality", "inspection"]
    df = df[df["fatality"].notna()]
    df = finalize(df)
    df.to_csv(out, index=False)
    return len(df)


def clean_fatalities_3(file, out):
    df = read_csv_safe(file)
    df.columns = [
        "date",
        "company",
        "victim",
        "description",
        "fatality",
        "inspection",
    ] + list(df.columns[6:])
    df = df[df["fatality"].notna()]
    df = finalize(df)
    df.to_csv(out, index=False)
    return len(df)


def clean_fatalities_4(file, out):
    df = read_csv_safe(file)
    df.columns = ["date", "company", "description", "fatality", "inspection"] + list(
        df.columns[5:]
    )
    df = df[df["fatality"].notna()]
    df = finalize(df)
    df.to_csv(out, index=False)
    return len(df)


def clean_fatalities_5(file, out):
    df = read_csv_safe(file)
    df.columns = ["date", "company", "description", "fatality"]
    df = df[df["fatality"].notna()]
    df = finalize(df)
    df.to_csv(out, index=False)
    return len(df)


def clean_fatalities_6_to_9(file, out):
    df = read_csv_safe(file)
    df = df.iloc[:, :4]
    df.columns = ["fiscal_year", "report_date", "date", "fatality"]
    df = df[df["fatality"].notna()]
    df = finalize(df)
    df.to_csv(out, index=False)
    return len(df)


# ------------------------------------------------------------------
# Robust Postgres loader (GUARDED)
# ------------------------------------------------------------------

def load_fatalities_to_postgres():
    print("\n=== Loading fatalities_clean.csv into Postgres ===")

    csv_path = os.path.join(DATA_DIR, "fatalities_clean.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    expected_cols = ["date", "no_of_fatalities", "country"]
    if list(df.columns) != expected_cols:
        raise ValueError(f"Unexpected columns: {df.columns}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df[df["date"].notna()]

    if df.empty:
        raise RuntimeError("No valid rows to load into fatalities_clean")

    conn = pg_connect()
    cur = conn.cursor()

    table = DB_CONFIG["fatalities_clean_table"]

    try:
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.commit()

        cur.execute(
            f"""
            CREATE TABLE "{table}" (
                fatality_id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                no_of_fatalities INTEGER NOT NULL,
                country TEXT NOT NULL
            )
            """
        )
        conn.commit()

        records = list(df.itertuples(index=False, name=None))

        execute_batch(
            cur,
            f"""
            INSERT INTO "{table}"
            (date, no_of_fatalities, country)
            VALUES (%s, %s, %s)
            """,
            records,
            page_size=1000,
        )

        conn.commit()

        # -----------------------
        # Guardrail verification
        # -----------------------
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        count = cur.fetchone()[0]

        if count != len(records):
            raise RuntimeError(
                f"Row count mismatch: inserted={len(records)}, table={count}"
            )

        print(f"✔ {count} rows verified in {table}")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


# ------------------------------------------------------------------
# Master orchestration
# ------------------------------------------------------------------

def create_fatalities_clean():
    print("=== Cleaning fatalities files ===")

    cleaners = [
        ("fatalities_1.csv", clean_fatalities_1_2),
        ("fatalities_2.csv", clean_fatalities_1_2),
        ("fatalities_3.csv", clean_fatalities_3),
        ("fatalities_4.csv", clean_fatalities_4),
        ("fatalities_5.csv", clean_fatalities_5),
        ("fatalities_6.csv", clean_fatalities_6_to_9),
        ("fatalities_7.csv", clean_fatalities_6_to_9),
        ("fatalities_8.csv", clean_fatalities_6_to_9),
        ("fatalities_9.csv", clean_fatalities_6_to_9),
    ]

    dfs = []

    for fname, fn in cleaners:
        src = os.path.join(DATA_DIR, fname)
        dst = os.path.join(DATA_DIR, fname.replace(".csv", "_clean.csv"))

        if not os.path.exists(src):
            print(f"⚠ File not found, skipping: {src}")
            continue

        rows = fn(src, dst)
        print(f"✔ {fname} → {rows} rows cleaned")
        dfs.append(pd.read_csv(dst))

    if not dfs:
        raise RuntimeError("No fatalities files were cleaned")

    final_df = pd.concat(dfs, ignore_index=True)
    final_path = os.path.join(DATA_DIR, "fatalities_clean.csv")
    final_df.to_csv(final_path, index=False)

    print(f"✔ Final merge written: {final_path} ({len(final_df)} rows)")

    load_fatalities_to_postgres()

    print("✔ fatalities_clean loaded into Postgres")
    return None


# =====================================================================================
# CREATE ARIADB CLEAN
# =====================================================================================


def create_ariadb_clean():
    src_table = DB_CONFIG["ariadb_table"]
    dst_table = DB_CONFIG["ariadb_clean_table"]
    conn = pg_connect()
    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)
    col_map = {
        "numéro_aria": "aria_id",
        "titre": "title",
        "type_de_publication": "publication_type",
        "date": "incident_date",
        "code_naf": "industry_code",
        "pays": "country",
        "départment": "department",
        "commune": "municipality",
        "type_d'accident": "accident_type",
        "type_évènement": "event_type",
        "classe_de_danger_clp": "hazard_class",
    }

    existing = [c for c in col_map if c in df.columns]
    df = df[existing].rename(columns={k: col_map[k] for k in existing}).copy()

    if "incident_date" in df.columns:
        df["incident_date"] = pd.to_datetime(
            df["incident_date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    if "aria_id" in df.columns:
        df.drop_duplicates(subset=["aria_id"], inplace=True)

    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')

    col_defs = ", ".join([f'"{c}" TEXT' for c in df.columns])
    pk = ", PRIMARY KEY (aria_id)" if "aria_id" in df.columns else ""

    cursor.execute(f'CREATE TABLE "{dst_table}" ({col_defs}{pk});')

    insert_sql = f"""
        INSERT INTO "{dst_table}"
        ({", ".join([f'"{c}"' for c in df.columns])})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """

    for _, row in df.iterrows():
        cursor.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    conn.close()

    print(f"✔ Created ariadb_clean ({len(df)} rows)")


# =====================================================================================
# CREATE WORKACCIDENTS CLEAN
# =====================================================================================


def create_workaccidents_clean():
    src_table = DB_CONFIG["workaccidents_table"]
    dst_table = "workaccidents_clean"

    conn = pg_connect()

    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)

    if "final_narrative" in df.columns:
        df.drop(columns=["final_narrative"], inplace=True)

    date_col = "eventdate" if "eventdate" in df.columns else None
    if date_col:
        df["accident_date"] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime(
            "%Y-%m-%d"
        )
    else:
        df["accident_date"] = None

    df["country"] = "USA"

    selected_columns = [
        "id",
        "upa",
        "accident_date",
        "employer",
        "address1",
        "address2",
        "city",
        "state",
        "zip",
        "latitude",
        "longitude",
        "primary_naics",
        "hospitalized",
        "amputation",
        "loss_of_eye",
        "inspection",
        "nature",
        "naturetitle",
        "part_of_body",
        "part_of_body_title",
        "event",
        "eventtitle",
        "source",
        "sourcetitle",
        "secondary_source",
        "secondary_source_title",
        "federalstate",
        "country",
    ]

    df_clean = df[[c for c in selected_columns if c in df.columns]].copy()

    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS "workaccidents_clean"')

    col_defs = ", ".join([f'"{c}" TEXT' for c in df_clean.columns])
    cursor.execute(f'CREATE TABLE "{dst_table}" ({col_defs});')

    insert_sql = f"""
        INSERT INTO "{dst_table}"
        ({", ".join([f'"{c}"' for c in df_clean.columns])})
        VALUES ({", ".join(["%s"] * len(df_clean.columns))})
    """

    for _, row in df_clean.iterrows():
        cursor.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    conn.close()

    print(f"✔ Created workaccidents_clean ({len(df_clean)} rows)")


# =====================================================================================
# DATABASE PROFILING
# =====================================================================================


def profile_db(db_config=DB_CONFIG, output_dir=DATA_DIR):
    os.makedirs(output_dir, exist_ok=True)

    tables = [
        db_config["ariadb_clean_table"],
        db_config["fatalities_clean_table"],
        "workaccidents_clean",
    ]

    conn = pg_connect()

    profile_records = []

    for table in tables:
        df = pd.read_sql(f'SELECT * FROM "{table}"', conn)

        for col in df.columns:
            profile_records.append(
                {
                    "table": table,
                    "column": col,
                    "dtype": str(df[col].dtype),
                    "non_null": df[col].notna().sum(),
                    "missing": df[col].isna().sum(),
                    "unique": df[col].nunique(dropna=True),
                    "top_5": df[col].value_counts(dropna=False).head(5).to_dict(),
                }
            )

    df_prof = pd.DataFrame(profile_records)
    profile_path = os.path.join(output_dir, "db_profiling_report.csv")
    df_prof.to_csv(profile_path, index=False)

    print(f"✔ Profile saved to {profile_path}")
    conn.close()


# =====================================================================================
# STAR SCHEMA CREATION — EXACT ORIGINAL LOGIC
# =====================================================================================

def create_dimensions_and_fact():
 
    conn = pg_connect()
    cur = conn.cursor()

    # ==================================================
    # HELPERS
    # ==================================================
    def normalize_text(obj):
        """Normalize text safely for Series or DataFrame."""
        if isinstance(obj, pd.Series):
            return obj.astype(str).str.strip().str.upper()
        elif isinstance(obj, pd.DataFrame):
            return obj.apply(lambda c: c.astype(str).str.strip().str.upper())
        return obj

    def drop_empty_rows(df, key_cols):
        """Drop rows where all key columns are null (only if they exist)."""
        valid = [c for c in key_cols if c in df.columns]
        if not valid:
            return df
        return df.dropna(how="all", subset=valid)

    # ==================================================
    # DROP TABLES (DEPENDENCY ORDER)
    # ==================================================
    tables_to_drop = [
        "fact_accidents",
        "dim_location",
        "dim_industry",
        "dim_accident_type",
        "dim_hazard",
        "dim_employer",
        "dim_date",
        "country_synonym",
        "dim_country",
        "original_dim_location",
    ]
    for t in tables_to_drop:
        cur.execute(f"DROP TABLE IF EXISTS {t}")
    conn.commit()

    # ==================================================
    # LOAD CLEAN TABLES
    # ==================================================
    df_aria = pd.read_sql(f'SELECT * FROM "{DB_CONFIG["ariadb_clean_table"]}"', conn)
    df_fatal = pd.read_sql(f'SELECT * FROM "{DB_CONFIG["fatalities_clean_table"]}"', conn)
    df_work = pd.read_sql("SELECT * FROM workaccidents_clean", conn)

    df_aria = drop_empty_rows(df_aria, ["municipality", "department", "country", "incident_date"])
    df_fatal = drop_empty_rows(df_fatal, ["city", "state", "country", "date_of_incident"])
    df_work = drop_empty_rows(df_work, ["city", "state", "country", "accident_date"])

    # ==================================================
    # ORIGINAL DIM LOCATION
    # ==================================================
    frames = []
    if all(c in df_aria.columns for c in ["municipality", "department", "country"]):
        frames.append(df_aria[["municipality", "department", "country"]])
    if all(c in df_fatal.columns for c in ["city", "state", "country"]):
        frames.append(df_fatal[["city", "state", "country"]].rename(columns={"city": "municipality", "state": "department"}))
    if all(c in df_work.columns for c in ["city", "state", "country"]):
        frames.append(df_work[["city", "state", "country"]].rename(columns={"city": "municipality", "state": "department"}))
    df_orig_loc = pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame(columns=["municipality", "department", "country"])
    for col in ["municipality", "department", "country"]:
        df_orig_loc[col] = df_orig_loc[col].fillna("UNKNOWN")

    cur.execute("""
        CREATE TABLE original_dim_location (
            municipality TEXT,
            department TEXT,
            country TEXT
        );
    """)
    for _, r in df_orig_loc.iterrows():
        cur.execute("INSERT INTO original_dim_location VALUES (%s,%s,%s)", [r.municipality, r.department, r.country])
    conn.commit()

    # ==================================================
    # DIM COUNTRY + SYNONYMS
    # ==================================================
    cur.execute("""
        CREATE TABLE dim_country (
            country_id INT PRIMARY KEY,
            country_name TEXT UNIQUE,
            country_code TEXT
        );
    """)
    cur.execute("""
        INSERT INTO dim_country VALUES
        (1,'United States','US'),(2,'United Kingdom','GB'),(3,'France','FR'),
        (4,'Canada','CA'),(5,'Germany','DE'),(6,'Italy','IT'),
        (7,'Spain','ES'),(8,'Switzerland','CH'),(9,'Netherlands','NL'),
        (10,'Belgium','BE'),(11,'China','CN'),(12,'Russia','RU'),
        (13,'Japan','JP'),(14,'India','IN'),(15,'Brazil','BR'),
        (16,'Australia','AU'),(17,'South Africa','ZA'),(18,'Mexico','MX'),
        (19,'UNKNOWN','XX');
    """)
    conn.commit()

    cur.execute("""
        CREATE TABLE country_synonym (
            synonym TEXT PRIMARY KEY,
            country_id INT REFERENCES dim_country(country_id)
        );
    """)
    cur.execute("""
        INSERT INTO country_synonym VALUES
        ('USA',1),('US',1),('UNITED STATES',1),('ETATS-UNIS',1),
        ('UK',2),('UNITED KINGDOM',2),('ROYAUME-UNI',2),
        ('FRANCE',3),('CANADA',4),
        ('GERMANY',5),('ALLEMAGNE',5),
        ('ITALY',6),('ITALIE',6),
        ('SPAIN',7),('ESPAGNE',7),
        ('SWITZERLAND',8),('SUISSE',8),
        ('NETHERLANDS',9),('PAYS-BAS',9),
        ('BELGIUM',10),('BELGIQUE',10),
        ('CHINA',11),('CHINE',11),
        ('RUSSIA',12),('RUSSIE',12);
    """)
    conn.commit()

    # ==================================================
    # DIM LOCATION
    # ==================================================
    cur.execute("""
        CREATE TABLE dim_location (
            location_id INT PRIMARY KEY,
            municipality TEXT,
            department TEXT,
            country TEXT,
            country_id INT REFERENCES dim_country(country_id)
        );
    """)
    cur.execute("""
        INSERT INTO dim_location
        SELECT
            ROW_NUMBER() OVER (ORDER BY m,d,c_id) AS location_id,
            m,d,c,c_id
        FROM (
            SELECT DISTINCT
                UPPER(TRIM(o.municipality)) AS m,
                UPPER(TRIM(o.department)) AS d,
                UPPER(TRIM(o.country)) AS c,
                COALESCE(cs.country_id,19) AS c_id
            FROM original_dim_location o
            LEFT JOIN country_synonym cs
                ON UPPER(TRIM(o.country)) = cs.synonym
        ) t;
    """)
    conn.commit()
    df_dim_location = pd.read_sql("SELECT * FROM dim_location", conn)

    # ==================================================
    # DIM DATE
    # ==================================================
    all_dates = pd.concat([
        pd.to_datetime(df_aria.get("incident_date"), errors="coerce"),
        pd.to_datetime(df_fatal.get("date_of_incident"), errors="coerce"),
        pd.to_datetime(df_work.get("accident_date"), errors="coerce")
    ]).dropna().drop_duplicates().sort_values()

    df_dates = pd.DataFrame({"date": all_dates})
    df_dates["year"] = df_dates["date"].dt.year
    df_dates["month"] = df_dates["date"].dt.month
    df_dates["day"] = df_dates["date"].dt.day
    df_dates["quarter"] = df_dates["date"].dt.quarter
    df_dates["date_id"] = range(1, len(df_dates)+1)

    cur.execute("""
        CREATE TABLE dim_date (
            date_id INT PRIMARY KEY,
            date DATE,
            year INT,
            month INT,
            day INT,
            quarter INT
        );
    """)
    for _, r in df_dates.fillna(0).iterrows():
        cur.execute("INSERT INTO dim_date VALUES (%s,%s,%s,%s,%s,%s)", [r.date_id,r.date,r.year,r.month,r.day,r.quarter])
    conn.commit()

    # ==================================================
    # DIM EMPLOYER
    # ==================================================
    emp_frames = [df[["employer"]] for df in [df_aria, df_fatal, df_work] if "employer" in df.columns]
    df_emp = pd.concat(emp_frames, ignore_index=True).dropna().drop_duplicates() if emp_frames else pd.DataFrame(columns=["employer"])
    if not df_emp.empty:
        df_emp["employer"] = normalize_text(df_emp["employer"])
        df_emp.insert(0, "employer_id", range(1, len(df_emp)+1))
    cur.execute("""
        CREATE TABLE dim_employer (
            employer_id INT PRIMARY KEY,
            employer TEXT
        );
    """)
    cur.execute("INSERT INTO dim_employer VALUES (0,'UNKNOWN')")
    for _, r in df_emp.fillna("UNKNOWN").iterrows():
        cur.execute("INSERT INTO dim_employer VALUES (%s,%s)", [r.employer_id,r.employer])
    conn.commit()

    # ==================================================
    # DIM HAZARD
    # ==================================================
    hazard_frames = []
    if "hazard_class" in df_aria.columns:
        hazard_frames.append(df_aria[["hazard_class"]].rename(columns={"hazard_class":"hazard"}))
    if "hazard_description" in df_fatal.columns:
        hazard_frames.append(df_fatal[["hazard_description"]].rename(columns={"hazard_description":"hazard"}))
    if "nature" in df_work.columns:
        hazard_frames.append(df_work[["nature"]].rename(columns={"nature":"hazard"}))
    df_haz = pd.concat(hazard_frames, ignore_index=True).dropna().drop_duplicates() if hazard_frames else pd.DataFrame(columns=["hazard"])
    if not df_haz.empty:
        df_haz["hazard"] = normalize_text(df_haz["hazard"])
        df_haz.insert(0, "hazard_id", range(1, len(df_haz)+1))
    cur.execute("""
        CREATE TABLE dim_hazard (
            hazard_id INT PRIMARY KEY,
            hazard TEXT
        );
    """)
    cur.execute("INSERT INTO dim_hazard VALUES (0,'UNKNOWN')")
    for _, r in df_haz.fillna("UNKNOWN").iterrows():
        cur.execute("INSERT INTO dim_hazard VALUES (%s,%s)", [r.hazard_id,r.hazard])
    conn.commit()

    # ==================================================
    # DIM ACCIDENT TYPE
    # ==================================================
    acc_frames = []
    if "accident_type" in df_fatal.columns:
        acc_frames.append(df_fatal[["accident_type"]])
    if "naturetitle" in df_work.columns:
        acc_frames.append(df_work[["naturetitle"]].rename(columns={"naturetitle":"accident_type"}))
    if "accident_type" in df_aria.columns:
        acc_frames.append(df_aria[["accident_type"]])
    df_act = pd.concat(acc_frames, ignore_index=True).dropna().drop_duplicates() if acc_frames else pd.DataFrame(columns=["accident_type"])
    if not df_act.empty:
        df_act["accident_type"] = normalize_text(df_act["accident_type"])
        df_act.insert(0,"accident_type_id", range(1,len(df_act)+1))
    cur.execute("""
        CREATE TABLE dim_accident_type (
            accident_type_id INT PRIMARY KEY,
            accident_type TEXT
        );
    """)
    cur.execute("INSERT INTO dim_accident_type VALUES (0,'UNKNOWN')")
    for _, r in df_act.fillna("UNKNOWN").iterrows():
        cur.execute("INSERT INTO dim_accident_type VALUES (%s,%s)", [r.accident_type_id,r.accident_type])
    conn.commit()

    # ==================================================
    # DIM INDUSTRY
    # ==================================================
    df_ind = df_aria[["industry_code"]].dropna().drop_duplicates() if "industry_code" in df_aria.columns else pd.DataFrame(columns=["industry_code"])
    if not df_ind.empty:
        df_ind["industry_code"] = normalize_text(df_ind["industry_code"])
        df_ind.insert(0,"industry_id", range(1,len(df_ind)+1))
    cur.execute("""
        CREATE TABLE dim_industry (
            industry_id INT PRIMARY KEY,
            industry_code TEXT
        );
    """)
    cur.execute("INSERT INTO dim_industry VALUES (0,'UNKNOWN')")
    for _, r in df_ind.fillna("UNKNOWN").iterrows():
        cur.execute("INSERT INTO dim_industry VALUES (%s,%s)", [r.industry_id,r.industry_code])
    conn.commit()

    # ==================================================
    # FACT TABLE
    # ==================================================
    cur.execute("""
        CREATE TABLE fact_accidents (
            date_id INT,
            employer_id INT,
            location_id INT,
            hazard_id INT,
            accident_type_id INT,
            industry_id INT
        );
    """)

    # ==================================================
    # BUILD FACT FUNCTION (COUNTRY ONLY)
    # ==================================================
    def build_fact(df, date_col, employer_col, hazard_col, acc_type_col, industry_col=None):
        df2 = df.copy()
        
        # Ensure country column exists
        if "country" not in df2.columns:
            df2["country"] = "UNKNOWN"
        df2["country"] = df2["country"].fillna("UNKNOWN").astype(str).str.upper()
        
        # Date dimension
        if date_col in df2.columns:
            df2[date_col] = pd.to_datetime(df2[date_col], errors="coerce")
            df2 = df2.merge(df_dates[["date","date_id"]], left_on=date_col, right_on="date", how="left")
            df2.drop(columns=["date"], inplace=True, errors="ignore")
        else:
            df2["date_id"] = 0
        
        # Location: merge on country only
        df2 = df2.merge(
            df_dim_location[["country","location_id"]],
            left_on="country",
            right_on="country",
            how="left"
        )
    
        # Employer
        if employer_col in df2.columns:
            df2[employer_col] = df2[employer_col].astype(str).str.strip().str.upper()
            df2 = df2.merge(df_emp[["employer","employer_id"]], left_on=employer_col, right_on="employer", how="left")
        
        # Hazard
        if hazard_col in df2.columns:
            df2[hazard_col] = df2[hazard_col].astype(str).str.strip().str.upper()
            df2 = df2.merge(df_haz[["hazard","hazard_id"]], left_on=hazard_col, right_on="hazard", how="left")
        
        # Accident type
        if acc_type_col and acc_type_col in df2.columns:
            df2[acc_type_col] = df2[acc_type_col].astype(str).str.strip().str.upper()
            df2 = df2.merge(df_act[["accident_type","accident_type_id"]], left_on=acc_type_col, right_on="accident_type", how="left")
        
        # Industry
        if industry_col and industry_col in df2.columns:
            df2[industry_col] = df2[industry_col].astype(str).str.strip().str.upper()
            df2 = df2.merge(df_ind[["industry_code","industry_id"]], left_on=industry_col, right_on="industry_code", how="left")
        
        # Fill missing
        for col in ["date_id","location_id","employer_id","hazard_id","accident_type_id","industry_id"]:
            if col not in df2.columns:
                df2[col] = 0
            df2[col] = df2[col].fillna(0).astype(int)
        
        return df2[["date_id","employer_id","location_id","hazard_id","accident_type_id","industry_id"]]
    
    # ==================================================
    # CREATE FACT TABLE
    # ==================================================
    cur.execute("""
        CREATE TABLE fact_accidents (
            date_id INT,
            employer_id INT,
            location_id INT,  -- location now represents country only
            hazard_id INT,
            accident_type_id INT,
            industry_id INT
        );
    """)
    conn.commit()
    
    # ==================================================
    # BUILD FACT FROM ALL SOURCES
    # ==================================================
    df_fact = pd.concat([
        build_fact(df_aria, "incident_date", "employer", "hazard_class", None, "industry_code"),
        build_fact(df_fatal, "date_of_incident", "employer", "hazard_description", "accident_type"),
        build_fact(df_work, "accident_date", "employer", "nature", "naturetitle"),
    ], ignore_index=True)
    
    # Remove rows with no date or country
    df_fact = df_fact[(df_fact["date_id"] != 0) & (df_fact["location_id"] != 0)]
    
    for _, r in df_fact.iterrows():
        cur.execute("INSERT INTO fact_accidents VALUES (%s,%s,%s,%s,%s,%s)", list(r))
    
    conn.commit()
    conn.close()
    
    print("✔ Star schema created successfully (all sources and dimensions included)")


# =====================================================================================
# STAR SCHEMA TESTS
# =====================================================================================

# ================== TESTS FOR STAR SCHEMA (COUNTRY-ONLY) ==================

def min_test_star_schema():
    """
    Minimal test: select top 5 rows from fact_accidents.
    """
    conn = pg_connect()

    query = """
        SELECT 
            f.date_id,
            f.employer_id,
            f.location_id,
            f.hazard_id,
            f.accident_type_id,
            f.industry_id
        FROM fact_accidents f
        LIMIT 5;
    """

    df_test = pd.read_sql(query, conn)
    conn.close()

    print("✔ min_test_star_schema result:")
    print(df_test)
    return df_test


def full_test_star_schema():
    """
    Full test: join fact_accidents with dimensions, country-only version.
    """
    conn = pg_connect()

    sql = """
        SELECT 
            f.date_id, d.date, d.year, d.month,
            f.employer_id, e.employer,
            f.location_id, l.country AS country_name,
            f.hazard_id, h.hazard,
            f.accident_type_id, a.accident_type,
            f.industry_id, i.industry_code
        FROM fact_accidents f
        LEFT JOIN dim_date d ON f.date_id = d.date_id
        LEFT JOIN dim_employer e ON f.employer_id = e.employer_id
        LEFT JOIN dim_location l ON f.location_id = l.location_id
        LEFT JOIN dim_hazard h ON f.hazard_id = h.hazard_id
        LEFT JOIN dim_accident_type a ON f.accident_type_id = a.accident_type_id
        LEFT JOIN dim_industry i ON f.industry_id = i.industry_id
        LIMIT 20;
    """

    df = pd.read_sql(sql, conn)
    conn.close()

    print("\n=== Full Star Schema Test (20 rows) ===")
    print(df)
    return df
