# ==================== etl_utils.py =================================
# The utilities contained therein are used by dag_etl_master.py
# This module provides utility functions for ETL processes
# specifically for downloading, cleaning, and loading datasets

# etl_utils.py

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

CSV_URL = "https://www.data.gouv.fr/api/1/datasets/r/e4a3ad9a-cc9d-40c6-8d1a-aebdf75ded7b"


ZIP_URL = "https://www.osha.gov/sites/default/files/January2015toMarch2025.zip"

# Docker Postgres connection
DB_CONFIG = {
    "host": "postgres",     # Important: Docker hostname
    "port": 5432,
    "database": "airflow",
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",

    "ariadb_table": "ariadb",
    "ariadb_clean_table": "ariadb_clean",
    "workaccidents_table": "workaccidents",
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

    headers = {
        "User-Agent": "Mozilla/5.0 (ETLBot/1.0)",
        "Accept": "text/csv,*/*;q=0.8"
    }

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
                raise FileNotFoundError(
                    f"❌ No local fallback available for {filename}"
                )
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
        c.strip().lower()
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


# =====================================================================================
# create_fatalities_clean.py
# =====================================================================================


def create_fatalities_clean(return_df=False):
    """
    Reads all fatalities CSVs from DATA_DIR matching 'fatalities_*.csv',
    performs basic cleaning, and loads the resulting table into Postgres.
    
    Parameters:
    - return_df (bool): If True, returns the cleaned DataFrame.
    
    Raises:
    - ValueError: If no matching CSV files are found.
    """
    
    # ==================== Find all CSV files ====================
    pattern = os.path.join(DATA_DIR, "fatalities_*.csv")
    files = sorted(glob.glob(pattern))
    
    if not files:
        raise ValueError(f"No fatalities CSV files found in {DATA_DIR} with pattern 'fatalities_*.csv'!")
    
    print(f"Found {len(files)} files: {files}")
    
    # ==================== Read and concatenate ====================
    df_list = []
    for f in files:
        try:
            df = pd.read_csv(f, encoding="utf-8")
            df_list.append(df)
            print(f"Read file: {f} ({len(df)} rows)")
        except Exception as e:
            print(f"Warning: Could not read {f}: {e}")
    
    if not df_list:
        raise ValueError("No fatalities CSV files could be read successfully!")
    
    df_clean = pd.concat(df_list, ignore_index=True)
    
    # ==================== Basic cleaning ====================
    # Example: strip strings and standardize column names
    df_clean.columns = [c.strip().lower() for c in df_clean.columns]
    for col in df_clean.select_dtypes(include="object").columns:
        df_clean[col] = df_clean[col].astype(str).str.strip()
    
    # ==================== Load into PostgreSQL ====================
    conn = pg_connect()
    cur = conn.cursor()
    
    # Drop old clean table if exists
    cur.execute(f"DROP TABLE IF EXISTS {DB_CONFIG['fatalities_clean_table']}")
    conn.commit()
    
    # Create table with inferred schema from pandas
    # We'll use simple TEXT columns for all
    cols_defs = ", ".join([f"{c} TEXT" for c in df_clean.columns])
    create_sql = f"CREATE TABLE {DB_CONFIG['fatalities_clean_table']} ({cols_defs});"
    cur.execute(create_sql)
    conn.commit()
    
    # Insert rows
    for _, row in df_clean.iterrows():
        placeholders = ",".join(["%s"] * len(df_clean.columns))
        insert_sql = f"INSERT INTO {DB_CONFIG['fatalities_clean_table']} VALUES ({placeholders})"
        cur.execute(insert_sql, list(row))
    
    conn.commit()
    conn.close()
    
    print(f"✔ Successfully loaded {len(df_clean)} rows into '{DB_CONFIG['fatalities_clean_table']}'")
    
    if return_df:
        return df_clean



def load_fatalities_first(csv_text, sep=","):
    """Drop & recreate fatalities table before loading fatalities_1."""
    return load_to_postgres(
        csv_content=csv_text,
        table_name=DB_CONFIG["fatalities_table"],
        sep=sep,
        skiprows=0,  # fatalities have no metadata rows
        drop_if_exists=True  # FORCE DROP
    )


def append_fatalities_next(csv_text, sep=","):
    """Append fatalities_2, fatalities_3, etc."""
    return append_to_fatalities(
        csv_text,
        sep=sep
    )

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
# CREATE FATALITIES CLEAN
# =====================================================================================

# ==========================================
# etl_utils.py (excerpt) – fatalities
# ==========================================

import glob


def create_fatalities_clean(return_df=False):
    """
    1. Read raw fatalities_*.csv files (excluding fatalities_clean.csv)
    2. Merge into one DataFrame
    3. Impute missing date/location per source_file (proven logic)
    4. Filter to valid fatality rows
    5. Create landing-zone file: fatalities_clean.csv
    6. Load fatalities_clean.csv into Postgres

    This function is the SINGLE source of truth for fatalities_clean.
    """

    # ============================================================
    # 1. Find raw source files ONLY
    # ============================================================
    pattern = os.path.join(DATA_DIR, "fatalities_[0-9]*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        raise ValueError(
            f"No raw fatalities CSV files found in {DATA_DIR} "
            f"(pattern: fatalities_[0-9]*.csv)"
        )

    print(f"✔ Found {len(files)} raw files")
    for f in files:
        print(f"  - {f}")

    # ============================================================
    # 2. Read & merge
    # ============================================================
    df_list = []
    for f in files:
        df = pd.read_csv(f, encoding="latin1")
        df["source_file"] = os.path.basename(f)
        df_list.append(df)
        print(f"Read {f}: {len(df)} rows")

    df_all = pd.concat(df_list, ignore_index=True)
    print(f"✔ Total merged rows: {len(df_all)}")

    # Normalize column names
    df_all.columns = [c.strip().lower() for c in df_all.columns]

    # ============================================================
    # 3. Impute missing date/location (WORKING LOGIC)
    # ============================================================
    def fill_missing(group):
        if "date" in group.columns:
            mode_date = group["date"].mode()
            if not mode_date.empty:
                group["date"] = group["date"].fillna(mode_date[0])

        if "location" in group.columns:
            mode_loc = group["location"].mode()
            if not mode_loc.empty:
                group["location"] = group["location"].fillna(mode_loc[0])

        return group

    df_all = df_all.groupby("source_file", group_keys=False).apply(fill_missing)

    # ============================================================
    # 4. Filter valid fatality records
    # ============================================================
    if "incident" not in df_all.columns:
        raise ValueError("Missing required column: incident")

    df_clean = df_all.dropna(subset=["incident"])
    df_clean = df_clean[
        df_clean["date"].notna() | df_clean["location"].notna()
    ].reset_index(drop=True)

    print(f"✔ Rows after incident/date/location filter: {len(df_clean)}")

    # ============================================================
    # 5. Final schema selection
    # ============================================================
    # Parse date safely
    df_clean["date"] = pd.to_datetime(
        df_clean["date"], errors="coerce", dayfirst=False
    )

    # Compute fatalities (binary, simple & stable)
    df_clean["no_of_fatalities"] = (
        df_clean["incident"]
        .astype(str)
        .str.lower()
        .str.contains("fatality")
        .astype(int)
    )

    # Keep state if present
    if "state" not in df_clean.columns:
        df_clean["state"] = None

    df_clean_final = df_clean[
        ["date", "no_of_fatalities", "state"]
    ].copy()

    df_clean_final["country"] = "USA"

    # Drop rows that still violate DB constraints
    df_clean_final = df_clean_final.dropna(subset=["date"])

    print(f"✔ Rows ready for load: {len(df_clean_final)}")

    # ============================================================
    # 6. Write landing-zone CSV (REQUIRED)
    # ============================================================
    output_path = os.path.join(DATA_DIR, "fatalities_clean.csv")
    df_clean_final.to_csv(output_path, index=False)

    file_size = os.path.getsize(output_path)
    print(
        f"✔ fatalities_clean.csv written: {output_path} "
        f"({len(df_clean_final)} rows, {file_size} bytes)"
    )

    print("✔ Preview:")
    print(df_clean_final.head(5))

    # ============================================================
    # 7. Load into PostgreSQL FROM CSV
    # ============================================================
    conn = pg_connect()
    cur = conn.cursor()

    table = DB_CONFIG["fatalities_clean_table"]

    cur.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()

    cur.execute(
        f"""
        CREATE TABLE {table} (
            fatality_id SERIAL PRIMARY KEY,
            date_of_incident DATE NOT NULL,
            no_of_fatalities INTEGER NOT NULL,
            state TEXT,
            country TEXT NOT NULL
        )
        """
    )
    conn.commit()

    insert_sql = f"""
        INSERT INTO {table}
        (date_of_incident, no_of_fatalities, state, country)
        VALUES (%s, %s, %s, %s)
    """

    load_df = pd.read_csv(output_path, parse_dates=["date"])

    inserted = 0
    for _, row in load_df.iterrows():
        cur.execute(
            insert_sql,
            (
                row["date"].date(),
                int(row["no_of_fatalities"]),
                row["state"],
                row["country"],
            ),
        )
        inserted += 1

    conn.commit()
    conn.close()

    print(f"✔ Loaded {inserted} rows into Postgres table '{table}'")

    if return_df:
        return df_clean_final


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
        df["accident_date"] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    else:
        df["accident_date"] = None

    df["country"] = "USA"

    selected_columns = [
        "id", "upa", "accident_date", "employer", "address1", "address2",
        "city", "state", "zip", "latitude", "longitude", "primary_naics",
        "hospitalized", "amputation", "loss_of_eye", "inspection",
        "nature", "naturetitle", "part_of_body", "part_of_body_title",
        "event", "eventtitle", "source", "sourcetitle",
        "secondary_source", "secondary_source_title",
        "federalstate", "country",
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
            profile_records.append({
                "table": table,
                "column": col,
                "dtype": str(df[col].dtype),
                "non_null": df[col].notna().sum(),
                "missing": df[col].isna().sum(),
                "unique": df[col].nunique(dropna=True),
                "top_5": df[col].value_counts(dropna=False).head(5).to_dict(),
            })

    df_prof = pd.DataFrame(profile_records)
    profile_path = os.path.join(output_dir, "db_profiling_report.csv")
    df_prof.to_csv(profile_path, index=False)

    print(f"✔ Profile saved to {profile_path}")
    conn.close()


# =====================================================================================
# STAR SCHEMA CREATION — EXACT ORIGINAL LOGIC
# =====================================================================================

def create_dimensions_and_fact():
    import pandas as pd

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
        "country_synonym",
        "dim_country",
        "original_dim_location",
        "dim_date",
        "dim_employer",
        "dim_hazard",
        "dim_accident_type",
    ]
    for t in tables_to_drop:
        cur.execute(f"DROP TABLE IF EXISTS {t}")
    conn.commit()

    # ==================================================
    # LOAD CLEAN TABLES (AS-IS, NO HEAVY CLEANING)
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
        frames.append(
            df_fatal[["city", "state", "country"]]
            .rename(columns={"city": "municipality", "state": "department"})
        )

    if all(c in df_work.columns for c in ["city", "state", "country"]):
        frames.append(
            df_work[["city", "state", "country"]]
            .rename(columns={"city": "municipality", "state": "department"})
        )

    if frames:
        df_orig_loc = pd.concat(frames, ignore_index=True).drop_duplicates()
    else:
        df_orig_loc = pd.DataFrame(columns=["municipality", "department", "country"])

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
        cur.execute(
            "INSERT INTO original_dim_location VALUES (%s,%s,%s)",
            [r.municipality, r.department, r.country],
        )
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
            ROW_NUMBER() OVER (ORDER BY m, d, c_id) AS location_id,
            m, d, c, c_id
        FROM (
            SELECT DISTINCT
                UPPER(TRIM(o.municipality)) AS m,
                UPPER(TRIM(o.department)) AS d,
                UPPER(TRIM(o.country)) AS c,
                COALESCE(cs.country_id, 19) AS c_id
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
        pd.to_datetime(df_work.get("accident_date"), errors="coerce"),
    ]).dropna().drop_duplicates().sort_values()

    df_dates = pd.DataFrame({"date": all_dates})
    df_dates["year"] = df_dates["date"].dt.year
    df_dates["month"] = df_dates["date"].dt.month
    df_dates["day"] = df_dates["date"].dt.day
    df_dates["quarter"] = df_dates["date"].dt.quarter
    df_dates["date_id"] = range(1, len(df_dates) + 1)

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
        cur.execute(
            "INSERT INTO dim_date VALUES (%s,%s,%s,%s,%s,%s)",
            [r.date_id, r.date, r.year, r.month, r.day, r.quarter],
        )
    conn.commit()

    # ==================================================
    # DIM EMPLOYER (SAFE)
    # ==================================================
    emp_frames = []
    for df in [df_aria, df_fatal, df_work]:
        if "employer" in df.columns:
            emp_frames.append(df[["employer"]])

    if emp_frames:
        df_emp = pd.concat(emp_frames, ignore_index=True).dropna().drop_duplicates()
        df_emp["employer"] = normalize_text(df_emp["employer"])
        df_emp.insert(0, "employer_id", range(1, len(df_emp) + 1))
    else:
        df_emp = pd.DataFrame(columns=["employer_id", "employer"])

    cur.execute("""
        CREATE TABLE dim_employer (
            employer_id INT PRIMARY KEY,
            employer TEXT
        );
    """)
    cur.execute("INSERT INTO dim_employer VALUES (0,'UNKNOWN')")
    for _, r in df_emp.fillna("UNKNOWN").iterrows():
        cur.execute("INSERT INTO dim_employer VALUES (%s,%s)", [r.employer_id, r.employer])
    conn.commit()

    # ==================================================
    # DIM HAZARD
    # ==================================================
    hazard_frames = []
    if "hazard_class" in df_aria.columns:
        hazard_frames.append(df_aria[["hazard_class"]].rename(columns={"hazard_class": "hazard"}))
    if "hazard_description" in df_fatal.columns:
        hazard_frames.append(df_fatal[["hazard_description"]].rename(columns={"hazard_description": "hazard"}))
    if "nature" in df_work.columns:
        hazard_frames.append(df_work[["nature"]].rename(columns={"nature": "hazard"}))

    if hazard_frames:
        df_haz = pd.concat(hazard_frames, ignore_index=True).dropna().drop_duplicates()
        df_haz["hazard"] = normalize_text(df_haz["hazard"])
        df_haz.insert(0, "hazard_id", range(1, len(df_haz) + 1))
    else:
        df_haz = pd.DataFrame(columns=["hazard_id", "hazard"])

    cur.execute("""
        CREATE TABLE dim_hazard (
            hazard_id INT PRIMARY KEY,
            hazard TEXT
        );
    """)
    cur.execute("INSERT INTO dim_hazard VALUES (0,'UNKNOWN')")
    for _, r in df_haz.fillna("UNKNOWN").iterrows():
        cur.execute("INSERT INTO dim_hazard VALUES (%s,%s)", [r.hazard_id, r.hazard])
    conn.commit()

    # ==================================================
    # DIM ACCIDENT TYPE
    # ==================================================
    acc_frames = []
    if "accident_type" in df_fatal.columns:
        acc_frames.append(df_fatal[["accident_type"]])
    if "naturetitle" in df_work.columns:
        acc_frames.append(df_work[["naturetitle"]].rename(columns={"naturetitle": "accident_type"}))

    if acc_frames:
        df_act = pd.concat(acc_frames, ignore_index=True).dropna().drop_duplicates()
        df_act["accident_type"] = normalize_text(df_act["accident_type"])
        df_act.insert(0, "accident_type_id", range(1, len(df_act) + 1))
    else:
        df_act = pd.DataFrame(columns=["accident_type_id", "accident_type"])

    cur.execute("""
        CREATE TABLE dim_accident_type (
            accident_type_id INT PRIMARY KEY,
            accident_type TEXT
        );
    """)
    cur.execute("INSERT INTO dim_accident_type VALUES (0,'UNKNOWN')")
    for _, r in df_act.fillna("UNKNOWN").iterrows():
        cur.execute("INSERT INTO dim_accident_type VALUES (%s,%s)", [r.accident_type_id, r.accident_type])
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
            accident_type_id INT
        );
    """)

    # ==================================================
    # BUILD FACT (BULLETPROOF)
    # ==================================================
    def build_fact(df, date_col, employer_col, hazard_col, acc_type_col):
        df2 = df.copy()

        for col in ["municipality", "department", "country"]:
            if col not in df2.columns:
                df2[col] = "UNKNOWN"

        df2[["municipality", "department", "country"]] = normalize_text(
            df2[["municipality", "department", "country"]].fillna("UNKNOWN")
        )

        if date_col in df2.columns:
            df2[date_col] = pd.to_datetime(df2[date_col], errors="coerce")
            df2 = df2.merge(
                df_dates[["date", "date_id"]],
                left_on=date_col,
                right_on="date",
                how="left",
            )
            df2.drop(columns=["date"], inplace=True, errors="ignore")
        else:
            df2["date_id"] = 0

        df2 = df2.merge(
            df_dim_location[["municipality", "department", "country", "location_id"]],
            on=["municipality", "department", "country"],
            how="left",
        )

        if employer_col in df2.columns:
            df2[employer_col] = normalize_text(df2[employer_col])
            df2 = df2.merge(
                df_emp[["employer", "employer_id"]],
                left_on=employer_col,
                right_on="employer",
                how="left",
            )

        if hazard_col in df2.columns:
            df2[hazard_col] = normalize_text(df2[hazard_col])
            df2 = df2.merge(
                df_haz[["hazard", "hazard_id"]],
                left_on=hazard_col,
                right_on="hazard",
                how="left",
            )

        if acc_type_col and acc_type_col in df2.columns:
            df2[acc_type_col] = normalize_text(df2[acc_type_col])
            df2 = df2.merge(
                df_act[["accident_type", "accident_type_id"]],
                left_on=acc_type_col,
                right_on="accident_type",
                how="left",
            )

        for col in ["date_id", "location_id", "employer_id", "hazard_id", "accident_type_id"]:
            if col not in df2.columns:
                df2[col] = 0
            df2[col] = df2[col].fillna(0).astype(int)

        return df2[["date_id", "employer_id", "location_id", "hazard_id", "accident_type_id"]]

    df_fact = pd.concat([
        build_fact(df_aria, "incident_date", "employer", "hazard_class", None),
        build_fact(df_fatal, "date_of_incident", "employer", "hazard_description", "accident_type"),
        build_fact(df_work, "accident_date", "employer", "nature", "naturetitle"),
    ], ignore_index=True)

    df_fact = df_fact[(df_fact["date_id"] != 0) | (df_fact["location_id"] != 0)]

    for _, r in df_fact.iterrows():
        cur.execute(
            "INSERT INTO fact_accidents VALUES (%s,%s,%s,%s,%s)",
            list(r),
        )

    conn.commit()
    conn.close()

    print("✔ Star schema created successfully (pandas-safe, deterministic, null-tolerant)")


# =====================================================================================
# STAR SCHEMA TESTS
# =====================================================================================

def min_test_star_schema():
    conn = pg_connect()


    query = """
        SELECT d.date_id, e.employer_id, l.location_id
        FROM dim_date d
        JOIN dim_employer e ON e.employer_id IS NOT NULL
        JOIN dim_location l ON l.location_id IS NOT NULL
        LIMIT 5;
    """

    df_test = pd.read_sql(query, conn)
    conn.close()

    print("✔ min_test_star_schema result:")
    print(df_test)


def full_test_star_schema():
    conn = pg_connect()

    sql = """
        SELECT 
            f.date_id, d.date, d.year, d.month,
            f.employer_id, e.employer,
            f.location_id, l.municipality, l.department, l.country,
            f.hazard_id, h.hazard,
            f.accident_type_id, a.accident_type
        FROM fact_accidents f
        LEFT JOIN dim_date d ON f.date_id = d.date_id
        LEFT JOIN dim_employer e ON f.employer_id = e.employer_id
        LEFT JOIN dim_location l ON f.location_id = l.location_id
        LEFT JOIN dim_hazard h ON f.hazard_id = h.hazard_id
        LEFT JOIN dim_accident_type a ON f.accident_type_id = a.accident_type_id
        LIMIT 20;
    """

    df = pd.read_sql(sql, conn)
    conn.close()

    print("\n=== Full Star Schema Test (20 rows) ===")
    print(df)
    return df
