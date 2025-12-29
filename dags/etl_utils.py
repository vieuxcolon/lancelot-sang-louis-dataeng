# ==================== etl_utils.py ================================================
# The utilities contained therein are used by dag_etl_master.py
# This module provides utility functions for ETL processes
# specifically for downloading, cleaning, and loading datasets
# etl_utils.py well formatted and self-documented.

from psycopg2.extras import execute_values
from psycopg2.extras import execute_batch
import sys, os, io, csv, unicodedata
import logging, traceback, warnings, datetime
from typing import List, Dict
from io import StringIO, BytesIO
import pandas as pd, numpy as np
import psycopg2, requests
from zipfile import ZipFile
from datetime import datetime
from time import sleep
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pymongo import MongoClient, ASCENDING

# =====================================================================================

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


# ---------------------------------------------------------------------
# Ensure required environment variables exist
# ---------------------------------------------------------------------
required_vars = [
    "DATA_POSTGRES_DB",
    "DATA_POSTGRES_USER",
    "DATA_POSTGRES_PASSWORD",
]

for var in required_vars:
    if not os.getenv(var):
        raise RuntimeError(
            f"[ERROR] Required ETL Postgres env var '{var}' is missing"
        )

# -------------------------------
# ETL PostgreSQL configuration
# -------------------------------
DB_CONFIG = {
    # Connection
    "host": os.getenv("DATA_POSTGRES_HOST", "postgres"),  # Docker service name
    "port": int(os.getenv("DATA_POSTGRES_PORT", 5432)),
    "database": os.getenv("DATA_POSTGRES_DB"),
    "dbname": os.getenv("DATA_POSTGRES_DB"),              # for psycopg2 compatibility
    "user": os.getenv("DATA_POSTGRES_USER"),
    "password": os.getenv("DATA_POSTGRES_PASSWORD"),

    # Tables
    "ariadb_table": "ariadb",
    "ariadb_clean_table": "ariadb_clean",
    "ariadb_prep_table": "ariadb_prep",

    "workaccidents_table": "workaccidents",
    "workaccidents_prep_table": "workaccidents_prep",
    "workaccidents_clean_table": "workaccidents_clean",

    "fatalities_table": "fatalities",
    "fatalities_clean_table": "fatalities_clean",
    "fatalities_prep_table": "fatalities_prep",
}


def pg_connect():
    """Safe Postgres connection for ETL functions (data_db / data_user)"""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
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

# ============================================================================
# Environment validation
# Ensure all required MongoDB-related environment variables are available
# ============================================================================
required_vars = [
    "MONGO_HOST",
    "MONGO_PORT",
    "MONGO_INITDB_ROOT_USERNAME",
    "MONGO_INITDB_ROOT_PASSWORD",
    "MONGO_DB",
    "MONGO_COLLECTION",
]
for var in required_vars:
    if os.getenv(var) is None:
        raise RuntimeError(f"[ERROR] Required environment variable '{var}' is missing")

# ============================================================================
# Environment configuration
# Read MongoDB connection details and runtime configuration
# ============================================================================
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

# =====================================================================================
# CONNECTION HELPERS
# =====================================================================================

def pg_connect():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )

def get_mongo_client():
    from pymongo import MongoClient
    return MongoClient(os.getenv("MONGO_URI", "mongodb://mongo:27017"))

# =====================================================================================
# CSV PROCESSING HELPERS
# =====================================================================================

def read_csv_robust(source, sep=",", skiprows=0, dtype=str):
    return pd.read_csv(
        source,
        sep=sep,
        skiprows=skiprows,
        dtype=dtype,
        engine="python",
        encoding_errors="ignore",
    )

def read_csv_safe(path):
    return pd.read_csv(
        path,
        sep=",",
        quotechar='"',
        encoding="latin1",
        engine="python",
    )

def clean_column_names(df):
    def clean(c):
        # remove accents
        c = unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode("ascii")
        # lowercase, remove/replace special chars
        c = (
            c.strip()
            .lower()
            .replace(" ", "_")
            .replace("/", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("'", "")
            .replace("#", "")
            .rstrip("_")
        )
        return c

    df.columns = [clean(c) for c in df.columns]
    return df


# =====================================================================================
# DOWNLOAD UTILITIES HELPERS
# =====================================================================================

def download_csv(URL, filename):
    filepath = os.path.join(DATA_DIR, filename)
    print(f"📥 Trying download: {URL}")
    try:
        r = requests.get(URL, timeout=60)
        r.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(r.content)
        print(f"Download OK ({len(r.content)} bytes)")
        return r.text
    except Exception as e:
        print(f"Download error: {e}")
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                return f.read()
        raise


# =====================================================================================
# GENERIC POSTGRES LOADERS AND TABLE PROFILER HELPERS
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

def log_and_count(func, step_name, table_name=None):
    """
    Wrapper for Airflow PythonOperator:
    - Logs start and end of step
    - Optionally logs row count for a given table
    """
    def wrapped(*args, **kwargs):
        logging.info(f"▶ START: {step_name}")
        result = func(*args, **kwargs)

        # If table_name is provided, count rows in Postgres
        if table_name:
            conn = pg_connect()
            df = pd.read_sql(f'SELECT COUNT(*) AS cnt FROM "{table_name}"', conn)
            logging.info(f"✔ {step_name}: {df['cnt'][0]} rows in {table_name}")
            conn.close()
        else:
            logging.info(f"✔ END: {step_name}")
        return result
    return wrapped

def load_to_postgres(csv_input, table_name, skiprows=0, sep=","):
    conn = pg_connect()
    cur = conn.cursor()

    if isinstance(csv_input, str) and os.path.exists(csv_input):
        df = read_csv_robust(csv_input, sep=sep, skiprows=skiprows)
    else:
        df = read_csv_robust(StringIO(csv_input), sep=sep, skiprows=skiprows)

    df = clean_column_names(df)

    cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    col_defs = ", ".join([f'"{c}" TEXT' for c in df.columns])
    cur.execute(f'CREATE TABLE "{table_name}" ({col_defs})')

    insert_sql = f"""
        INSERT INTO "{table_name}"
        ({", ".join([f'"{c}"' for c in df.columns])})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """

    for _, row in df.iterrows():
        cur.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    conn.close()
    print(f"✔ Loaded table {table_name} ({len(df)} rows)")

# =====================================================================================
# NORMALIZATION HELPERS
# =====================================================================================

def normalize_country(df, column_name="country"):
    if column_name not in df.columns:
        df[column_name] = "UNKNOWN"
        return df

    def norm(v):
        if pd.isna(v):
            return "UNKNOWN"
        s = str(v).strip().upper()
        s = s.replace("É", "E").replace("È", "E").replace("Ê", "E")
        if s in {"USA", "US", "UNITED STATES", "ETATS-UNIS", "ETATS UNIS"}:
            return "USA"
        if s in {"UK", "UNITED KINGDOM", "ROYAUME-UNI"}:
            return "UK"
        return s

    df[column_name] = df[column_name].apply(norm)
    return df

# =======================================================================================
# DAG_DATA_DOWNLOAD FUNCTIONS: 1. download_ariadb_via_mongo, 2. download_all_fatalities, 
# 3. download_and_extract_zip (download workaccidents)
# ========================================================================================

# =======================================================================================
# DAG_DATA_DOWNLOAD: # 1. download_ariadb_via_mongo
# ARIADB LOADING VIA MONGODB FUNCTION AND HELPERS
# Centralized, authenticated MongoDB client creation with connectivity check
# Reads an entire MongoDB collection and writes it to a CSV file and load it to postgres
# Source CSV (URL) → MongoDB (atomic load) → [CSV on disk, Postgres table]
# ========================================================================================

def get_mongo_client():
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
    print(f"[DEBUG] Connecting to MongoDB with URI: {uri}")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print("✔ Connected to MongoDB")
        return client
    except Exception:
        print("[ERROR] Cannot connect to MongoDB")
        traceback.print_exc()
        sys.exit(1)

# ---------------------------------------------------------------------
# Download CSV from source
# ---------------------------------------------------------------------
def download_csv_from_web(url: str) -> List[Dict]:
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    csv_buffer = io.StringIO(response.text)
    reader = csv.DictReader(csv_buffer, delimiter=";")

    rows = list(reader)
    print(f"Downloaded {len(rows)} rows from ARIADB source")

    return rows

# ---------------------------------------------------------------------
# Load CSV rows into MongoDB
# ---------------------------------------------------------------------
def load_rows_into_mongo(rows: List[Dict]):
    client = get_mongo_client()
    collection = client[MONGO_DB][MONGO_COLLECTION]

    # Idempotent load
    collection.delete_many({})
    collection.insert_many(rows)

    print(f"Inserted {len(rows)} rows into MongoDB ({MONGO_DB}.{MONGO_COLLECTION})")

# ---------------------------------------------------------------------
# Export MongoDB collection to CSV on disk
# ---------------------------------------------------------------------
def export_mongo_to_csv(filename: str):
    client = get_mongo_client()
    collection = client[MONGO_DB][MONGO_COLLECTION]

    cursor = collection.find({}, {"_id": 0})
    rows = list(cursor)

    if not rows:
        raise RuntimeError("MongoDB collection is empty — cannot export CSV")

    output_path = os.path.join(DATA_DIR, filename)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=rows[0].keys(),
            delimiter=";"
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"ARIADB CSV written to {output_path}")


def download_ariadb_via_mongo(url: str, batch_size: int = 5000):
    """
    End-to-end flow:
        Source CSV (URL)
            → MongoDB temp collection (batch inserts, safe column keys)
            → Atomic rename to main collection
            → Export back to CSV ($DATA_DIR/ariadb.csv) and load to postgres 
    The output CSV filename is fixed by contract. Batch inserts prevent memory issues.
    """
    print(f"[DEBUG] Starting download_ariadb_via_mongo for URL: {url}")

    # ------------------------------------------------------------------------
    # Step 1: Download CSV from source URL
    # ------------------------------------------------------------------------
    try:
        print("[DEBUG] Attempting UTF-8 CSV read")
        df = pd.read_csv(url, sep=";", skiprows=7, encoding="utf-8")
        print(f"[DEBUG] CSV loaded successfully: {len(df)} rows")
    except UnicodeDecodeError:
        print("[WARNING] UTF-8 failed, trying latin1")
        try:
            df = pd.read_csv(url, sep=";", skiprows=7, encoding="latin1")
            print(f"[DEBUG] CSV loaded with latin1 encoding: {len(df)} rows")
        except Exception:
            print("[ERROR] CSV read failed (latin1)")
            traceback.print_exc()
            sys.exit(1)
    except Exception:
        print("[ERROR] CSV download failed")
        traceback.print_exc()
        sys.exit(1)

    # ------------------------------------------------------------------------
    # Step 2: Add ETL metadata
    # ------------------------------------------------------------------------
    try:
        run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        df["_etl_run_id"] = run_id
        df["_etl_loaded_at"] = datetime.utcnow()
        print(f"[DEBUG] ETL metadata added (_etl_run_id={run_id})")
    except Exception:
        print("[ERROR] Failed to add ETL metadata")
        traceback.print_exc()
        sys.exit(1)

    # ------------------------------------------------------------------------
    # Step 3: Load into MongoDB (temp → atomic rename) with batch inserts
    # ------------------------------------------------------------------------
    client = get_mongo_client()
    db = client[MONGO_DB]
    tmp_coll_name = f"{MONGO_COLLECTION}_tmp"
    tmp_coll = db[tmp_coll_name]

    try:
        # Drop tmp collection if exists
        if tmp_coll_name in db.list_collection_names():
            db.drop_collection(tmp_coll_name)

        # Clean column names: remove None, strip, fallback to col_i
        df.columns = [
            str(c).strip() if c is not None and str(c).strip() != "" else f"col_{i}"
            for i, c in enumerate(df.columns)
        ]

        # Batch insert to avoid memory issues
        records = df.to_dict(orient="records")
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            tmp_coll.insert_many(batch)

        # Verify row count
        if tmp_coll.count_documents({}) != len(df):
            raise RuntimeError("Row count mismatch after Mongo insert")

        # Atomic rename
        if MONGO_COLLECTION in db.list_collection_names():
            db.drop_collection(MONGO_COLLECTION)
        tmp_coll.rename(MONGO_COLLECTION)
        print(f"[DEBUG] Mongo atomic rename: {tmp_coll_name} → {MONGO_COLLECTION}")

    except Exception:
        print("[ERROR] MongoDB load/rename failed")
        traceback.print_exc()
        sys.exit(1)
    finally:
        client.close()
        print("✔ MongoDB connection closed")

    # ------------------------------------------------------------------------
    # Step 4: Export MongoDB → CSV (fixed path)
    # ------------------------------------------------------------------------
    try:
        print(f"[INFO] Exporting MongoDB collection '{MONGO_DB}.{MONGO_COLLECTION}' → 'ariadb.csv'")
        export_mongo_to_csv("ariadb.csv")
        print(f"[OK] ariadb.csv written to {DATA_DIR}")
    except Exception:
        print("[ERROR] MongoDB export to CSV failed")
        traceback.print_exc()
        sys.exit(1)

    #- ------------------------------------------------------------------------
    # Step 5: Load MongoDB collection into Postgres table 'ariadb'
    # ------------------------------------------------------------------------
    try:
        
        conn = pg_connect()
        cur = conn.cursor()
        dst_table = "ariadb"

        # Drop table if exists
        cur.execute(f'DROP TABLE IF EXISTS "{dst_table}"')

        # Create table with all columns as TEXT
        col_defs = ", ".join([f'"{c}" TEXT' for c in df.columns])
        cur.execute(f'CREATE TABLE "{dst_table}" ({col_defs})')

        # Insert rows in batches
        insert_sql = f"""
            INSERT INTO "{dst_table}" ({", ".join([f'"{c}"' for c in df.columns])})
            VALUES ({", ".join(["%s"] * len(df.columns))})
        """
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            cur.executemany(insert_sql, batch.where(pd.notnull(batch), None).values.tolist())

        conn.commit()
        conn.close()
        print(f"✔ MongoDB collection loaded into Postgres table '{dst_table}' ({len(df)} rows)")

    except Exception:
        print("[ERROR] Failed to load ARIADB into Postgres")
        traceback.print_exc()
        sys.exit(1)

# ========================================================================================================
# DAG_DATA_DOWNLOAD: # 2. download_all_fatalities (fatalities dataset is multiple CSVs)
# Download ALL fatalities CSV files 1-9 from source website, fallback to local files if in case of errors
# =========================================================================================================

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

        logger.info(f"Downloading fatalities file {i}: {url}")

        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()

            with open(local_path, "wb") as f:
                f.write(r.content)

            logger.info(f" Saved {filename} ({len(r.content)} bytes)")

        except Exception as e:
            logger.warning(
                f" Failed to download {url}: {e}. Trying local fallback..."
            )

            if not os.path.exists(local_path):
                raise FileNotFoundError(f" No local fallback available for {filename}")
            else:
                logger.info(f" Using existing local copy: {local_path}")

        saved_files.append(local_path)
        sleep(1)  # politeness delay toward OSHA servers

    logger.info(" All fatalities files obtained (downloaded or fallback).")
    return saved_files

def finalize(df):
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()]
    df["no_of_fatalities"] = 1
    df["country"] = "USA"
    return df[["date", "no_of_fatalities", "country"]]

def clean_fatalities_1_2(file, out):
    df = read_csv_safe(file).iloc[1:]
    df.columns = ["date", "employer", "victim", "hazard", "fatality", "inspection"]
    df = df[df["fatality"].notna()]
    df = finalize(df)
    df.to_csv(out, index=False)
    return len(df)

def clean_fatalities_3(file, out):
    df = read_csv_safe(file)
    df.columns = ["date","company","victim","description","fatality","inspection"] + list(df.columns[6:])
    df = df[df["fatality"].notna()]
    df = finalize(df)
    df.to_csv(out, index=False)
    return len(df)

def clean_fatalities_4(file, out):
    df = read_csv_safe(file)
    df.columns = ["date","company","description","fatality","inspection"] + list(df.columns[5:])
    df = df[df["fatality"].notna()]
    df = finalize(df)
    df.to_csv(out, index=False)
    return len(df)

def clean_fatalities_5(file, out):
    df = read_csv_safe(file)
    df.columns = ["date","company","description","fatality"]
    df = df[df["fatality"].notna()]
    df = finalize(df)
    df.to_csv(out, index=False)
    return len(df)

def clean_fatalities_6_to_9(file, out):
    df = read_csv_safe(file).iloc[:, :4]
    df.columns = ["fiscal_year","report_date","date","fatality"]
    df = df[df["fatality"].notna()]
    df = finalize(df)
    df.to_csv(out, index=False)
    return len(df)

# ========================================================================================================
# DAG_DATA_DOWNLOAD: # 3. download_and_extract_zip (used for workaccidents dataset)
# Download the zip file and unzip it to the landing zone 
# =========================================================================================================

def download_and_extract_zip(zip_url=None):
    url = zip_url or ZIP_URL
    filename = os.path.basename(url)
    filepath = os.path.join(DATA_DIR, filename)
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        zip_bytes = BytesIO(r.content)
    except Exception:
        if not os.path.exists(filepath):
            raise
        zip_bytes = open(filepath, "rb")
    with ZipFile(zip_bytes) as zf:
        csv_files = [f for f in zf.namelist() if f.lower().endswith(".csv")]
        with zf.open(csv_files[0]) as f:
            return f.read().decode("utf-8", errors="ignore")


# ==========================================================================================================
# DAG_DATA_CLEAN FUNCTIONS: 1. create_ariadb_clean, 2. create_workaccidents_clean, 3.create_fatalities_clean
# Data cleaning functions for ARIADB, Workaccidents, and Fatalities datasets
# ==========================================================================================================

# ==========================================================================================================
# DAG_DATA_CLEAN FUNCTIONS: 1. create_ariadb_clean  
# Data cleaning function for the ariadb dataset
# ==========================================================================================================
    
def create_ariadb_clean():
    """
    1 Load raw ARIADB CSV from disk
    2 Normalize column names (Mongo-safe)
    3 Apply column mapping, date normalization, deduplication, country normalization
    4 Create Postgres clean table ariadb_clean
    """
    src_csv = os.path.join(DATA_DIR, "ariadb.csv")
    dst_table = DB_CONFIG["ariadb_clean_table"]

    print(f"[INFO] Loading ARIADB raw CSV from {src_csv}")
    try:
        df = pd.read_csv(src_csv, sep=";")
        print(f"[INFO] ARIADB raw CSV loaded, {len(df)} rows")
        print("[DEBUG] ARIADB raw columns (before cleaning):")
        print(list(df.columns))
    except Exception:
        print("[ERROR] Failed to read ARIADB CSV")
        traceback.print_exc()
        sys.exit(1)

    # -----------------------------
    # Normalize column names
    # -----------------------------
    df = clean_column_names(df)
    print("[DEBUG] ARIADB columns after clean_column_names():")
    print(list(df.columns))

    # -----------------------------
    # Column mapping
    # -----------------------------
    col_map = {
        "numero_aria": "aria_id",
        "titre": "title",
        "type_de_publication": "publication_type",
        "date": "incident_date",
        "code_naf": "industry_code",
        "pays": "country",
        "department": "department",
        "commune": "municipality",
        "type_daccident": "accident_type",
        "type_evenement": "event_type",
        "classe_de_danger_clp": "hazard_class",
    }

    # Keep only present columns
    present_cols = {c: col_map[c] for c in df.columns if c in col_map}

    # -----------------------------
    # Validate required columns
    # -----------------------------
    required_cols = ["numero_aria", "department", "type_daccident", "type_evenement"]
    missing = [c for c in required_cols if c not in df.columns and c not in present_cols]
    if missing:
        raise RuntimeError(
            f"Cannot continue: missing critical columns in ARIADB CSV after normalization: {missing}"
        )

    # Rename columns
    df = df[list(present_cols.keys())].rename(columns=present_cols).copy()
    print("[DEBUG] ARIADB columns after mapping:")
    print(list(df.columns))

    # -----------------------------
    # Normalize date
    # -----------------------------
    if "incident_date" in df.columns:
        df["incident_date"] = (
            pd.to_datetime(df["incident_date"], errors="coerce")
            .dt.strftime("%Y-%m-%d")
        )

    # -----------------------------
    # Deduplicate on primary key
    # -----------------------------
    if "aria_id" in df.columns:
        df.drop_duplicates(subset=["aria_id"], inplace=True)

    # -----------------------------
    # Normalize country
    # -----------------------------
    if "country" in df.columns:
        df = normalize_country(df, "country")

    # -----------------------------
    # Enforce final column ordering
    # -----------------------------
    final_columns = [
        "aria_id",
        "title",
        "publication_type",
        "incident_date",
        "industry_code",
        "country",
        "department",
        "municipality",
        "accident_type",
        "event_type",
        "hazard_class",
    ]
    df = df[[c for c in final_columns if c in df.columns]]

    print("[DEBUG] Final ARIADB clean dataframe preview (first 10 rows):")
    print(df.head(10).to_string(index=False))

    # -----------------------------
    # Create clean table in Postgres
    # -----------------------------
    conn = pg_connect()
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')
    col_defs = ", ".join([f'"{c}" TEXT' for c in df.columns])
    pk = ', PRIMARY KEY ("aria_id")' if "aria_id" in df.columns else ""
    cursor.execute(f'CREATE TABLE "{dst_table}" ({col_defs}{pk});')

    # -----------------------------
    # Batch insert rows
    # -----------------------------
    from psycopg2.extras import execute_values

    if len(df) > 0:
        cols = ", ".join(f'"{c}"' for c in df.columns)
        insert_sql = f'INSERT INTO "{dst_table}" ({cols}) VALUES %s'
        values = [[None if pd.isna(v) else v for v in row] for row in df.to_numpy()]
        execute_values(cursor, insert_sql, values)

    conn.commit()
    conn.close()
    print(f"Created {dst_table} ({len(df)} rows)")

# ==========================================================================================================
# DAG_DATA_CLEAN FUNCTIONS: 2. create_workaccidents_clean
# Data cleaning function for the workaccidents dataset
# ==========================================================================================================

def create_workaccidents_clean():
    """
    ETL function to load Workaccidents CSV (already downloaded)
    into Postgres, clean it, and create workaccidents_clean.

    DAG semantics:
    - dag_data_download → download only
    - dag_data_clean → load + clean
    """

    # ----------------------------
    # Step 0: Paths & tables
    # ----------------------------
    raw_csv_path = os.path.join(DATA_DIR, "workaccidents.csv")
    src_table = DB_CONFIG["workaccidents_table"]
    dst_table = DB_CONFIG["workaccidents_clean_table"]

    print("\n🔎 STEP 0 — CSV PATH CHECK")
    print(f"CSV path: {raw_csv_path}")

    if not os.path.exists(raw_csv_path):
        raise FileNotFoundError(
            f" Required CSV not found: {raw_csv_path}. "
            "dag_data_download must run first."
        )

    file_size = os.path.getsize(raw_csv_path)
    print(f"CSV size: {file_size} bytes")

    if file_size == 0:
        raise ValueError(" CSV file exists but is EMPTY")

    # ----------------------------
    # Step 1: Load raw CSV → Postgres
    # ----------------------------
    print("\n STEP 1 — LOAD RAW CSV INTO POSTGRES")
    print(f"Target table: {src_table}")

    load_to_postgres(
        raw_csv_path,
        table_name=src_table,
        sep=","
    )

    # ----------------------------
    # Step 2: Validate raw table
    # ----------------------------
    conn = pg_connect()

    print("\n STEP 2 — RAW TABLE VALIDATION")

    count_df = pd.read_sql(
        f'SELECT COUNT(*) AS cnt FROM "{src_table}"',
        conn
    )

    raw_count = int(count_df["cnt"][0])
    print(f"Rows in raw table '{src_table}': {raw_count}")

    if raw_count == 0:
        conn.close()
        raise RuntimeError(
            f" Raw table '{src_table}' is EMPTY after load. "
            "Aborting to avoid silent data loss."
        )

    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)
    print("Raw columns:", list(df.columns))

    # ----------------------------
    # Step 3: Cleaning & normalization
    # ----------------------------
    print("\n STEP 3 — CLEANING PHASE")

    # Drop large text column if present
    if "final_narrative" in df.columns:
        df.drop(columns=["final_narrative"], inplace=True)

    # Normalize date
    date_col = "eventdate" if "eventdate" in df.columns else None
    print(f"Detected date column: {date_col}")

    if not date_col:
        conn.close()
        raise RuntimeError(" Required column 'eventdate' not found")

    df["accident_date"] = (
        pd.to_datetime(df[date_col], errors="coerce")
        .dt.strftime("%Y-%m-%d")
    )

    # Normalize country
    df["country"] = "USA"

    # ----------------------------
    # Step 4: Column selection
    # ----------------------------
    selected_columns = [
        "id", "upa", "accident_date", "employer", "address1", "address2",
        "city", "state", "zip", "latitude", "longitude", "primary_naics",
        "hospitalized", "amputation", "loss_of_eye", "inspection", "nature",
        "naturetitle", "part_of_body", "part_of_body_title", "event",
        "eventtitle", "source", "sourcetitle", "secondary_source",
        "secondary_source_title", "federalstate", "country",
    ]

    df_clean = df[[c for c in selected_columns if c in df.columns]].copy()
    print(f"Rows after selection: {len(df_clean)}")

    if df_clean.empty:
        conn.close()
        raise RuntimeError("Cleaning resulted in EMPTY dataframe")

    # Deduplicate
    if "upa" in df_clean.columns:
        before = len(df_clean)
        df_clean.drop_duplicates(subset=["upa"], inplace=True)
        print(f"Deduplicated by upa: {before} → {len(df_clean)}")

    # ----------------------------
    # Step 5: Create clean table
    # ----------------------------
    print("\n STEP 5 — CREATE CLEAN TABLE")

    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')

    col_defs = ", ".join([f'"{c}" TEXT' for c in df_clean.columns])
    cursor.execute(f'CREATE TABLE "{dst_table}" ({col_defs});')

    insert_sql = f"""
        INSERT INTO "{dst_table}"
        ({", ".join([f'"{c}"' for c in df_clean.columns])})
        VALUES ({", ".join(["%s"] * len(df_clean.columns))})
    """

    inserted = 0
    for _, row in df_clean.iterrows():
        cursor.execute(
            insert_sql,
            [None if pd.isna(v) else v for v in row.values]
        )
        inserted += 1

    conn.commit()
    conn.close()

    print(f" Created '{dst_table}' with {inserted} rows")

# ==========================================================================================================
# DAG_DATA_CLEAN FUNCTIONS: 3. create_fatalities_clean
# Data cleaning function for the fatalities dataset
# ==========================================================================================================
# =====================================================================================
# LOAD FATALITIES_CLEAN INTO POSTGRES
# =====================================================================================

def load_fatalities_to_postgres():
    csv_path = os.path.join(DATA_DIR, "fatalities_clean.csv")
    df = pd.read_csv(csv_path)

    conn = pg_connect()
    cur = conn.cursor()

    table = DB_CONFIG["fatalities_clean_table"]
    cur.execute(f'DROP TABLE IF EXISTS "{table}"')
    cur.execute("""
        CREATE TABLE fatalities_clean (
            fatality_id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            no_of_fatalities INTEGER NOT NULL,
            country TEXT NOT NULL
        )
    """)

    records = list(df.itertuples(index=False, name=None))
    execute_batch(
        cur,
        f"""
        INSERT INTO "{table}" (date, no_of_fatalities, country)
        VALUES (%s, %s, %s)
        """,
        records,
        page_size=1000,
    )

    conn.commit()
    conn.close()
    print(f"fatalities_clean loaded ({len(records)} rows)")

# =====================================================================================
# READ FATALITIES CLEAN FILES AND CREATE FINAL COMBINED TABLE
# =====================================================================================
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
            continue
        fn(src, dst)
        dfs.append(pd.read_csv(dst))

    if not dfs:
        raise RuntimeError("No fatalities files cleaned")

    final_df = pd.concat(dfs, ignore_index=True)

    # RAW TABLE
    conn = pg_connect()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS fatalities")
    col_defs = ", ".join([f'"{c}" TEXT' for c in final_df.columns])
    cur.execute(f"CREATE TABLE fatalities ({col_defs})")

    insert_sql = f"""
        INSERT INTO fatalities ({", ".join([f'"{c}"' for c in final_df.columns])})
        VALUES ({", ".join(["%s"] * len(final_df.columns))})
    """

    for _, r in final_df.iterrows():
        cur.execute(insert_sql, [None if pd.isna(v) else str(v) for v in r.values])

    conn.commit()
    conn.close()
    print(f"Raw fatalities table created ({len(final_df)} rows)")

    # CLEAN
    final_df = normalize_country(final_df)
    final_df.to_csv(os.path.join(DATA_DIR, "fatalities_clean.csv"), index=False)
    load_fatalities_to_postgres()

    
# ==========================================================================================================
# DAG_DATA_PREP FUNCTIONS:  1. create_ariadb_prep, 2. create_workaccidents_clean 2. create_fatalities_prep
# Data preparation functions for star schema source tables
# ==========================================================================================================
# ==========================================================================================================
# DAG_DATA_PREP FUNCTIONS:  1. create_ariadb_prep
# create ariadb_prep table from ariadb_clean
# ==========================================================================================================

def create_ariadb_prep():
    src_table = DB_CONFIG["ariadb_clean_table"]
    dst_table = DB_CONFIG["ariadb_prep_table"]
    conn = pg_connect()
    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)

    # 1 Rename and convert date to string YYYY-MM-DD
    if "incident_date" in df.columns:
        df["date"] = pd.to_datetime(df["incident_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df.drop(columns=["incident_date"], inplace=True)

    # 2 Normalize country
    if "country" in df.columns:
        df = normalize_country(df, "country")
    else:
        df["country"] = "UNKNOWN"

    # 3 Convert numeric IDs
    for col in ["industry_code", "aria_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # 4 Add fatality flag based on hazard_class
    df["fatality"] = df.get("hazard_class", "").apply(
        lambda x: 1 if pd.notna(x) and str(x).strip() != "" else 0
    )

    # 5 Drop rows where all key columns are null
    key_cols = ["aria_id","date","country","department","municipality","hazard_class","industry_code"]
    existing_keys = [c for c in key_cols if c in df.columns]
    df = df.dropna(how="all", subset=existing_keys)

    # 6 Drop rows with only PK populated
    non_pk_cols = [c for c in existing_keys if c != "aria_id"]
    if non_pk_cols:
        df = df[df[non_pk_cols].notna().any(axis=1)]

    # 7 Deduplicate on PK
    if "aria_id" in df.columns:
        df = df.drop_duplicates(subset=["aria_id"])

    # 8 Create prep table with correct types
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')
    col_defs = []
    for c in df.columns:
        if c in ["aria_id","industry_code"]:
            col_defs.append(f'"{c}" BIGINT')
        elif c == "fatality":
            col_defs.append(f'"{c}" INT')
        else:
            col_defs.append(f'"{c}" TEXT')
    pk = ", PRIMARY KEY (aria_id)" if "aria_id" in df.columns else ""
    cursor.execute(f'CREATE TABLE "{dst_table}" ({", ".join(col_defs)}{pk});')

    # 9 Insert data
    insert_sql = f"""
        INSERT INTO "{dst_table}" ({", ".join([f'"{c}"' for c in df.columns])})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """
    for _, row in df.iterrows():
        cursor.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    conn.close()
    print(f" Created ariadb_prep ({len(df)} rows)")
    
# ==========================================================================================================
# DAG_DATA_PREP FUNCTIONS:  2. create_workaccidents_prep
# create workaccidents_prep table from workaccidents_clean
# ==========================================================================================================

def create_workaccidents_prep():
    src_table = DB_CONFIG["workaccidents_clean_table"]
    dst_table = DB_CONFIG["workaccidents_prep_table"]
    conn = pg_connect()
    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)

    # 1 Rename and convert date
    if "accident_date" in df.columns:
        df["date"] = pd.to_datetime(df["accident_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df.drop(columns=["accident_date"], inplace=True)

    # 2 Normalize country
    if "country" not in df.columns:
        df["country"] = "USA"
    df = normalize_country(df, "country")

    # 3 Convert numeric IDs
    for col in ["upa","primary_naics"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # 4 Add fatality flag based on naturetitle
    df["fatality"] = df.get("naturetitle", "").apply(lambda x: 1 if pd.notna(x) and str(x).strip() != "" else 0)

    # 5 Drop rows where all key columns are null
    key_cols = ["upa","id","date","country","employer","city","state","primary_naics","event","naturetitle"]
    existing_keys = [c for c in key_cols if c in df.columns]
    df = df.dropna(how="all", subset=existing_keys)

    # 6 Drop rows with only PK populated
    non_pk_cols = [c for c in existing_keys if c != "upa"]
    if non_pk_cols:
        df = df[df[non_pk_cols].notna().any(axis=1)]

    # 7 Deduplicate on PK
    if "upa" in df.columns:
        df = df.drop_duplicates(subset=["upa"])

    # 8 Create prep table with correct types
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')
    col_defs = []
    for c in df.columns:
        if c in ["upa","primary_naics"]:
            col_defs.append(f'"{c}" BIGINT')
        elif c == "fatality":
            col_defs.append(f'"{c}" INT')
        else:
            col_defs.append(f'"{c}" TEXT')
    pk = ", PRIMARY KEY (upa)" if "upa" in df.columns else ""
    cursor.execute(f'CREATE TABLE "{dst_table}" ({", ".join(col_defs)}{pk});')

    # 9 Insert data
    insert_sql = f"""
        INSERT INTO "{dst_table}" ({", ".join([f'"{c}"' for c in df.columns])})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """
    for _, row in df.iterrows():
        cursor.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    conn.close()
    print(f" Created workaccidents_prep ({len(df)} rows)")
  
# ==========================================================================================================
# DAG_DATA_PREP FUNCTIONS:  3. create_fatalities_prep
# create fatalities_prep table from fatalities_clean
# ==========================================================================================================

def create_fatalities_prep():
    src_table = DB_CONFIG["fatalities_clean_table"]
    dst_table = DB_CONFIG["fatalities_prep_table"]
    conn = pg_connect()
    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)

    # 1 Convert date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # 2 Normalize country
    if "country" not in df.columns:
        df["country"] = "UNKNOWN"
    df = normalize_country(df, "country")

    # 3 Convert fatality column to int
    if "no_of_fatalities" in df.columns:
        df["fatality"] = df["no_of_fatalities"].apply(lambda x: 1 if pd.notna(x) and x > 0 else 0)
    else:
        df["fatality"] = 1

    # 4 Drop rows where all key columns are null
    key_cols = ["fatality_id","date","country","fatality"]
    df = df.dropna(how="all", subset=[c for c in key_cols if c in df.columns])

    # 5 Drop rows with only PK populated
    non_pk_cols = [c for c in key_cols if c != "fatality_id" and c in df.columns]
    if non_pk_cols:
        df = df[df[non_pk_cols].notna().any(axis=1)]

    # 6 Deduplicate on PK
    if "fatality_id" in df.columns:
        df = df.drop_duplicates(subset=["fatality_id"])

    # 7 Create prep table with correct types
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')
    col_defs = []
    for c in df.columns:
        if c == "fatality_id":
            col_defs.append(f'"{c}" BIGINT')
        elif c == "fatality":
            col_defs.append(f'"{c}" INT')
        else:
            col_defs.append(f'"{c}" TEXT')
    pk = ", PRIMARY KEY (fatality_id)" if "fatality_id" in df.columns else ""
    cursor.execute(f'CREATE TABLE "{dst_table}" ({", ".join(col_defs)}{pk});')

    # 8 Insert data
    insert_sql = f"""
        INSERT INTO "{dst_table}" ({", ".join([f'"{c}"' for c in df.columns])})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """
    for _, row in df.iterrows():
        cursor.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    conn.close()
    print(f" Created fatalities_prep ({len(df)} rows)")
    
    
# ==========================================================================================================
# DAG_DATA_ANALYZE FUNCTIONS:  1. drop_dimensions, 2. drop_fact, 3. create_dimensions, 4. populate_dimensions
# 5. create_fact, 6.populate_fact, 7. min_test_star_schema, 8. full_test_star_schema
# for star schema creation and testing
# ==========================================================================================================
# ==========================================================================================================
# DAG_DATA_ANALYZE FUNCTIONS:  1. drop_dimensions
# Drops all star schema dimension tables if they exist.
# ==========================================================================================================

def drop_dimensions(*args, **kwargs):
    """
    Drops all star schema dimension tables if they exist.
    """
    conn = pg_connect()
    cur = conn.cursor()
    tables = ["dim_country", "dim_date", "dim_location", "dim_industry",
              "dim_accident_type", "dim_hazard", "dim_employer",
              "country_synonym", "original_dim_location"]
    for t in tables:
        cur.execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
    conn.commit()
    conn.close()
    
# ==========================================================================================================
# DAG_DATA_ANALYSE FUNCTIONS:  2. drop_fact
# Drops the star schema fact table if it exists.
# ==========================================================================================================

def drop_fact(*args, **kwargs):
    """
    Drops the star schema fact table if it exists.
    """
    conn = pg_connect()
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS fact_accidents CASCADE')
    conn.commit()
    conn.close()

# ==========================================================================================================
# DAG_DATA_ANALYSE FUNCTIONS:  3. create_dimensions
# Creates all dimension tables (without data) for the star schema.
# ==========================================================================================================

def create_dimensions(*args, **kwargs):
    """
    Creates all dimension tables (without data) for the star schema.
    Column names are aligned with populate_dimensions inserts.
    """
    conn = pg_connect()
    cur = conn.cursor()

    # ----------------------------
    # dim_date
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id SERIAL PRIMARY KEY,
            date DATE UNIQUE
        )
    """)

    # ----------------------------
    # dim_country
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_country (
            country_id SERIAL PRIMARY KEY,
            country_name TEXT UNIQUE,
            country_code TEXT
        )
    """)

    # ----------------------------
    # dim_location (optional, populated later)
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_location (
            location_id SERIAL PRIMARY KEY,
            country TEXT,
            country_id INT REFERENCES dim_country(country_id)
        )
    """)

    # ----------------------------
    # dim_industry
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_industry (
            industry_id SERIAL PRIMARY KEY,
            industry_code TEXT UNIQUE
        )
    """)

    # ----------------------------
    # dim_accident_type
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_accident_type (
            accident_type_id SERIAL PRIMARY KEY,
            accident_type TEXT UNIQUE
        )
    """)

    # ----------------------------
    # dim_hazard
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_hazard (
            hazard_id SERIAL PRIMARY KEY,
            hazard_class TEXT UNIQUE
        )
    """)

    # ----------------------------
    # dim_employer
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_employer (
            employer_id SERIAL PRIMARY KEY,
            employer TEXT UNIQUE
        )
    """)

    conn.commit()
    conn.close()
    print(" Dimension tables created successfully")

# ==========================================================================================================
# DAG_DATA_ANALYSE FUNCTIONS:  4. populate_dimensions
# Populates all dimension tables from prep tables with proper primary keys.
# ==========================================================================================================  

def populate_dimensions(*args, **kwargs):
    """
    Populates all dimension tables from prep tables with proper primary keys.
    Uses *_prep tables to ensure date, country, and fatality columns are available and normalized.
    """
    conn = pg_connect()
    cur = conn.cursor()

    print(" Loading prep tables for dimension population...")

    # Load prep tables (cleaned and normalized with dates, country, fatality)
    df_aria = pd.read_sql('SELECT * FROM ariadb_prep', conn)
    df_work = pd.read_sql('SELECT * FROM workaccidents_prep', conn)
    df_fatal = pd.read_sql('SELECT * FROM fatalities_prep', conn)

    print(f"ARIA prep rows: {len(df_aria)}, Work prep rows: {len(df_work)}, Fatalities prep rows: {len(df_fatal)}")

    # ----------------------------
    # Helper: Insert unique values into dimension with surrogate PK
    # ----------------------------
    def insert_dim(df_list, col, table, id_col):
        frames = [df[[col]] for df in df_list if col in df.columns]
        if not frames:
            print(f"⚠ No column '{col}' found in prep tables, skipping {table}")
            return
        df_dim = pd.concat(frames, ignore_index=True).dropna().drop_duplicates()
        inserted = 0
        for v in df_dim[col]:
            cur.execute(
                f"INSERT INTO {table} ({col}) VALUES (%s) ON CONFLICT ({col}) DO NOTHING",
                (v,)
            )
            inserted += 1
        conn.commit()
        print(f" Populated {table} with {inserted} unique values from column '{col}'")

    # ----------------------------
    # Populate standard dimensions
    # ----------------------------
    insert_dim([df_aria, df_work, df_fatal], "industry_code", "dim_industry", "industry_id")
    insert_dim([df_aria, df_work, df_fatal], "accident_type", "dim_accident_type", "accident_type_id")
    insert_dim([df_aria, df_work, df_fatal], "hazard_class", "dim_hazard", "hazard_id")
    insert_dim([df_aria, df_work, df_fatal], "employer", "dim_employer", "employer_id")

    # ----------------------------
    # Populate dim_date
    # ----------------------------
    all_dates = pd.concat([
        df_aria.get("date", pd.Series(dtype=str)),
        df_work.get("date", pd.Series(dtype=str)),
        df_fatal.get("date", pd.Series(dtype=str))
    ]).dropna().drop_duplicates()
    inserted_dates = 0
    for d in all_dates:
        cur.execute(
            "INSERT INTO dim_date (date) VALUES (%s) ON CONFLICT (date) DO NOTHING",
            (pd.to_datetime(d).date(),)  # ensures Python datetime.date object
        )
        inserted_dates += 1
    conn.commit()
    print(f" Populated dim_date with {inserted_dates} unique dates")

    # ----------------------------
    # Populate dim_country
    # ----------------------------
    all_countries = pd.concat([
        df_aria.get("country", pd.Series(dtype=str)),
        df_work.get("country", pd.Series(dtype=str)),
        df_fatal.get("country", pd.Series(dtype=str))
    ]).dropna().drop_duplicates()

    predefined_codes = {
        "FRANCE": "FR","USA": "US", "GERMANY": "DE", "UK": "GB", "PAYS-BAS": "NL", "AUTRICHE": "AT", "LIBAN": "LB", "CHINE": "CN",
        "HONGRIE": "HU", "BELGIQUE": "BE", "SUEDE": "SE", "ITALIE": "IT",  "SUISSE": "CH", "BRESIL": "BR", "DANEMARK": "DK",
        "SENEGAL": "SN", "INDE": "IN", "FINLANDE": "FI", "BULGARIE": "BG", "POLOGNE": "PL", "CANADA": "CA", "LITUANIE": "LT",
        "AUSTRALIE": "AU", "MEXIQUE": "MX", "ESPAGNE": "ES", "JAPON": "JP", "NOUVELLE-ZELANDE": "NZ", "PORTUGAL": "PT", "SLOVENIE": "SI",
        "TCHEQUE (REP.)": "CZ", "UKRAINE": "UA", "BIELORUSSIE": "BY", "ROUMANIE": "RO", "SLOVAQUIE": "SK", "COREE DU SUD": "KR",
        "JORDANIE": "JO", "OUZBEKISTAN": "UZ", "LAOS": "LA", "VENEZUELA": "VE", "CHYPRE": "CY", "GRECE": "GR", "RUSSIE": "RU",
        "ESTONIE": "EE", "CONGO (REP.)": "CG", "SRI LANKA": "LK", "PAKISTAN": "PK", "THAILANDE": "TH", "ZIMBABWE": "ZW", "IRLANDE": "IE",
        "AFRIQUE DU SUD": "ZA", "CUBA": "CU", "PEROU": "PE", "SIERRA LEONE": "SL", "TAIWAN": "TW", "ARABIE SAOUDITE": "SA", "ALGERIE": "DZ",
        "TURQUIE": "TR", "LIBYE": "LY", "IRAK": "IQ", "ISRAEL": "IL", "OUGANDA": "UG", "INDONESIE": "ID", "SAINTE-LUCIE": "LC", 
        "HAITI": "HT", "MAROC": "MA", "LUXEMBOURG": "LU", "NORVEGE": "NO", "NIGERIA": "NG", "MAURICE": "MU", "NIGER": "NE", "KENYA": "KE",
        "ETHIOPIE": "ET", "SURINAME": "SR", "AZERBAIDJAN": "AZ", "GHANA": "GH", "KAZAKHSTAN": "KZ", "CAMEROUN": "CM", "SEYCHELLES": "SC",
        "CHILI": "CL", "COLOMBIE": "CO", "EQUATEUR": "EC", "UNKNOWN": "XX"
        # more countries can be added as needed
}
    inserted_countries = 0
    for c in all_countries:
        code = predefined_codes.get(c.upper(), "XX")
        cur.execute(
            """
            INSERT INTO dim_country (country_name, country_code)
            VALUES (%s, %s)
            ON CONFLICT (country_name) DO NOTHING
            """,
            (c, code)
        )
        inserted_countries += 1
    conn.commit()
    print(f" Populated dim_country with {inserted_countries} unique countries")

    conn.close()
    print(" Dimension tables populated successfully from prep tables")

# ==========================================================================================================
# DAG_DATA_ANALYSE FUNCTIONS:  5. create_fact
# Creates the fact_accidents table for the star schema (with no data).
# =========================================================================================================

def create_fact(*args, **kwargs):
    conn = pg_connect()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_accidents (
            date_id BIGINT,
            country_id BIGINT,
            industry_id BIGINT,
            accident_type_id BIGINT,
            hazard_id BIGINT,
            employer_id BIGINT,
            fatality TEXT
        )
    """)

    conn.commit()
    conn.close()
    print(" fact_accidents table created successfully")
    
# ==========================================================================================================
# DAG_DATA_ANALYSE FUNCTIONS:  6. populate_fact
# Populates the fact_accidents table from prep tables and dimension tables.
# =========================================================================================================


def populate_fact(*args, **kwargs):
    """
    Populates the fact_accidents table from prep tables and dimension tables.
    Prep tables already have correct types, so no further conversion needed.
    Full debug: tracks prep source, duplicates, missing FKs, and bulk insert.
    """
    conn = pg_connect()
    cur = conn.cursor()

    print(" Loading prep tables for fact table population...")

    # ----------------------------
    # Load prep tables with source tracking
    # ----------------------------
    df_aria = pd.read_sql("SELECT *, 'aria' AS __source FROM ariadb_prep", conn)
    df_work = pd.read_sql("SELECT *, 'work' AS __source FROM workaccidents_prep", conn)
    df_fatal = pd.read_sql("SELECT *, 'fatal' AS __source FROM fatalities_prep", conn)

    df_fact = pd.concat([df_aria, df_work, df_fatal], ignore_index=True)
    print(f" Combined prep tables: {len(df_fact)} rows")
    print("Sample prep sources distribution:")
    print(df_fact['__source'].value_counts())

    # ----------------------------
    # Standardize join columns (strings only)
    # ----------------------------
    df_fact["date"] = df_fact["date"].astype(str).str.strip()
    df_fact["country"] = df_fact["country"].astype(str).str.strip().str.upper()

    # ----------------------------
    # Load dimension tables
    # ----------------------------
    dim_industry = pd.read_sql("SELECT industry_id, industry_code FROM dim_industry", conn)
    dim_accident_type = pd.read_sql("SELECT accident_type_id, accident_type FROM dim_accident_type", conn)
    dim_hazard = pd.read_sql("SELECT hazard_id, hazard_class FROM dim_hazard", conn)
    dim_employer = pd.read_sql("SELECT employer_id, employer FROM dim_employer", conn)
    dim_country = pd.read_sql("SELECT country_id, country_name FROM dim_country", conn)
    dim_date = pd.read_sql("SELECT date_id, date FROM dim_date", conn)

    dim_country["country_name"] = dim_country["country_name"].astype(str).str.strip().str.upper()
    dim_date["date"] = dim_date["date"].astype(str).str.strip()

    # ----------------------------
    # Merge dimension tables
    # ----------------------------
    print(" Merging dimension tables into prep data...")
    df_fact = df_fact.merge(dim_industry, how="left", on="industry_code")
    df_fact = df_fact.merge(dim_accident_type, how="left", on="accident_type")
    df_fact = df_fact.merge(dim_hazard, how="left", on="hazard_class")
    df_fact = df_fact.merge(dim_employer, how="left", on="employer")
    df_fact = df_fact.merge(dim_country, how="left", left_on="country", right_on="country_name")
    df_fact = df_fact.merge(dim_date, how="left", on="date")

    # ----------------------------
    # FK checks
    # ----------------------------
    fk_cols = ["date_id", "country_id", "industry_id", "accident_type_id", "hazard_id", "employer_id"]
    missing_fk_counts = df_fact[fk_cols].isna().sum()
    print(" Missing foreign keys count per column:")
    print(missing_fk_counts)

    # ----------------------------
    # Duplicate check
    # ----------------------------
    duplicate_count = df_fact.duplicated(subset=fk_cols + ["fatality"]).sum()
    print(f" Total duplicates (FKs + fatality): {duplicate_count}")

    # ----------------------------
    # Prepare final fact table (already correctly typed)
    # ----------------------------
    df_fact_final = df_fact[fk_cols + ["fatality"]].copy()

    # ----------------------------
    # Drop rows with missing FKs
    # ----------------------------
    df_fact_final_clean = df_fact_final.dropna(subset=fk_cols)
    print(f" Rows prepared for insert after dropping missing FKs: {len(df_fact_final_clean)}")

    # ----------------------------
    # Bulk insert using execute_values
    # ----------------------------
    rows_to_insert = [tuple(x) for x in df_fact_final_clean.to_numpy()]
    sql = """
        INSERT INTO fact_accidents (
            date_id, country_id, industry_id, accident_type_id,
            hazard_id, employer_id, fatality
        ) VALUES %s
    """
    psycopg2.extras.execute_values(cur, sql, rows_to_insert, template=None, page_size=1000)

    conn.commit()
    conn.close()
    print(f" Fact table populated successfully with {len(rows_to_insert)} rows (bulk insert)")


# ==========================================================================================================
# DAG_DATA_ANALYSE FUNCTIONS:  7. min_test_star_schema
# Minimal test of star schema functions.
#  =========================================================================================================

def min_test_star_schema(*args, **kwargs):
    """
    Select top 5 rows from fact_accidents to ensure foreign keys exist.
    Updated for current schema: country_id instead of location_id.
    """
    conn = pg_connect()
    query = """
        SELECT date_id, country_id, employer_id, hazard_id,
               accident_type_id, industry_id, fatality
        FROM fact_accidents
        LIMIT 5;
    """
    df_test = pd.read_sql(query, conn)
    conn.close()

    print("min_test_star_schema result (top 5 rows):")
    print(df_test)
    print("\n Data types:")
    print(df_test.dtypes)
    print("\n Numeric ranges:")
    for col in ['date_id', 'country_id', 'industry_id', 'accident_type_id', 'hazard_id', 'employer_id', 'fatality']:
        if col in df_test.columns:
            print(f"{col}: min={df_test[col].min()}, max={df_test[col].max()}")

    return df_test

# ==========================================================================================================
# DAG_DATA_ANALYSE FUNCTIONS:  8. full_test_star_schema
# Full test of star schema functions with joins to all dimensions.
#  =========================================================================================================

def full_test_star_schema(*args, **kwargs):
    """
    Select top 20 rows from fact_accidents with joins to all dimensions.
    Updated for current schema: country_id instead of location_id, dim_country instead of dim_location.
    """
    conn = pg_connect()
    query = """
    SELECT 
        f.date_id, d.date,
        f.country_id, c.country_name,
        f.employer_id, e.employer AS employer_name,
        f.hazard_id, h.hazard_class AS hazard_name,
        f.accident_type_id, a.accident_type AS accident_type_name,
        f.industry_id, i.industry_code AS industry_name,
        f.fatality
    FROM fact_accidents f
    LEFT JOIN dim_date d ON f.date_id = d.date_id
    LEFT JOIN dim_country c ON f.country_id = c.country_id
    LEFT JOIN dim_employer e ON f.employer_id = e.employer_id
    LEFT JOIN dim_hazard h ON f.hazard_id = h.hazard_id
    LEFT JOIN dim_accident_type a ON f.accident_type_id = a.accident_type_id
    LEFT JOIN dim_industry i ON f.industry_id = i.industry_id
    LIMIT 20;
    """
    df_test = pd.read_sql(query, conn)
    conn.close()

    print(" full_test_star_schema query returned:")
    print(df_test.head(20))
    print(f"Total rows returned: {len(df_test)}")

    # Debug: data types and ranges
    print("\n Data types:")
    print(df_test.dtypes)
    print("\n Numeric ranges for IDs and fatality:")
    for col in ['date_id', 'country_id', 'industry_id', 'accident_type_id', 'hazard_id', 'employer_id', 'fatality']:
        if col in df_test.columns:
            print(f"{col}: min={df_test[col].min()}, max={df_test[col].max()}")

    return df_test

# ==========================================================================================================
# DAG_DATA_ANALYTICS_VALIDATION FUNCTIONS:  1. run_analytics_validation
# Run key analytics validation queries on the star schema fact table.
# ==========================================================================================================

def run_analytics_validation():
    """
    Run all predefined analytics validation queries and save outputs to DATA_DIR as text files.
    Each query touches dim_date, dim_country, and fatalities.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = pg_connect()

    queries = {
        "1_fatalities_by_year": """
            SELECT d.date AS year, COUNT(*) AS total_fatalities
            FROM fact_accidents f
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.date
            ORDER BY d.date
        """,
        "2_fatalities_by_country": """
            SELECT c.country_name AS country, COUNT(*) AS total_fatalities
            FROM fact_accidents f
            JOIN dim_country c ON f.country_id = c.country_id
            GROUP BY c.country_name
            ORDER BY total_fatalities DESC
        """,
        "3_fatalities_by_industry": """
            SELECT i.industry_code AS industry, COUNT(*) AS total_fatalities
            FROM fact_accidents f
            JOIN dim_industry i ON f.industry_id = i.industry_id
            GROUP BY i.industry_code
            ORDER BY total_fatalities DESC
        """,
        "4_france_vs_usa_last_10_years": """
            SELECT d.date AS year,
                   SUM(CASE WHEN c.country_name='FRANCE' THEN 1 ELSE 0 END) AS france,
                   SUM(CASE WHEN c.country_name='USA' THEN 1 ELSE 0 END) AS usa
            FROM fact_accidents f
            JOIN dim_country c ON f.country_id = c.country_id
            JOIN dim_date d ON f.date_id = d.date_id
            WHERE d.date >= EXTRACT(YEAR FROM CURRENT_DATE)-10
            GROUP BY d.date
            ORDER BY d.date
        """,
        "5_top_10_countries_fatalities": """
            SELECT c.country_name, COUNT(*) AS total_fatalities
            FROM fact_accidents f
            JOIN dim_country c ON f.country_id = c.country_id
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY c.country_name
            ORDER BY total_fatalities DESC
            LIMIT 10
        """,
        "6_top_10_industries_fatalities": """
            SELECT i.industry_code, COUNT(*) AS total_fatalities
            FROM fact_accidents f
            JOIN dim_industry i ON f.industry_id = i.industry_id
            JOIN dim_country c ON f.country_id = c.country_id
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY i.industry_code
            ORDER BY total_fatalities DESC
            LIMIT 10
        """,
        "7_top_10_employers_fatalities": """
            SELECT e.employer, COUNT(*) AS total_fatalities
            FROM fact_accidents f
            JOIN dim_employer e ON f.employer_id = e.employer_id
            JOIN dim_country c ON f.country_id = c.country_id
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY e.employer
            ORDER BY total_fatalities DESC
            LIMIT 10
        """,
        "8_recent_10_year_fatalities_by_country": """
            SELECT c.country_name, COUNT(*) AS total_fatalities
            FROM fact_accidents f
            JOIN dim_country c ON f.country_id = c.country_id
            JOIN dim_date d ON f.date_id = d.date_id
            WHERE d.date >= EXTRACT(YEAR FROM CURRENT_DATE)-10
            GROUP BY c.country_name
            ORDER BY total_fatalities DESC
            LIMIT 10
        """,
        "9_fatalities_trend_by_industry_last_10_years": """
            SELECT d.date AS year, i.industry_code, COUNT(*) AS total_fatalities
            FROM fact_accidents f
            JOIN dim_industry i ON f.industry_id = i.industry_id
            JOIN dim_date d ON f.date_id = d.date_id
            JOIN dim_country c ON f.country_id = c.country_id
            WHERE d.date >= EXTRACT(YEAR FROM CURRENT_DATE)-10
            GROUP BY d.date, i.industry_code
            ORDER BY d.date, total_fatalities DESC
            LIMIT 10
        """
    }

    for name, query in queries.items():
        df = pd.read_sql(query, conn)
        file_path = os.path.join(DATA_DIR, f"{name}.txt")
        with open(file_path, "w") as f:
            f.write(df.to_string(index=False))
        print(f"✔ Saved query '{name}' to {file_path}")

    conn.close()
    print("All analytics validation queries executed successfully.")
