# ==================== etl_utils.py ================================================
# The utilities contained therein are used by dag_etl_master.py
# This module provides utility functions for ETL processes
# specifically for downloading, cleaning, and loading datasets
# etl_utils.py well formatted and self-documented.

from psycopg2.extras import execute_values
from psycopg2.extras import execute_batch
import psycopg2.extras
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
    "host": "postgres",
    "port": "5432",
    "database": "data_db",
    "dbname": "data_db",
    "user": "data_user",
    "password": "root",

    # postgres tables
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
# DOWNLOAD UTILITIES HELPERS_download_csv(URL, filename):
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
        df = pd.read_csv(url, sep=";", skiprows=7, encoding="latin1")
        print(f"[DEBUG] CSV loaded with latin1 encoding: {len(df)} rows")
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
    """
    Downloads a ZIP from `zip_url` and extracts the first CSV inside it.
    Always writes the CSV to DATA_DIR as 'workaccidents.csv'.
    Returns the full path to the CSV.
    """
    url = zip_url
    filename = os.path.basename(url)
    zip_path = os.path.join(DATA_DIR, filename)

    # Ensure DATA_DIR exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # Download the ZIP if not already present
    if not os.path.exists(zip_path):
        print(f"[INFO] Downloading ZIP from {url} → {zip_path}")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(r.content)
    else:
        print(f"[INFO] Using existing ZIP file: {zip_path}")

    # Extract the first CSV
    with ZipFile(zip_path, "r") as zf:
        csv_files = [f for f in zf.namelist() if f.lower().endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError(f"No CSV found in ZIP: {zip_path}")

        csv_name_in_zip = csv_files[0]
        csv_path = os.path.join(DATA_DIR, "workaccidents.csv")  # <-- always this name

        with zf.open(csv_name_in_zip) as f:
            csv_data = f.read().decode("utf-8", errors="ignore")
            with open(csv_path, "w", encoding="utf-8") as out_f:
                out_f.write(csv_data)

    print(f"[INFO] Extracted CSV from ZIP: {csv_name_in_zip} → {csv_path}")
    return csv_path


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

def create_fatalities_csv():
    """
    Merge all 9 fatalities source CSVs into one fatalities.csv.
    Drops the 2nd row (index 1) only once for files 1–4.
    Preserves all other columns and rows.
    Returns the path to the merged CSV.
    """
    sources = [
        ("fatalities_1.csv", True),
        ("fatalities_2.csv", True),
        ("fatalities_3.csv", True),
        ("fatalities_4.csv", True),
        ("fatalities_5.csv", False),
        ("fatalities_6.csv", False),
        ("fatalities_7.csv", False),
        ("fatalities_8.csv", False),
        ("fatalities_9.csv", False),
    ]

    dfs = []

    for fname, drop_second_row in sources:
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            print(f"[WARN] Source file not found: {path}")
            continue

        # robust read to handle encoding issues
        df = read_csv_safe(path)

        # Drop only the 2nd row (index 1) once for files 1–4
        if drop_second_row and len(df) > 1:
            df = df.drop(df.index[1])

        # Rename company_city_state_zip column if exists
        if "Company, City, State, ZIP" in df.columns:
            df = df.rename(columns={"Company, City, State, ZIP": "employer_address"})

        dfs.append(df)

    if not dfs:
        raise RuntimeError("No source fatalities files found!")

    # Merge all source DataFrames
    final_df = pd.concat(dfs, ignore_index=True)

    # Write merged CSV
    fatalities_csv_path = os.path.join(DATA_DIR, "fatalities.csv")
    final_df.to_csv(fatalities_csv_path, index=False)
    print(f"[INFO] fatalities.csv created: {fatalities_csv_path} ({len(final_df)} rows)")

    return fatalities_csv_path


def create_fatalities_clean():
    print("=== Cleaning fatalities files ===")

    # --- STEP 1: Build merged fatalities.csv ---
    fatalities_csv_path = create_fatalities_csv()
    print(f"=== Merged fatalities CSV created: {fatalities_csv_path} ===")

    # --- STEP 2: Load merged CSV into Postgres as raw table ---
    merged_df = pd.read_csv(fatalities_csv_path)

    conn = pg_connect()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS fatalities")

    col_defs = ", ".join([f'"{c}" TEXT' for c in merged_df.columns])
    cur.execute(f"CREATE TABLE fatalities ({col_defs})")

    insert_sql = f"""
        INSERT INTO fatalities ({", ".join([f'"{c}"' for c in merged_df.columns])})
        VALUES ({", ".join(["%s"] * len(merged_df.columns))})
    """

    for _, r in merged_df.iterrows():
        cur.execute(insert_sql, [None if pd.isna(v) else str(v) for v in r.values])

    conn.commit()
    conn.close()
    print(f"Raw fatalities table created ({len(merged_df)} rows)")

    # --- STEP 3: Build fatalities_clean as before ---
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

    dfs_clean = []
    for fname, fn in cleaners:
        src = os.path.join(DATA_DIR, fname)
        dst = os.path.join(DATA_DIR, fname.replace(".csv", "_clean.csv"))
        if not os.path.exists(src):
            continue
        fn(src, dst)
        dfs_clean.append(pd.read_csv(dst))

    if not dfs_clean:
        raise RuntimeError("No fatalities files cleaned")

    final_df_clean = pd.concat(dfs_clean, ignore_index=True)

    final_df_clean = normalize_country(final_df_clean)
    final_df_clean.to_csv(os.path.join(DATA_DIR, "fatalities_clean.csv"), index=False)
    load_fatalities_to_postgres()
    print(f"Fatalities clean table created ({len(final_df_clean)} rows)")


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

    # 1. Rename and convert date to string YYYY-MM-DD
    if "incident_date" in df.columns:
        df["date"] = pd.to_datetime(
            df["incident_date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        df.drop(columns=["incident_date"], inplace=True)

    # 2. Normalize country
    if "country" in df.columns:
        df = normalize_country(df, "country")
    else:
        df["country"] = "UNKNOWN"

    # 3. Convert IDs (IMPORTANT FIX)
    # aria_id is numeric
    if "aria_id" in df.columns:
        df["aria_id"] = pd.to_numeric(
            df["aria_id"], errors="coerce"
        ).astype("Int64")

    # industry_code MUST remain TEXT
    if "industry_code" in df.columns:
        df["industry_code"] = (
            df["industry_code"]
            .astype(str)
            .str.strip()
            .replace({"nan": None})
        )

    # 4. Add fatality flag
    df["fatality"] = df.get("hazard_class", "").apply(
        lambda x: 1 if pd.notna(x) and str(x).strip() != "" else 0
    )

    # 5. Drop rows where all key columns are null
    key_cols = [
        "aria_id",
        "date",
        "country",
        "department",
        "municipality",
        "hazard_class",
        "industry_code",
    ]
    existing_keys = [c for c in key_cols if c in df.columns]
    df = df.dropna(how="all", subset=existing_keys)

    # 6. Drop rows with only PK populated
    non_pk_cols = [c for c in existing_keys if c != "aria_id"]
    if non_pk_cols:
        df = df[df[non_pk_cols].notna().any(axis=1)]

    # 7. Deduplicate on PK
    if "aria_id" in df.columns:
        df = df.drop_duplicates(subset=["aria_id"])

    # 8. Recreate prep table with correct types
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')

    col_defs = []
    for c in df.columns:
        if c == "aria_id":
            col_defs.append(f'"{c}" BIGINT')
        elif c == "fatality":
            col_defs.append(f'"{c}" INT')
        else:
            col_defs.append(f'"{c}" TEXT')

    pk = ", PRIMARY KEY (aria_id)" if "aria_id" in df.columns else ""
    cursor.execute(
        f'CREATE TABLE "{dst_table}" ({", ".join(col_defs)}{pk});'
    )

    # 9. Insert data
    insert_sql = f"""
        INSERT INTO "{dst_table}" ({", ".join([f'"{c}"' for c in df.columns])})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """

    for _, row in df.iterrows():
        cursor.execute(
            insert_sql,
            [None if pd.isna(v) else v for v in row.values],
        )

    conn.commit()
    conn.close()

    print(f" Created {dst_table} ({len(df)} rows)")

    
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
# DAG_DATA_CREATE_STAR_SCHEMA FUNCTIONS:  1. drop_dimensions, 2. drop_fact, 3. create_dimensions, 4. populate_dimensions
# 5. create_fact, 6.populate_fact, 7. min_test_star_schema, 8. full_test_star_schema
# for star schema creation and testing
# ==========================================================================================================
# ==========================================================================================================
# DAG_DATA_CREATE_STAR_SCHEMA FUNCTIONS:  1. drop_dimensions
# Drops all star schema dimension tables if they exist.
# ==========================================================================================================

def drop_dimensions(*args, **kwargs):
    """
    Drops all star schema dimension tables if they exist.
    """
    conn = pg_connect()
    cur = conn.cursor()
    tables = ["dim_country", "dim_date", "dim_fatality", "dim_industry",
              "dim_accident_type", "dim_hazard", "dim_employer",
              "country_synonym", "original_dim_location"]
    for t in tables:
        cur.execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
    conn.commit()
    conn.close()
    
# ==========================================================================================================
# DAG_DATA_CREATE_STAR_SCHEMA FUNCTIONS:  2. drop_fact
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
# DAG_DATA_CREATE_STAR_SCHEMA FUNCTIONS:  3. create_dimensions
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
    # dim_fatality
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_fatality (
            fatality_id SERIAL PRIMARY KEY,
            no_of_fatality INT UNIQUE NOT NULL,
            fatality_label TEXT NOT NULL
        )
    """)


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
    # dim_industry
    # ----------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_industry (
            industry_id SERIAL PRIMARY KEY,
            industry_code TEXT NOT NULL UNIQUE
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
# DAG_DATA_CREATE_STAR_SCHEMA FUNCTIONS:  4. populate_dimensions
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
            print(f"No column '{col}' found in prep tables, skipping {table}")
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
    # Populate dim_fatality
    # ----------------------------
    cur.execute("""
        INSERT INTO dim_fatality (no_of_fatality, fatality_label)
        VALUES (1, 'ACCIDENT')
        ON CONFLICT (no_of_fatality) DO NOTHING
    """)
    conn.commit()
    print(" Populated dim_fatality with 1 row (ACCIDENT)")

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
            (pd.to_datetime(d).date(),)
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
        "AFGHANISTAN": "AF", "AFRIQUE DU SUD": "ZA", "ALBANIE": "AL", "ALGERIE": "DZ", "ALLEMAGNE": "DE",
        "ANDORRE": "AD", "ANGOLA": "AO", "ARABIE SAOUDITE": "SA", "ARGENTINE": "AR", "ARMENIE": "AM",
        "AUSTRALIE": "AU", "AUTRE": "ZZ", "AUTRICHE": "AT", "AZERBAIDJAN": "AZ", "BAHAMAS": "BS",
        "BANGLADESH": "BD", "BELGIQUE": "BE", "BENIN": "BJ", "BIELORUSSIE": "BY", "BIRMANIE": "MM",
        "BOLIVIE": "BO", "BOSNIE-HERZEGOVINE": "BA", "BRESIL": "BR", "BULGARIE": "BG", "BURKINA FASO": "BF",
        "BURUNDI": "BI", "CAMBODGE": "KH", "CAMEROUN": "CM", "CANADA": "CA", "CHILI": "CL", "CHINE": "CN",
        "CHYPRE": "CY", "COLOMBIE": "CO", "CONGO (REP.)": "CG", "COREE DU NORD": "KP", "COREE DU SUD": "KR",
        "COSTA RICA": "CR", "COTE D'IVOIRE": "CI", "CROATIE": "HR", "CUBA": "CU", "DANEMARK": "DK", "DJIBOUTI": "DJ",
        "DOMINICAINE (REP.)": "DO", "EGYPTE": "EG", "EMIRATS ARABES UNIS": "AE", "EQUATEUR": "EC", "ESPAGNE": "ES",
        "ESTONIE": "EE", "ETHIOPIE": "ET", "FINLANDE": "FI", "FRANCE": "FR", "GABON": "GA", "GEORGIE": "GE",
        "GHANA": "GH", "GRECE": "GR", "GUATEMALA": "GT", "GUINEE": "GN", "GUINEE EQUATORIALE": "GQ", "GUYANA": "GY",
        "HAITI": "HT", "HONDURAS": "HN", "HONGRIE": "HU", "ILES SALOMON": "SB", "INDE": "IN", "INDONESIE": "ID",
        "IRAK": "IQ", "IRAN": "IR", "IRLANDE": "IE", "ISLANDE": "IS", "ISRAEL": "IL", "ITALIE": "IT", "JAMAIQUE": "JM",
        "JAPON": "JP", "JORDANIE": "JO", "KAZAKHSTAN": "KZ", "KENYA": "KE", "KIRGHIZSTAN": "KG", "KOWEIT": "KW", "LAOS": "LA",
        "LETTONIE": "LV", "LIBAN": "LB", "LIBYE": "LY", "LITUANIE": "LT", "LUXEMBOURG": "LU", "MACEDOINE (EX YOUGOSLAVIE)": "MK",
        "MADAGASCAR": "MG", "MALAISIE": "MY", "MALTE": "MT", "MAROC": "MA", "MAURICE": "MU", "MAURITANIE": "MR", "MEXIQUE": "MX",
        "MONACO": "MC", "MONGOLIE": "MN", "MOZAMBIQUE": "MZ", "NC": "NC", "NIGER": "NE", "NIGERIA": "NG", "NICARAGUA": "NI",
        "NORVEGE": "NO", "NOUVELLE-ZELANDE": "NZ", "OMAN": "OM", "OUGANDA": "UG", "OUZBEKISTAN": "UZ", "PAKISTAN": "PK",
        "PANAMA": "PA", "PAPOUASIE-NOUVELLE-GUINEE": "PG", "PAYS-BAS": "NL", "PEROU": "PE", "PHILIPPINES": "PH", "POLOGNE": "PL",
        "PORTO RICO": "PR", "PORTUGAL": "PT", "QATAR": "QA", "ROUMANIE": "RO", "RUSSIE": "RU", "RWANDA": "RW", "SAINTE-LUCIE": "LC",
        "SALVADOR": "SV", "SENEGAL": "SN", "SERBIE": "RS", "SERBIE-ET-MONTENEGRO": "CS", "SEYCHELLES": "SC", "SIERRA LEONE": "SL",
        "SINGAPOUR": "SG", "SLOVAQUIE": "SK", "SLOVENIE": "SI", "SOUDAN": "SD", "SRI LANKA": "LK", "SUISSE": "CH", "SURINAME": "SR",
        "SUEDE": "SE", "SYRIE": "SY", "TAIWAN": "TW", "TANZANIE": "TZ", "TCHEQUE (REP.)": "CZ", "THAILANDE": "TH", "TOGO": "TG",
        "TRINITE-ET-TOBAGO": "TT", "TUNISIE": "TN", "TURQUIE": "TR", "UK": "GB", "UKRAINE": "UA", "UNKNOWN": "XX", "URUGUAY": "UY",
        "USA": "US", "VIETNAM": "VN", "YEMEN": "YE", "ZAMBIE": "ZM"
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
# DAG_DATA_CREATE_STAR_SCHEMA FUNCTIONS:  5. create_fact
# Creates the fact_accidents table for the star schema (with no data).
# =========================================================================================================

def create_fact(*args, **kwargs):
    """
    Creates the fact_accidents table for the star schema.

    Columns:
    - fact_id: surrogate key (BIGSERIAL)
    - date_id, country_id, fatality_id: mandatory FKs
    - industry_id, accident_type_id, hazard_id, employer_id: optional FKs
    - no_of_fatality: default 1 for each row
    """
    conn = pg_connect()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_accidents (
            fact_id BIGSERIAL PRIMARY KEY,          -- surrogate key
            date_id BIGINT NOT NULL,                -- FK to dim_date
            country_id BIGINT NOT NULL,             -- FK to dim_country
            fatality_id BIGINT NOT NULL,            -- FK to dim_fatality
            industry_id BIGINT NULL,                -- FK to dim_industry (optional)
            accident_type_id BIGINT NULL,           -- FK to dim_accident_type (optional)
            hazard_id BIGINT NULL,                  -- FK to dim_hazard (optional)
            employer_id BIGINT NULL,                -- FK to dim_employer (optional)
            no_of_fatality BIGINT NOT NULL DEFAULT 1
        )
    """)

    conn.commit()
    conn.close()
    print("✔ fact_accidents table created successfully")


# ==========================================================================================================
# DAG_DATA_CREATE_STAR_SCHEMA FUNCTIONS:  6. populate_fact
# Populates the fact_accidents table from prep tables and dimension tables.
# =========================================================================================================

def populate_fact(*args, **kwargs):
    """
    Populate fact_accidents table from prep tables using dimension tables.
    Step 0: dynamically unify prep tables into a CTE prep_data
    Step 1: insert full data with optional FKs
    Step 2: fallback: insert only mandatory FKs
    """
    conn = pg_connect()
    cur = conn.cursor()

    # Define prep tables and optional columns
    prep_tables = ["ariadb_prep", "workaccidents_prep", "fatalities_prep"]
    optional_cols = ["industry_code", "accident_type", "hazard_class", "employer"]
    mandatory_cols = ["date", "country", "fatality"]

    try:
        # ----------------------------
        # Step 0: dynamic union CTE with normalized employer
        # ----------------------------
        cte_parts = []
        for table in prep_tables:
            # Get actual columns in the table
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table}' 
            """)
            table_cols = [row[0] for row in cur.fetchall()]

            select_parts = []
            for col in mandatory_cols + optional_cols:
                if col in table_cols:
                    if col == "employer":
                        # normalize employer text if exists
                        select_parts.append(f"TRIM(UPPER({col})) AS employer")
                    else:
                        select_parts.append(col)
                else:
                    select_parts.append(f"NULL AS {col}")
            cte_parts.append(f"SELECT {', '.join(select_parts)} FROM {table}")

        prep_union_sql = "WITH prep_data AS (\n" + "\nUNION ALL\n".join(cte_parts) + "\n)\n"

        # ----------------------------
        # Step 0a: debug counts
        # ----------------------------
        debug_sql = prep_union_sql + f"""
        SELECT
            COUNT(*) FILTER (WHERE industry_code IS NULL) AS missing_industry,
            COUNT(*) FILTER (WHERE accident_type IS NULL) AS missing_accident_type,
            COUNT(*) FILTER (WHERE hazard_class IS NULL) AS missing_hazard,
            COUNT(*) FILTER (WHERE employer IS NULL) AS missing_employer,
            COUNT(*) AS total_rows
        FROM prep_data
        """
        cur.execute(debug_sql)
        missing_counts = cur.fetchone()
        print("Prep table NULL summary (optional FKs):")
        print(f"  missing_industry: {missing_counts[0]}")
        print(f"  missing_accident_type: {missing_counts[1]}")
        print(f"  missing_hazard: {missing_counts[2]}")
        print(f"  missing_employer: {missing_counts[3]}")
        print(f"  total_rows: {missing_counts[4]}")

        # ----------------------------
        # Step 1: full insert
        # ----------------------------
        insert_sql = prep_union_sql + """
        INSERT INTO fact_accidents (
            date_id, country_id, fatality_id,
            industry_id, accident_type_id, hazard_id, employer_id,
            no_of_fatality
        )
        SELECT
            d.date_id,
            c.country_id,
            f.fatality_id,
            i.industry_id,
            atype.accident_type_id,
            h.hazard_id,
            e.employer_id,
            COALESCE(p.fatality, 1) AS no_of_fatality
        FROM prep_data p
        JOIN dim_date d
            ON d.date = p.date::date
        JOIN dim_country c
            ON c.country_name = p.country
        JOIN dim_fatality f
            ON f.no_of_fatality = 1
        LEFT JOIN dim_industry i
            ON i.industry_code = p.industry_code
        LEFT JOIN dim_accident_type atype
            ON atype.accident_type = p.accident_type
        LEFT JOIN dim_hazard h
            ON h.hazard_class = p.hazard_class
        LEFT JOIN dim_employer e
            ON e.employer = p.employer
        """
        cur.execute(insert_sql)
        conn.commit()
        print(" fact_accidents populated successfully (full)")

    except Exception as e:
        conn.rollback()
        print(" Full insert failed, falling back to mandatory FKs only:", e)

        # ----------------------------
        # Step 2: fallback: only mandatory FKs
        # ----------------------------
        insert_mandatory_sql = prep_union_sql + """
        INSERT INTO fact_accidents (
            date_id, country_id, fatality_id, no_of_fatality
        )
        SELECT
            d.date_id,
            c.country_id,
            f.fatality_id,
            COALESCE(p.fatality, 1) AS no_of_fatality
        FROM prep_data p
        JOIN dim_date d
            ON d.date = p.date::date
        JOIN dim_country c
            ON c.country_name = p.country
        JOIN dim_fatality f
            ON f.no_of_fatality = 1
        """
        cur.execute(insert_mandatory_sql)
        conn.commit()
        print(" fact_accidents populated with mandatory FKs only")

    finally:
        conn.close()
        print(" Connection closed")


# ==========================================================================================================
# DAG_DATA_CREATE_STAR_SCHEMA FUNCTIONS:  7. min_test_star_schema
# Minimal test of star schema functions.
#  ==========================================================================================================

def min_test_star_schema(*args, **kwargs):
    """
    Select top 5 rows from fact_accidents to ensure foreign keys exist.
    Updated for current schema: country_id instead of location_id.
    """
    conn = pg_connect()
    query = """
        SELECT date_id, country_id, employer_id, hazard_id,
               accident_type_id, industry_id, no_of_fatality AS fatality
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


def full_test_star_schema(*args, **kwargs):
    """
    Select top 20 rows from fact_accidents with joins to all dimensions.
    Ensures that all dimension references exist and the schema is correct.
    """

    conn = pg_connect()

    try:
        query = """
        SELECT 
            f.date_id, d.date AS date_value,
            f.country_id, c.country_name,
            f.employer_id, e.employer AS employer_name,
            f.hazard_id, h.hazard_class AS hazard_name,
            f.accident_type_id, a.accident_type AS accident_type_name,
            f.industry_id, i.industry_code AS industry_code,
            f.no_of_fatality
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

        print(" full_test_star_schema query returned:")
        print(df_test.head(20))
        print(f"Total rows returned: {len(df_test)}")

        # Debug: data types and ranges
        print("\nData types:")
        print(df_test.dtypes)
        print("\nNumeric ranges for IDs and fatality:")
        for col in ['date_id', 'country_id', 'industry_id', 'accident_type_id', 'hazard_id', 'employer_id', 'no_of_fatality']:
            if col in df_test.columns:
                print(f"{col}: min={df_test[col].min()}, max={df_test[col].max()}")

        return df_test

    finally:
        conn.close()
        print(" Connection closed")

# ==========================================================================================================
# DAG_DATA_ANALYTICS_VALIDATION FUNCTIONS:  1. run_analytics_validation
# Run predefined analytics validation queries and save outputs.
#= =========================================================================================================

"""
run_analytics_validation.py

Runs a full set of analytics validation queries on the current star schema.
Outputs deterministic, auditable result files including:
- Query title
- Query SQL
- Query results

No regression overwrites. Uses current dimension and fact table schemas.
"""
def run_analytics_validation():
    """
    Run a full set of analytics queries on the current star schema.
    Uses current dimension and fact table columns and types.
    """

    # 1. Open DB connection
    conn = pg_connect()

    try:
        # 2. Log database connection identity
        identity_df = pd.read_sql(
            """
            SELECT
                current_user,
                session_user,
                current_database(),
                current_schema();
            """,
            conn
        )
        print(" Database connection identity:")
        print(identity_df.to_string(index=False))

        # 3. Force schema visibility
        cur = conn.cursor()
        cur.execute("SET search_path TO public;")
        cur.close()

        # 4. Validate dim_date exists and has data
        dim_check = pd.read_sql(
            "SELECT COUNT(*) AS row_count FROM dim_date;",
            conn
        )

        if dim_check.loc[0, "row_count"] == 0:
            raise RuntimeError(
                " dim_date is visible but empty — analytics validation aborted."
            )

        print(" dim_date table is visible and populated. Proceeding with analytics.")

        # 5. Define analytics queries
        queries = {
            "1_fatalities_by_year": """
                SELECT
                    d.date AS year,
                    SUM(f.no_of_fatality) AS total_fatalities
                FROM fact_accidents f
                JOIN dim_date d ON f.date_id = d.date_id
                GROUP BY d.date
                ORDER BY d.date
            """,

            "2_fatalities_by_country": """
                SELECT
                    c.country_name AS country,
                    SUM(f.no_of_fatality) AS total_fatalities
                FROM fact_accidents f
                JOIN dim_country c ON f.country_id = c.country_id
                GROUP BY c.country_name
                ORDER BY total_fatalities DESC
            """,

            "3_fatalities_by_industry": """
                SELECT
                    i.industry_code AS industry,
                    SUM(f.no_of_fatality) AS total_fatalities
                FROM fact_accidents f
                JOIN dim_industry i ON f.industry_id = i.industry_id
                GROUP BY i.industry_code
                ORDER BY total_fatalities DESC
            """,

            "4_france_vs_usa_last_10_years": """
                SELECT
                    d.date AS year,
                    SUM(CASE WHEN c.country_name = 'FRANCE'
                             THEN f.no_of_fatality ELSE 0 END) AS france,
                    SUM(CASE WHEN c.country_name = 'USA'
                             THEN f.no_of_fatality ELSE 0 END) AS usa
                FROM fact_accidents f
                JOIN dim_country c ON f.country_id = c.country_id
                JOIN dim_date d ON f.date_id = d.date_id
                WHERE d.date >= (CURRENT_DATE - INTERVAL '10 years')
                GROUP BY d.date
                ORDER BY d.date
            """,

            "5_top_10_countries_fatalities": """
                SELECT
                    c.country_name,
                    SUM(f.no_of_fatality) AS total_fatalities
                FROM fact_accidents f
                JOIN dim_country c ON f.country_id = c.country_id
                JOIN dim_date d ON f.date_id = d.date_id
                GROUP BY c.country_name
                ORDER BY total_fatalities DESC
                LIMIT 10
            """,

            "6_top_10_industries_fatalities": """
                SELECT
                    i.industry_code,
                    SUM(f.no_of_fatality) AS total_fatalities
                FROM fact_accidents f
                JOIN dim_industry i ON f.industry_id = i.industry_id
                JOIN dim_country c ON f.country_id = c.country_id
                JOIN dim_date d ON f.date_id = d.date_id
                GROUP BY i.industry_code
                ORDER BY total_fatalities DESC
                LIMIT 10
            """,

            "7_top_10_employers_fatalities": """
                SELECT
                    e.employer,
                    SUM(f.no_of_fatality) AS total_fatalities
                FROM fact_accidents f
                JOIN dim_employer e ON f.employer_id = e.employer_id
                JOIN dim_country c ON f.country_id = c.country_id
                JOIN dim_date d ON f.date_id = d.date_id
                GROUP BY e.employer
                ORDER BY total_fatalities DESC
                LIMIT 10
            """,

            "8_recent_10_year_fatalities_by_country": """
                SELECT
                    c.country_name,
                    SUM(f.no_of_fatality) AS total_fatalities
                FROM fact_accidents f
                JOIN dim_country c ON f.country_id = c.country_id
                JOIN dim_date d ON f.date_id = d.date_id
                WHERE d.date >= (CURRENT_DATE - INTERVAL '10 years')
                GROUP BY c.country_name
                ORDER BY total_fatalities DESC
                LIMIT 10
            """,

            "9_fatalities_trend_by_industry_last_10_years": """
                SELECT
                    d.date AS year,
                    i.industry_code,
                    SUM(f.no_of_fatality) AS total_fatalities
                FROM fact_accidents f
                JOIN dim_industry i ON f.industry_id = i.industry_id
                JOIN dim_date d ON f.date_id = d.date_id
                JOIN dim_country c ON f.country_id = c.country_id
                WHERE d.date >= (CURRENT_DATE - INTERVAL '10 years')
                GROUP BY d.date, i.industry_code
                ORDER BY d.date, total_fatalities DESC
                
            """
        }

        # 6. Execute analytics queries and save results
        ANALYTICS_DATA_DIR = "/opt/airflow/data/analytics_results"
        os.makedirs(ANALYTICS_DATA_DIR, exist_ok=True)

        for name, query in queries.items():
            df = pd.read_sql(query, conn)
            file_path = os.path.join(ANALYTICS_DATA_DIR, f"{name}.txt")

            with open(file_path, "w") as f:
                f.write(f"Query Title:\n{name}\n\n")
                f.write("Query SQL:\n")
                f.write("-" * 50 + "\n")
                f.write(query.strip() + "\n\n")
                f.write("Query Results:\n")
                f.write("=" * 50 + "\n")
                f.write(df.to_string(index=False))

            print(f" Saved analytics result: {file_path}")

        print(" All analytics validation queries executed successfully.")

    finally:
        # 7. Close connection safely
        conn.close()
