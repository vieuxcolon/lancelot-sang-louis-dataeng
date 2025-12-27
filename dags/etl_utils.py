# ==================== etl_utils.py =================================
# The utilities contained therein are used by dag_etl_master.py
# This module provides utility functions for ETL processes
# specifically for downloading, cleaning, and loading datasets

# etl_utils.py well formatted and self-documented.

import sys
import traceback
import csv
from typing import List, Dict
import os
import io
from io import StringIO, BytesIO
import pandas as pd
import psycopg2
import requests
from zipfile import ZipFile
from datetime import datetime
import warnings
import logging
from time import sleep
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv
from datetime import datetime


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

    # Tables (unchanged — no regression)
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

# DATA_DIR is shared across containers via Docker volume
DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")  # Default if not explicitly set

# ============================================================================
# MongoDB connection helper
# Centralized, authenticated MongoDB client creation with connectivity check
# ============================================================================
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

# ============================================================================
# MongoDB → CSV export utility
# Reads an entire MongoDB collection and writes it to a CSV file
# ============================================================================

def export_mongo_to_csv(db_name: str, collection_name: str, output_file: str):
    client = get_mongo_client()
    db = client[db_name]
    try:
        print(
            f"[DEBUG] Exporting MongoDB collection "
            f"'{db_name}.{collection_name}' to CSV '{output_file}'"
        )
        cursor = db[collection_name].find()
        df = pd.DataFrame(list(cursor))

        # Remove MongoDB internal ID field
        if "_id" in df.columns:
            df.drop(columns=["_id"], inplace=True)

        df.to_csv(output_file, index=False)
        print(f"✔ Collection exported successfully to {output_file}, rows: {len(df)}")
    except Exception:
        print("[ERROR] Failed to export MongoDB collection to CSV")
        traceback.print_exc()
    finally:
        client.close()
        print("✔ MongoDB connection closed after export")

# ============================================================================
# Main ETL function
# Source CSV (URL) → MongoDB (atomic load) → CSV on disk
# ============================================================================

def download_ariadb_via_mongo(url: str):
    """
    End-to-end flow:
        Source CSV (URL)
            → MongoDB temp collection
            → Atomic rename to main collection
            → Export back to CSV ($DATA_DIR/ariadb.csv)

    The output CSV filename is fixed by contract.
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
    # Step 3: Load into MongoDB (temp → atomic rename)
    # ------------------------------------------------------------------------
    client = get_mongo_client()
    db = client[MONGO_DB]
    tmp_coll_name = f"{MONGO_COLLECTION}_tmp"
    tmp_coll = db[tmp_coll_name]

    try:
        if tmp_coll_name in db.list_collection_names():
            db.drop_collection(tmp_coll_name)

        tmp_coll.insert_many(df.to_dict(orient="records"))

        if tmp_coll.count_documents({}) != len(df):
            raise RuntimeError("Row count mismatch after Mongo insert")

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
    export_path = os.path.join(DATA_DIR, "ariadb.csv")

    try:
        print(f"[INFO] Exporting MongoDB collection '{MONGO_DB}.{MONGO_COLLECTION}' → '{export_path}'")
        export_mongo_to_csv(MONGO_DB, MONGO_COLLECTION, export_path)
        print(f"[OK] ariadb.csv written to {export_path}")
    except Exception:
        print("[ERROR] MongoDB export to CSV failed")
        traceback.print_exc()
        sys.exit(1)

# ---------------------------------------------------------------------
# Step 1: Download CSV from source
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
# Step 2: Load CSV rows into MongoDB
# ---------------------------------------------------------------------
def load_rows_into_mongo(rows: List[Dict]):
    client = get_mongo_client()
    collection = client[MONGO_DB][MONGO_COLLECTION]

    # Idempotent load
    collection.delete_many({})
    collection.insert_many(rows)

    print(f"Inserted {len(rows)} rows into MongoDB ({MONGO_DB}.{MONGO_COLLECTION})")

# ---------------------------------------------------------------------
# Step 3: Export MongoDB collection to CSV on disk
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

# ---------------------------------------------------------------------
# Public API — REPLACES download_csv
# ---------------------------------------------------------------------

def create_database_and_set_config(db_name: str):
    """
    Create the PostgreSQL database if it doesn't exist and update DB_CONFIG['database']
    so all ETL scripts point to it automatically.

    Args:
        db_name (str): Name of the database to create and use.
    """
    # Connect to default DB first
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database="postgres",  # default DB for initial connection
        port=DB_CONFIG.get("port", 5432)
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Check if database exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
    if not cur.fetchone():
        cur.execute(f"CREATE DATABASE {db_name};")
        print(f"✔ Database '{db_name}' created.")
    else:
        print(f"✔ Database '{db_name}' already exists.")

    # Grant privileges to user
    cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {DB_CONFIG['user']};")
    print(f"✔ Granted all privileges on '{db_name}' to '{DB_CONFIG['user']}'.")

    cur.close()
    conn.close()

    # Update DB_CONFIG to point ETL scripts to the new database
    DB_CONFIG["database"] = db_name
    print(f"✔ DB_CONFIG['database'] updated to '{db_name}' for all ETL scripts.")

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
# Helper function
def normalize_country(df: pd.DataFrame, col: str = "country") -> pd.DataFrame:
    """
    Normalize country names in the dataframe column to standard names.
    Currently maps all USA variants to 'USA'.
    """
    if col not in df.columns:
        df[col] = "USA"  # default if column doesn't exist
        return df

    df[col] = df[col].astype(str).str.strip().str.upper()
    country_map = {
        "USA": "USA",
        "US": "USA",
        "UNITED STATES": "USA",
        "UNITED STATES OF AMERICA": "USA",
        "ETATS-UNIS": "USA",
        "ETATS UNIS": "USA",
        # add more mappings if needed
    }
    df[col] = df[col].replace(country_map)
    return df

# 1️⃣ ARIADB

# =================== etl_utils.py ===================================================================
def create_ariadb_clean():
    """
    1️⃣ Load raw ARIADB CSV from disk
    2️⃣ Apply column mapping, date normalization, deduplication, and country normalization
    3️⃣ Create Postgres clean table ariadb_clean
    """
    src_csv = os.path.join(DATA_DIR, "ariadb.csv")
    dst_table = DB_CONFIG["ariadb_clean_table"]

    print(f"[INFO] Loading ARIADB raw CSV from {src_csv}")
    try:
        df = pd.read_csv(src_csv, sep=";")
        print(f"[INFO] ARIADB raw CSV loaded, {len(df)} rows")
    except Exception:
        print("[ERROR] Failed to read ARIADB CSV")
        traceback.print_exc()
        sys.exit(1)

    # ------------------------------------------------------------------------
    # Column mapping
    # ------------------------------------------------------------------------
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
        "type_évènement": "hazard_class",
    }

    # Keep only existing columns and rename
    existing = [c for c in col_map if c in df.columns]
    df = df[existing].rename(columns={k: col_map[k] for k in existing}).copy()

    # ------------------------------------------------------------------------
    # Normalize date
    # ------------------------------------------------------------------------
    if "incident_date" in df.columns:
        df["incident_date"] = pd.to_datetime(df["incident_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # ------------------------------------------------------------------------
    # Deduplicate on primary key
    # ------------------------------------------------------------------------
    if "aria_id" in df.columns:
        df.drop_duplicates(subset=["aria_id"], inplace=True)

    # ------------------------------------------------------------------------
    # Normalize country
    # ------------------------------------------------------------------------
    df = normalize_country(df, "country")

    # ------------------------------------------------------------------------
    # Create clean table in Postgres
    # ------------------------------------------------------------------------
    conn = pg_connect()
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')
    col_defs = ", ".join([f'"{c}" TEXT' for c in df.columns])
    pk = ", PRIMARY KEY (aria_id)" if "aria_id" in df.columns else ""
    cursor.execute(f'CREATE TABLE "{dst_table}" ({col_defs}{pk});')

    # Insert rows
    insert_sql = f"""
        INSERT INTO "{dst_table}" ({", ".join([f'"{c}"' for c in df.columns])})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """
    for _, row in df.iterrows():
        cursor.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    conn.close()
    print(f"✔ Created {dst_table} ({len(df)} rows)")


# 2️⃣ WORKACCIDENTS

def create_workaccidents_clean():
    """
    ETL function to load raw Workaccidents CSV into Postgres, then clean and create
    the workaccidents_clean table.
    """
    # ----------------------------
    # Step 0: Define paths and tables
    # ----------------------------
    raw_csv_path = os.path.join(DATA_DIR, "workaccidents.csv")
    src_table = DB_CONFIG["workaccidents_table"]
    dst_table = DB_CONFIG["workaccidents_clean_table"]

    # ----------------------------
    # Step 1: Load raw CSV into Postgres
    # ----------------------------
    print(f"[INFO] Loading Workaccidents raw CSV into Postgres: {raw_csv_path}")
    load_to_postgres(raw_csv_path, table_name=src_table, sep=",")
    print(f"✔ Loaded Workaccidents raw table in Postgres from {raw_csv_path}")

    # ----------------------------
    # Step 2: Read raw table
    # ----------------------------
    conn = pg_connect()
    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)
    print(f"[INFO] Loaded {len(df)} rows from raw table '{src_table}'")

    # ----------------------------
    # Step 3: Cleaning & normalization
    # ----------------------------
    # Drop unnecessary column
    if "final_narrative" in df.columns:
        df.drop(columns=["final_narrative"], inplace=True)

    # Normalize accident date
    date_col = "eventdate" if "eventdate" in df.columns else None
    if date_col:
        df["accident_date"] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    else:
        df["accident_date"] = None

    # Normalize country (USA for all workaccidents)
    df["country"] = "USA"

    # Select relevant columns
    selected_columns = [
        "id", "upa", "accident_date", "employer", "address1", "address2",
        "city", "state", "zip", "latitude", "longitude", "primary_naics",
        "hospitalized", "amputation", "loss_of_eye", "inspection", "nature",
        "naturetitle", "part_of_body", "part_of_body_title", "event",
        "eventtitle", "source", "sourcetitle", "secondary_source",
        "secondary_source_title", "federalstate", "country",
    ]
    df_clean = df[[c for c in selected_columns if c in df.columns]].copy()

    # Deduplicate by 'upa'
    if "upa" in df_clean.columns:
        df_clean = df_clean.drop_duplicates(subset=["upa"])

    # ----------------------------
    # Step 4: Create clean table in Postgres
    # ----------------------------
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')

    col_defs = ", ".join([f'"{c}" TEXT' for c in df_clean.columns])
    cursor.execute(f'CREATE TABLE "{dst_table}" ({col_defs});')

    # Insert rows
    insert_sql = f"""
        INSERT INTO "{dst_table}" ({", ".join([f'"{c}"' for c in df_clean.columns])})
        VALUES ({", ".join(["%s"] * len(df_clean.columns))})
    """
    for _, row in df_clean.iterrows():
        cursor.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    conn.close()
    print(f"✔ Created workaccidents_clean ({len(df_clean)} rows)")


# 3️⃣ FATALITIES
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

    # Normalize country
    final_df = normalize_country(final_df, "country")

    final_path = os.path.join(DATA_DIR, "fatalities_clean.csv")
    final_df.to_csv(final_path, index=False)
    print(f"✔ Final merge written: {final_path} ({len(final_df)} rows)")

    load_fatalities_to_postgres()
    print("✔ fatalities_clean loaded into Postgres")

    
def normalize_country(df, column_name):
    """
    Normalize country values in a DataFrame column.
    Converts None/NaN to 'UNKNOWN', strips whitespace, uppercases,
    normalizes accents, and maps common synonyms to canonical names.
    """
    if column_name not in df.columns:
        return df
    
    def normalize_value(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "UNKNOWN"
        s = str(value).strip().upper()
        # Normalize accents
        s = s.replace("É", "E").replace("È", "E").replace("Ê", "E")
        # Map common synonyms
        if s in {"USA", "US", "UNITED STATES", "ETATS-UNIS", "ETATS UNIS", "ETATSUNIS"}:
            return "USA"
        if s in {"UK", "UNITED KINGDOM", "ROYAUME-UNI"}:
            return "UK"
        if s == "FRANCE":
            return "FRANCE"
        if s in {"GERMANY", "ALLEMAGNE"}:
            return "GERMANY"
        if s == "CANADA":
            return "CANADA"
        return s

    df[column_name] = df[column_name].apply(normalize_value)
    return df

def create_ariadb_prep():
    src_table = DB_CONFIG["ariadb_clean_table"]
    dst_table = DB_CONFIG["ariadb_prep_table"]
    conn = pg_connect()
    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)

    # 1️⃣ Rename date
    if "incident_date" in df.columns:
        df["date"] = pd.to_datetime(df["incident_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df.drop(columns=["incident_date"], inplace=True)

    # 2️⃣ Normalize country
    if "country" in df.columns:
        df = normalize_country(df, "country")

    # 3️⃣ Add fatality flag based on hazard_class
    if "hazard_class" in df.columns:
        df["fatality"] = df["hazard_class"].apply(
            lambda x: 1 if pd.notna(x) and str(x).strip() != "" else 0
        )
    else:
        df["fatality"] = 0

    # 4️⃣ Drop rows where all key columns are null
    key_cols = ["aria_id","date","country","department","municipality","hazard_class","industry_code"]
    existing_keys = [c for c in key_cols if c in df.columns]
    if existing_keys:
        df = df.dropna(how="all", subset=existing_keys)

    # 5️⃣ Drop rows with only PK populated
    non_pk_cols = [c for c in existing_keys if c != "aria_id"]
    if non_pk_cols:
        df = df[df[non_pk_cols].notna().any(axis=1)]

    # 6️⃣ Deduplicate on PK
    if "aria_id" in df.columns:
        df = df.drop_duplicates(subset=["aria_id"])

    # 7️⃣ Create prep table
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')
    col_defs = ", ".join([f'"{c}" TEXT' for c in df.columns])
    pk = ", PRIMARY KEY (aria_id)" if "aria_id" in df.columns else ""
    cursor.execute(f'CREATE TABLE "{dst_table}" ({col_defs}{pk});')

    # 8️⃣ Insert data
    insert_sql = f"""
        INSERT INTO "{dst_table}" ({", ".join([f'"{c}"' for c in df.columns])})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """
    for _, row in df.iterrows():
        cursor.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    conn.close()
    print(f"✔ Created ariadb_prep ({len(df)} rows)")


def create_workaccidents_prep():
    src_table = DB_CONFIG["workaccidents_clean_table"]
    dst_table = DB_CONFIG["workaccidents_prep_table"]
    conn = pg_connect()
    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)

    # 1️⃣ Rename date
    if "accident_date" in df.columns:
        df["date"] = pd.to_datetime(df["accident_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df.drop(columns=["accident_date"], inplace=True)

    # 2️⃣ Normalize country (default USA)
    if "country" not in df.columns:
        df["country"] = "USA"
    df = normalize_country(df, "country")

    # 3️⃣ Add fatality
    df["fatality"] = df["naturetitle"].apply(lambda x: 1 if pd.notna(x) and str(x).strip() != "" else 0) if "naturetitle" in df.columns else 0

    # 4️⃣ Drop rows where all key columns are null
    key_cols = ["upa","id","date","country","employer","city","state","primary_naics","event","naturetitle"]
    existing_keys = [c for c in key_cols if c in df.columns]
    if existing_keys:
        df = df.dropna(how="all", subset=existing_keys)

    # 5️⃣ Drop rows with only PK populated
    non_pk_cols = [c for c in existing_keys if c != "upa"]
    if non_pk_cols:
        df = df[df[non_pk_cols].notna().any(axis=1)]

    # 6️⃣ Deduplicate on PK
    if "upa" in df.columns:
        df = df.drop_duplicates(subset=["upa"])

    # 7️⃣ Create prep table
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')
    col_defs = ", ".join([f'"{c}" TEXT' for c in df.columns])
    pk = ", PRIMARY KEY (upa)" if "upa" in df.columns else ""
    cursor.execute(f'CREATE TABLE "{dst_table}" ({col_defs}{pk});')

    # 8️⃣ Insert data
    insert_sql = f"""
        INSERT INTO "{dst_table}" ({", ".join([f'"{c}"' for c in df.columns])})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """
    for _, row in df.iterrows():
        cursor.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    conn.close()
    print(f"✔ Created workaccidents_prep ({len(df)} rows)")


def create_fatalities_prep():
    src_table = DB_CONFIG["fatalities_clean_table"]
    dst_table = DB_CONFIG["fatalities_prep_table"]
    conn = pg_connect()
    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)

    # 1️⃣ Rename date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # 2️⃣ Normalize country
    if "country" not in df.columns:
        df["country"] = "UNKNOWN"
    df = normalize_country(df, "country")

    # 3️⃣ Add fatality
    if "no_of_fatalities" in df.columns:
        df["fatality"] = df["no_of_fatalities"].apply(lambda x: 1 if pd.notna(x) and x > 0 else 0)
    else:
        df["fatality"] = 1

    # 4️⃣ Drop rows where all key columns are null
    key_cols = ["fatality_id","date","country","fatality"]
    df = df.dropna(how="all", subset=[c for c in key_cols if c in df.columns])

    # 5️⃣ Drop rows with only PK populated
    non_pk_cols = [c for c in key_cols if c != "fatality_id" and c in df.columns]
    if non_pk_cols:
        df = df[df[non_pk_cols].notna().any(axis=1)]

    # 6️⃣ Deduplicate on PK
    if "fatality_id" in df.columns:
        df = df.drop_duplicates(subset=["fatality_id"])

    # 7️⃣ Create prep table
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')
    col_defs = ", ".join([f'"{c}" TEXT' for c in df.columns])
    pk = ", PRIMARY KEY (fatality_id)" if "fatality_id" in df.columns else ""
    cursor.execute(f'CREATE TABLE "{dst_table}" ({col_defs}{pk});')

    # 8️⃣ Insert data
    insert_sql = f"""
        INSERT INTO "{dst_table}" ({", ".join([f'"{c}"' for c in df.columns])})
        VALUES ({", ".join(["%s"] * len(df.columns))})
    """
    for _, row in df.iterrows():
        cursor.execute(insert_sql, [None if pd.isna(v) else v for v in row.values])

    conn.commit()
    conn.close()
    print(f"✔ Created fatalities_prep ({len(df)} rows)")

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

def create_star_schema():
    import pandas as pd
    import numpy as np
    from psycopg2.extras import execute_values

    conn = pg_connect()
    cur = conn.cursor()

    # ==================================================
    # 1️⃣ DROP OLD TABLES
    # ==================================================
    tables_to_drop = [
        "fact_accidents",
        "dim_location",
        "dim_industry",
        "dim_accident_type",
        "dim_hazard",
        "dim_employer",
        "dim_date",
        "dim_country",
        "country_synonym",
        "original_dim_location"
    ]
    for t in tables_to_drop:
        cur.execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
    conn.commit()

    # ==================================================
    # 2️⃣ LOAD PREP TABLES
    # ==================================================
    df_aria = pd.read_sql(f'SELECT * FROM "{DB_CONFIG["ariadb_prep_table"]}"', conn)
    df_work = pd.read_sql(f'SELECT * FROM "{DB_CONFIG["workaccidents_prep_table"]}"', conn)
    df_fatal = pd.read_sql(f'SELECT * FROM "{DB_CONFIG["fatalities_prep_table"]}"', conn)

    # ==================================================
    # 3️⃣ ENSURE DATE AND COUNTRY ARE PRESENT
    # ==================================================
    def enforce_date_country(df, source_name):
        before = len(df)
        # Drop rows where BOTH date and country are missing
        df = df.dropna(how='all', subset=['date', 'country'])
        # Fill remaining missing values
        n_missing_date = df['date'].isna().sum()
        n_missing_country = df['country'].isna().sum()
        df['date'] = df['date'].fillna('UNKNOWN')
        df['country'] = df['country'].fillna('UNKNOWN')
        after = len(df)
        print(f"✔ {source_name}: dropped {before - after} rows where both date and country were missing")
        print(f"   Rows with missing date filled: {n_missing_date}, missing country filled: {n_missing_country}")
        return df

    df_aria = enforce_date_country(df_aria, "aria")
    df_work = enforce_date_country(df_work, "work")
    df_fatal = enforce_date_country(df_fatal, "fatal")

    # ==================================================
    # 4️⃣ ORIGINAL DIM LOCATION
    # ==================================================
    df_orig_loc = pd.concat(
        [df_aria[["country"]], df_work[["country"]], df_fatal[["country"]]],
        ignore_index=True
    ).drop_duplicates()

    df_orig_loc["country"] = df_orig_loc["country"].fillna("UNKNOWN").astype(str).str.strip().str.upper()

    cur.execute("""
        CREATE TABLE original_dim_location (
            country TEXT
        )
    """)
    for _, r in df_orig_loc.iterrows():
        cur.execute(
            "INSERT INTO original_dim_location (country) VALUES (%s)",
            (r.country,)
        )
    conn.commit()

    # ==================================================
    # 5️⃣ DIM COUNTRY + SYNONYMS
    # ==================================================
    cur.execute("""
        CREATE TABLE dim_country (
            country_id INT PRIMARY KEY,
            country_name TEXT UNIQUE,
            country_code TEXT
        )
    """)
    cur.execute("""
        INSERT INTO dim_country VALUES
        (1,'United States','US'),
        (2,'United Kingdom','GB'),
        (3,'France','FR'),
        (4,'Germany','DE'),
        (5,'Canada','CA'),
        (6,'UNKNOWN','XX')
    """)
    cur.execute("""
        CREATE TABLE country_synonym (
            synonym TEXT PRIMARY KEY,
            country_id INT REFERENCES dim_country(country_id)
        )
    """)
    cur.execute("""
        INSERT INTO country_synonym VALUES
        ('USA',1),('US',1),('UNITED STATES',1),
        ('ETATS-UNIS',1),('ETATS UNIS',1),
        ('UK',2),('UNITED KINGDOM',2),('ROYAUME-UNI',2),
        ('FRANCE',3),
        ('GERMANY',4),('ALLEMAGNE',4),
        ('CANADA',5),
        ('UNKNOWN',6)
    """)
    conn.commit()

    # ==================================================
    # 6️⃣ DIM DATE
    # ==================================================
    cur.execute("""
        CREATE TABLE dim_date (
            date_id SERIAL PRIMARY KEY,
            date TEXT UNIQUE
        )
    """)
    all_dates = pd.concat([df_aria[["date"]], df_work[["date"]], df_fatal[["date"]]],
                          ignore_index=True).drop_duplicates().sort_values("date")

    for _, r in all_dates.iterrows():
        cur.execute("INSERT INTO dim_date (date) VALUES (%s)", (r.date,))
    conn.commit()

    # ==================================================
    # 7️⃣ DIM LOCATION
    # ==================================================
    cur.execute("""
        CREATE TABLE dim_location (
            location_id INT PRIMARY KEY,
            country TEXT,
            country_id INT REFERENCES dim_country(country_id)
        )
    """)
    cur.execute("""
        INSERT INTO dim_location
        SELECT
            ROW_NUMBER() OVER (ORDER BY o.country)::INT,
            o.country,
            COALESCE(cs.country_id, 6)
        FROM original_dim_location o
        LEFT JOIN country_synonym cs
            ON o.country = cs.synonym
    """)
    conn.commit()

    # ==================================================
    # 8️⃣ OTHER DIMENSIONS
    # ==================================================
    def create_dim(df_list, col, table, id_col):
        frames = [df[[col]] for df in df_list if col in df.columns]
        if not frames:
            return
        df_dim = pd.concat(frames, ignore_index=True).dropna().drop_duplicates()
        cur.execute(f"""
            CREATE TABLE {table} (
                {id_col} SERIAL PRIMARY KEY,
                name TEXT UNIQUE
            )
        """)
        for v in df_dim[col]:
            cur.execute(f"INSERT INTO {table} (name) VALUES (%s)", (v,))
        conn.commit()

    create_dim([df_aria, df_work, df_fatal], "industry_code", "dim_industry", "industry_id")
    create_dim([df_aria, df_work, df_fatal], "accident_type", "dim_accident_type", "accident_type_id")
    create_dim([df_aria, df_work, df_fatal], "hazard_class", "dim_hazard", "hazard_id")
    create_dim([df_aria, df_work, df_fatal], "employer", "dim_employer", "employer_id")

    # ==================================================
    # 9️⃣ FACT TABLE
    # ==================================================
    cur.execute("""
        CREATE TABLE fact_accidents (
            date_id INT,
            location_id INT,
            employer_id INT,
            hazard_id INT,
            accident_type_id INT,
            industry_id INT
        )
    """)
    conn.commit()

    # Reload dimensions
    df_dim_date = pd.read_sql("SELECT * FROM dim_date", conn)
    df_dim_location = pd.read_sql("SELECT * FROM dim_location", conn)
    df_dim_industry = pd.read_sql("SELECT * FROM dim_industry", conn)
    df_dim_accident_type = pd.read_sql("SELECT * FROM dim_accident_type", conn)
    df_dim_hazard = pd.read_sql("SELECT * FROM dim_hazard", conn)
    df_dim_employer = pd.read_sql("SELECT * FROM dim_employer", conn)

    # ==================================================
    # 10️⃣ Robust build_fact()
    # ==================================================
    def build_fact(df):
        f = df.copy()
        f = f.merge(df_dim_date, on="date", how="left")
        f = f.merge(df_dim_location, on="country", how="left")

        for src, dim, id_col in [
            ("industry_code", df_dim_industry, "industry_id"),
            ("accident_type", df_dim_accident_type, "accident_type_id"),
            ("hazard_class", df_dim_hazard, "hazard_id"),
            ("employer", df_dim_employer, "employer_id"),
        ]:
            if src in f.columns:
                f = f.merge(dim, left_on=src, right_on="name", how="left")
                f[id_col] = f[id_col].fillna(0).astype(int)
            else:
                f[id_col] = 0

        for c in ["date_id", "location_id", "employer_id", "hazard_id", "accident_type_id", "industry_id"]:
            if c not in f.columns:
                f[c] = 0
            else:
                f[c] = f[c].fillna(0).astype(int)

        return f[
            ["date_id", "location_id", "employer_id",
             "hazard_id", "accident_type_id", "industry_id"]
        ]

    df_fact = pd.concat([build_fact(df_aria), build_fact(df_work), build_fact(df_fatal)],
                        ignore_index=True)

    unknown_date_rows = (df_fact['date_id'] == 0).sum()
    unknown_location_rows = (df_fact['location_id'] == 0).sum()
    print(f"Rows with unknown date_id: {unknown_date_rows}, unknown location_id: {unknown_location_rows}")

    # ==================================================
    # 11️⃣ Insert fact table using native Python types
    # ==================================================
    records = [
        tuple(int(v) if isinstance(v, (np.integer, np.int64)) else
              float(v) if isinstance(v, (np.floating, np.float64)) else
              None if pd.isna(v) else v
              for v in r.values)
        for _, r in df_fact.iterrows()
    ]

    execute_values(cur,
        "INSERT INTO fact_accidents VALUES %s",
        records
    )

    conn.commit()
    conn.close()
    print("✔ Star schema successfully created (country-level location)")

def min_test_star_schema():
    """
    Minimal test: select top 5 rows from fact_accidents and ensure dimension IDs exist.
    """
    conn = pg_connect()
    query = """
        SELECT 
            date_id,
            location_id,
            employer_id,
            hazard_id,
            accident_type_id,
            industry_id
        FROM fact_accidents
        LIMIT 5;
    """
    df_test = pd.read_sql(query, conn)
    conn.close()

    print("✔ min_test_star_schema result (top 5 rows):")
    print(df_test)
    return df_test

def full_test_star_schema():
    import pandas as pd

    conn = pg_connect()

    query = """
    SELECT 
        f.date_id, d.date,
        f.location_id, l.country, l.country_id,
        f.employer_id, e.name AS employer_name,
        f.hazard_id, h.name AS hazard_name,
        f.accident_type_id, a.name AS accident_type_name,
        f.industry_id, i.name AS industry_name
    FROM fact_accidents f
    LEFT JOIN dim_date d ON f.date_id = d.date_id
    LEFT JOIN dim_location l ON f.location_id = l.location_id
    LEFT JOIN dim_employer e ON f.employer_id = e.employer_id
    LEFT JOIN dim_hazard h ON f.hazard_id = h.hazard_id
    LEFT JOIN dim_accident_type a ON f.accident_type_id = a.accident_type_id
    LEFT JOIN dim_industry i ON f.industry_id = i.industry_id
    LIMIT 20;
    """

    df_test = pd.read_sql(query, conn)
    conn.close()

    print("✔ Full star schema test query returned:")
    print(df_test.head())
    print(f"Total rows returned: {len(df_test)}")

