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
    "ariadb_prep_table": "ariadb_prep",
    "workaccidents_table": "workaccidents",
    "workaccidents_prep_table": "workaccidents_prep",
    "workaccidents_clean_table": "workaccidents_clean",
    "fatalities_table": "fatalities",
    "fatalities_clean_table": "fatalities_clean",
    "fatalities_prep_table": "fatalities_prep",
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
def create_ariadb_clean():
    src_table = DB_CONFIG["ariadb_table"]
    dst_table = DB_CONFIG["ariadb_clean_table"]
    conn = pg_connect()
    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)

    # Updated column mapping
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
        "type_évènement": "hazard_class",  # ✅ New mapping
        # "classe_de_danger_clp": "hazard_class",  # removed
    }

    # Keep only existing columns and rename them
    existing = [c for c in col_map if c in df.columns]
    df = df[existing].rename(columns={k: col_map[k] for k in existing}).copy()

    # Normalize date
    if "incident_date" in df.columns:
        df["incident_date"] = pd.to_datetime(df["incident_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Deduplicate on primary key
    if "aria_id" in df.columns:
        df.drop_duplicates(subset=["aria_id"], inplace=True)

    # Normalize country
    df = normalize_country(df, "country")

    # Create clean table
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
    print(f"✔ Created ariadb_clean ({len(df)} rows)")

# 2️⃣ WORKACCIDENTS
def create_workaccidents_clean():
    src_table = DB_CONFIG["workaccidents_table"]
    dst_table = DB_CONFIG["workaccidents_clean_table"]
    conn = pg_connect()
    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)

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

    # Create clean table
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
    # 3️⃣ ORIGINAL DIM LOCATION
    # ==================================================
    frames = []
    if all(c in df_aria.columns for c in ["municipality", "department", "country"]):
        frames.append(df_aria[["municipality", "department", "country"]])
    if all(c in df_work.columns for c in ["city", "state", "country"]):
        frames.append(df_work[["city","state","country"]].rename(columns={"city":"municipality","state":"department"}))
    if all(c in df_fatal.columns for c in ["city","state","country"]):
        frames.append(df_fatal[["city","state","country"]].rename(columns={"city":"municipality","state":"department"}))

    df_orig_loc = pd.concat(frames, ignore_index=True).drop_duplicates().fillna("UNKNOWN")

    # Create original_dim_location
    cur.execute("""
        CREATE TABLE original_dim_location (
            municipality TEXT,
            department TEXT,
            country TEXT
        )
    """)
    for _, r in df_orig_loc.iterrows():
        cur.execute("INSERT INTO original_dim_location VALUES (%s,%s,%s)",
                    (r.municipality, r.department, r.country))
    conn.commit()

    # ==================================================
    # 4️⃣ DIM COUNTRY + SYNONYMS
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
    # 5️⃣ DIM DATE (TEXT, merge-safe YYYY-MM-DD strings)
    # ==================================================
    cur.execute("""
        CREATE TABLE dim_date (
            date_id SERIAL PRIMARY KEY,
            date TEXT UNIQUE
        )
    """)
    all_dates = pd.concat([
        df_aria[["date"]],
        df_work[["date"]],
        df_fatal[["date"]]
    ], ignore_index=True)
    # Already standardized in prep tables as YYYY-MM-DD string
    all_dates = all_dates.dropna().drop_duplicates().sort_values("date").reset_index(drop=True)
    for _, r in all_dates.iterrows():
        cur.execute("INSERT INTO dim_date (date) VALUES (%s)", (r.date,))
    conn.commit()

    # ==================================================
    # 6️⃣ OTHER DIM TABLES (EMPTY STRUCTURE)
    # ==================================================
    for table_name in ["dim_industry","dim_accident_type","dim_hazard","dim_employer","dim_location"]:
        if table_name != "dim_location":  # dim_location created separately
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    {table_name[:-4]}_id SERIAL PRIMARY KEY,
                    name TEXT
                )
            """)
    conn.commit()

    # ==================================================
    # 7️⃣ DIM LOCATION
    # ==================================================
    cur.execute("""
        CREATE TABLE dim_location (
            location_id INT PRIMARY KEY,
            municipality TEXT,
            department TEXT,
            country TEXT,
            country_id INT REFERENCES dim_country(country_id)
        )
    """)
    cur.execute("""
        INSERT INTO dim_location
        SELECT
            ROW_NUMBER() OVER (ORDER BY municipality, department, country)::INT,
            municipality,
            department,
            country,
            COALESCE(cs.country_id,6)
        FROM original_dim_location o
        LEFT JOIN country_synonym cs
            ON o.country = cs.synonym
    """)
    conn.commit()

    # ==================================================
    # 8️⃣ BUILD FACT TABLE
    # ==================================================
    cur.execute("DROP TABLE IF EXISTS fact_accidents")
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

    # Load dim tables for mapping
    df_dim_location = pd.read_sql("SELECT * FROM dim_location", conn)
    df_dim_date = pd.read_sql("SELECT * FROM dim_date", conn)
    df_dim_industry = pd.read_sql("SELECT * FROM dim_industry", conn)
    df_dim_accident_type = pd.read_sql("SELECT * FROM dim_accident_type", conn)
    df_dim_hazard = pd.read_sql("SELECT * FROM dim_hazard", conn)
    df_dim_employer = pd.read_sql("SELECT * FROM dim_employer", conn)

    def build_fact(df, date_col, location_cols=["municipality","department","country"]):
        df2 = df.copy()
        # Ensure date column is string to match dim_date
        df2[date_col] = df2[date_col].astype(str)
        df2 = df2.merge(df_dim_date[["date","date_id"]], left_on=date_col, right_on="date", how="left").drop(columns=["date"], errors="ignore")
        df2 = df2.merge(df_dim_location, on=location_cols, how="left")
        for col_map, df_dim_map, id_name in [
            ("industry_code", df_dim_industry, "industry_id"),
            ("accident_type", df_dim_accident_type, "accident_type_id"),
            (["hazard_class","event_type"], df_dim_hazard, "hazard_id"),
            ("employer", df_dim_employer, "employer_id")
        ]:
            if isinstance(col_map, list):
                keys = col_map
                if all(k in df2.columns for k in keys):
                    df2 = df2.merge(df_dim_map, on=keys, how="left")
            else:
                if col_map in df2.columns:
                    df2 = df2.merge(df_dim_map, left_on=col_map, right_on="name", how="left")
        # Fill missing IDs with 0
        for id_col in ["date_id","location_id","employer_id","hazard_id","accident_type_id","industry_id"]:
            if id_col in df2.columns:
                df2[id_col] = df2[id_col].fillna(0).astype(int)
        return df2[["date_id","location_id","employer_id","hazard_id","accident_type_id","industry_id"]]

    df_fact_all = pd.concat([
        build_fact(df_aria, "date"),
        build_fact(df_work, "date"),
        build_fact(df_fatal, "date")
    ], ignore_index=True)

    # Insert fact rows
    insert_sql = """
        INSERT INTO fact_accidents (date_id, location_id, employer_id, hazard_id, accident_type_id, industry_id)
        VALUES (%s,%s,%s,%s,%s,%s)
    """
    for _, row in df_fact_all.iterrows():
        cur.execute(insert_sql, tuple(row.values))

    conn.commit()
    conn.close()
    print("✔ Star schema created successfully using prep tables!")


def create_dimensions_and_fact():
    import pandas as pd
    import numpy as np

    conn = pg_connect()
    cur = conn.cursor()

    # ==================================================
    # HELPERS
    # ==================================================
    def normalize_country(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return "UNKNOWN"
        s = str(value).strip().upper()
        s = s.replace("É", "E").replace("È", "E").replace("Ê", "E")
        if s in {"USA", "US", "UNITED STATES", "ETATS-UNIS", "ETATS UNIS", "ETATSUNIS"}:
            return "USA"
        return s

    def normalize_text(obj):
        if isinstance(obj, pd.Series):
            return obj.astype(str).str.strip().str.upper()
        elif isinstance(obj, pd.DataFrame):
            return obj.apply(lambda c: c.astype(str).str.strip().str.upper())
        return obj

    def drop_empty_rows(df, key_cols):
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
    # LOAD CLEAN TABLES AND RENAME DATES
    # ==================================================
    df_aria = pd.read_sql(f'SELECT * FROM "{DB_CONFIG["ariadb_clean_table"]}"', conn)
    df_fatal = pd.read_sql(f'SELECT * FROM "{DB_CONFIG["fatalities_clean_table"]}"', conn)
    df_work = pd.read_sql('SELECT * FROM "workaccidents_clean"', conn)

    for df in [df_aria, df_fatal, df_work]:
        # Normalize country
        if "country" in df.columns:
            df["country"] = df["country"].apply(normalize_country)
        # Standardize date column
        if "incident_date" in df.columns:
            df.rename(columns={"incident_date": "date_of_incident"}, inplace=True)
        if "accident_date" in df.columns:
            df.rename(columns={"accident_date": "date_of_incident"}, inplace=True)
        if "date" in df.columns:
            df.rename(columns={"date": "date_of_incident"}, inplace=True)

    # Drop empty rows
    df_aria = drop_empty_rows(df_aria, ["municipality", "department", "country", "date_of_incident"])
    df_fatal = drop_empty_rows(df_fatal, ["city", "state", "country", "date_of_incident"])
    df_work = drop_empty_rows(df_work, ["city", "state", "country", "date_of_incident"])

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

    df_orig_loc = pd.concat(frames, ignore_index=True).drop_duplicates()
    df_orig_loc = normalize_text(df_orig_loc.fillna("UNKNOWN"))

    cur.execute("""
        CREATE TABLE original_dim_location (
            municipality TEXT,
            department TEXT,
            country TEXT
        );
    """)
    for _, r in df_orig_loc.iterrows():
        cur.execute("INSERT INTO original_dim_location VALUES (%s,%s,%s)", (r.municipality, r.department, r.country))
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
        (1,'United States','US'),
        (2,'United Kingdom','GB'),
        (3,'France','FR'),
        (4,'Germany','DE'),
        (5,'Canada','CA'),
        (6,'UNKNOWN','XX');
    """)
    cur.execute("""
        CREATE TABLE country_synonym (
            synonym TEXT PRIMARY KEY,
            country_id INT REFERENCES dim_country(country_id)
        );
    """)
    cur.execute("""
        INSERT INTO country_synonym VALUES
        ('USA',1),('US',1),('UNITED STATES',1),
        ('ETATS-UNIS',1),('ETATS UNIS',1),
        ('UK',2),('UNITED KINGDOM',2),('ROYAUME-UNI',2),
        ('FRANCE',3),
        ('GERMANY',4),('ALLEMAGNE',4),
        ('CANADA',5),
        ('UNKNOWN',6);
    """)
    conn.commit()

    # ==================================================
    # DIM DATE
    # ==================================================
    cur.execute("""
        CREATE TABLE dim_date (
            date_id SERIAL PRIMARY KEY,
            date TEXT UNIQUE
        );
    """)

    # Collect unique dates from all prep tables
    all_dates = pd.concat([
        df_aria[["date_of_incident"]],
        df_fatal[["date_of_incident"]],
        df_work[["date_of_incident"]]
    ], ignore_index=True)

    # Convert to datetime to standardize, then format as YYYY-MM-DD strings
    all_dates["date_of_incident"] = pd.to_datetime(all_dates["date_of_incident"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Drop invalid or duplicate dates
    all_dates = all_dates.dropna().drop_duplicates().sort_values("date_of_incident").reset_index(drop=True)

    # Insert into dim_date
    for idx, r in all_dates.iterrows():
        cur.execute("INSERT INTO dim_date (date) VALUES (%s)", (r.date_of_incident,))
    conn.commit()
    
    # ==================================================
    # OTHER DIM TABLES (EMPTY STRUCTURES TO AVOID ERRORS)
    # ==================================================
    for table_name in ["dim_industry", "dim_accident_type", "dim_hazard", "dim_employer"]:
        cur.execute(f"""
            CREATE TABLE {table_name} (
                {table_name[:-4]}_id SERIAL PRIMARY KEY,
                name TEXT
            );
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
            ROW_NUMBER() OVER (ORDER BY m,d,c_id)::INT,
            m,d,c,c_id
        FROM (
            SELECT DISTINCT
                municipality AS m,
                department AS d,
                country AS c,
                COALESCE(cs.country_id,6) AS c_id
            FROM original_dim_location o
            LEFT JOIN country_synonym cs
                ON o.country = cs.synonym
        ) t;
    """)
    conn.commit()
    df_dim_location = pd.read_sql("SELECT * FROM dim_location", conn)

    # ==================================================
    # BUILD FACT
    # ==================================================
    def build_fact(df):
        df2 = df.copy()
        df2[["municipality","department","country"]] = normalize_text(df2[["municipality","department","country"]].fillna("UNKNOWN"))
        df2["date_of_incident"] = pd.to_datetime(df2["date_of_incident"], errors="coerce")
        df2 = df2.merge(df_dates[["date","date_id"]], left_on="date_of_incident", right_on="date", how="left").drop(columns=["date"], errors="ignore")
        df2 = df2.merge(df_dim_location, on=["municipality","department","country"], how="left")
        df2[["date_id","location_id"]] = df2[["date_id","location_id"]].fillna(0).astype(int)
        return df2[["date_id","location_id"]]

    # ==================================================
    # FACT TABLE
    # ==================================================
    cur.execute("""
        CREATE TABLE fact_accidents (
            date_id INT,
            location_id INT
        );
    """)
    df_dates = pd.read_sql("SELECT * FROM dim_date", conn)
    df_fact = pd.concat([
        build_fact(df_aria),
        build_fact(df_fatal),
        build_fact(df_work)
    ], ignore_index=True)

    for _, r in df_fact.iterrows():
        cur.execute("INSERT INTO fact_accidents VALUES (%s,%s)", (int(r.date_id), int(r.location_id)))

    conn.commit()
    conn.close()
    print("✔ Star schema created successfully (all dims recreated, consistent date_of_incident, USA-normalized, synonym-safe)")

# =====================================================================================
# STAR SCHEMA TESTS — UPDATED FOR CURRENT ETL
# =====================================================================================

def min_test_star_schema():
    """
    Minimal test: select top 5 rows from fact_accidents (only existing columns).
    """
    conn = pg_connect()

    query = """
        SELECT 
            date_id,
            location_id
        FROM fact_accidents
        LIMIT 5;
    """

    df_test = pd.read_sql(query, conn)
    conn.close()

    print("✔ min_test_star_schema result:")
    print(df_test)
    return df_test


def full_test_star_schema():
    """
    Full test: join fact_accidents with available dimensions (country-only version).
    Only includes dim_date and dim_location which exist currently.
    """
    conn = pg_connect()

    sql = """
        SELECT 
            f.date_id, d.date, d.year, d.month,
            f.location_id, l.country AS country_name
        FROM fact_accidents f
        LEFT JOIN dim_date d ON f.date_id = d.date_id
        LEFT JOIN dim_location l ON f.location_id = l.location_id
        LIMIT 20;
    """

    df = pd.read_sql(sql, conn)
    conn.close()

    print("\n=== Full Star Schema Test (20 rows) ===")
    print(df)
    return df
