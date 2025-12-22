# ==================== etl_utils.py =================================
# The utilities contained therein are used by dag_etl_master.py
# This module provides utility functions for ETL processes
# specifically for downloading, cleaning, and loading datasets
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

CSV_URL = "https://www.data.gouv.fr/api/1/datasets/r/e4a3ad9a-cc9d-40c6-8d1a-aebdf75ded7b"


ZIP_URL = "https://www.osha.gov/sites/default/files/January2015toMarch2025.zip"

# Docker Postgres connection
DB_CONFIG = {
    "host": "postgres",     # Important: Docker hostname
    "port": 5432,
    "database": "airflow",
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
# FATALITIES: APPEND + SCHEMA MERGE
# =====================================================================================

# ==================== etl_utils.py ====================
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

def create_fatalities_clean():
    src_table = DB_CONFIG["fatalities_table"]
    dst_table = DB_CONFIG["fatalities_clean_table"]
    
    conn = pg_connect()

    df = pd.read_sql(f'SELECT * FROM "{src_table}"', conn)

    if "employer_address_of_incident" in df.columns:
        df[["employer","address","zip","city","state"]] = df[
            "employer_address_of_incident"
        ].apply(parse_address)
    else:
        df[["employer","address","zip","city","state"]] = [None, None, None, None, None]

    df["country"] = "USA"

    keep_cols = [
        "date_of_incident","employer","address","zip","city","state",
        "country","victims","hazard_description","fatality_or_catastrophe"
    ]

    df_clean = df[[c for c in keep_cols if c in df.columns]].copy()

    if "date_of_incident" in df_clean.columns:
        df_clean["date_of_incident"] = pd.to_datetime(
            df_clean["date_of_incident"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{dst_table}"')

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

    print(f"✔ Created fatalities_clean ({len(df_clean)} rows)")


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

    conn = pg_connect()

    cur = conn.cursor()

    # Load clean tables
    df_aria = pd.read_sql(f'SELECT * FROM "{DB_CONFIG["ariadb_clean_table"]}"', conn)
    df_fatal = pd.read_sql(f'SELECT * FROM "{DB_CONFIG["fatalities_clean_table"]}"', conn)
    df_work = pd.read_sql('SELECT * FROM workaccidents_clean', conn)

    # =====================================================================
    # DIM DATE
    # =====================================================================

    all_dates = pd.concat([
        pd.to_datetime(df_aria.get("incident_date"), errors="coerce"),
        pd.to_datetime(df_fatal.get("date_of_incident"), errors="coerce"),
        pd.to_datetime(df_work.get("accident_date"), errors="coerce"),
    ]).dropna().drop_duplicates().sort_values().reset_index(drop=True)

    df_dates = pd.DataFrame({"date": all_dates})
    df_dates["year"] = df_dates["date"].dt.year
    df_dates["month"] = df_dates["date"].dt.month
    df_dates["day"] = df_dates["date"].dt.day
    df_dates["quarter"] = df_dates["date"].dt.quarter
    df_dates["date_id"] = range(1, len(df_dates) + 1)

    cur.execute("DROP TABLE IF EXISTS dim_date")
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

    for _, r in df_dates.iterrows():
        cur.execute("""
            INSERT INTO dim_date (date_id, date, year, month, day, quarter)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, [
            int(r.date_id), r.date, int(r.year),
            int(r.month), int(r.day), int(r.quarter)
        ])

    # =====================================================================
    # DIM LOCATION
    # =====================================================================

    def safe_loc(df, mapping):
        out = {}
        for target, src in mapping.items():
            out[target] = df[src] if src in df.columns else pd.Series([None] * len(df))
        return pd.DataFrame(out)

    df_loc = pd.concat([
        safe_loc(df_aria, {"municipality": "municipality", "department": "department", "country": "country"}),
        safe_loc(df_fatal, {"municipality": "city", "department": "state", "country": "country"}),
        safe_loc(df_work, {"municipality": "city", "department": "state", "country": "country"}),
    ], ignore_index=True).drop_duplicates().reset_index(drop=True)

    df_loc["location_id"] = range(1, len(df_loc) + 1)

    cur.execute("DROP TABLE IF EXISTS dim_location")
    cur.execute("""
        CREATE TABLE dim_location (
            location_id INT PRIMARY KEY,
            municipality TEXT,
            department TEXT,
            country TEXT
        );
    """)

    for _, r in df_loc.iterrows():
        cur.execute("""
            INSERT INTO dim_location (location_id, municipality, department, country)
            VALUES (%s, %s, %s, %s)
        """, [int(r.location_id), r.municipality, r.department, r.country])

    # =====================================================================
    # DIM EMPLOYER
    # =====================================================================

    def extract_employer(df):
        return df[["employer"]].dropna().drop_duplicates().reset_index(drop=True) \
            if "employer" in df.columns else pd.DataFrame(columns=["employer"])

    df_emp = pd.concat([
        extract_employer(df_aria),
        extract_employer(df_fatal),
        extract_employer(df_work),
    ], ignore_index=True).drop_duplicates().reset_index(drop=True)

    df_emp["employer_id"] = range(1, len(df_emp) + 1)

    cur.execute("DROP TABLE IF EXISTS dim_employer")
    cur.execute("""
        CREATE TABLE dim_employer (
            employer_id INT PRIMARY KEY,
            employer TEXT
        );
    """)

    for _, r in df_emp.iterrows():
        cur.execute("""
            INSERT INTO dim_employer (employer_id, employer)
            VALUES (%s, %s)
        """, [int(r.employer_id), r.employer])

    # =====================================================================
    # DIM HAZARD
    # =====================================================================

    hazard_frames = []
    if "hazard_class" in df_aria.columns:
        hazard_frames.append(df_aria[["hazard_class"]].rename(columns={"hazard_class": "hazard"}))
    if "hazard_description" in df_fatal.columns:
        hazard_frames.append(df_fatal[["hazard_description"]].rename(columns={"hazard_description": "hazard"}))
    if "nature" in df_work.columns:
        hazard_frames.append(df_work[["nature"]].rename(columns={"nature": "hazard"}))

    df_haz = pd.concat(hazard_frames, ignore_index=True) \
               .dropna().drop_duplicates().reset_index(drop=True)

    df_haz["hazard_id"] = range(1, len(df_haz) + 1)

    cur.execute("DROP TABLE IF EXISTS dim_hazard")
    cur.execute("""
        CREATE TABLE dim_hazard (
            hazard_id INT PRIMARY KEY,
            hazard TEXT
        );
    """)

    for _, r in df_haz.iterrows():
        cur.execute("""
            INSERT INTO dim_hazard (hazard_id, hazard)
            VALUES (%s, %s)
        """, [int(r.hazard_id), r.hazard])

    # =====================================================================
    # DIM ACCIDENT TYPE
    # =====================================================================

    accident_frames = []
    if "accident_type" in df_fatal.columns:
        accident_frames.append(df_fatal[["accident_type"]])
    if "naturetitle" in df_work.columns:
        accident_frames.append(df_work[["naturetitle"]].rename(columns={"naturetitle": "accident_type"}))

    df_act = pd.concat(accident_frames, ignore_index=True) \
               .dropna().drop_duplicates().reset_index(drop=True)

    df_act["accident_type_id"] = range(1, len(df_act) + 1)

    cur.execute("DROP TABLE IF EXISTS dim_accident_type")
    cur.execute("""
        CREATE TABLE dim_accident_type (
            accident_type_id INT PRIMARY KEY,
            accident_type TEXT
        );
    """)

    for _, r in df_act.iterrows():
        cur.execute("""
            INSERT INTO dim_accident_type (accident_type_id, accident_type)
            VALUES (%s, %s)
        """, [int(r.accident_type_id), r.accident_type])

    # =====================================================================
    # FACT TABLE
    # =====================================================================

    def build_fact(df, date_col, employer_col, hazard_col, acc_type_col):
        df2 = df.copy()

        # DATE
        df2[date_col] = pd.to_datetime(df2.get(date_col), errors="coerce")
        df2 = df2.merge(df_dates[["date", "date_id"]], left_on=date_col, right_on="date", how="left")

        # LOCATION
        for col in ["municipality", "department", "country"]:
            if col not in df2.columns:
                df2[col] = None

        df2 = df2.merge(
            df_loc[["municipality", "department", "country", "location_id"]],
            on=["municipality", "department", "country"],
            how="left"
        )

        # EMPLOYER
        if employer_col and employer_col in df2.columns:
            df2 = df2.merge(
                df_emp[["employer", "employer_id"]],
                on="employer", how="left"
            )
        else:
            df2["employer_id"] = None

        # HAZARD
        if hazard_col and hazard_col in df2.columns:
            df2 = df2.merge(
                df_haz[["hazard", "hazard_id"]],
                left_on=hazard_col, right_on="hazard",
                how="left"
            )
        else:
            df2["hazard_id"] = None

        # ACCIDENT TYPE
        if acc_type_col and acc_type_col in df2.columns:
            df2 = df2.merge(
                df_act[["accident_type", "accident_type_id"]],
                left_on=acc_type_col, right_on="accident_type",
                how="left"
            )
        else:
            df2["accident_type_id"] = None

        return df2[["date_id", "employer_id", "location_id", "hazard_id", "accident_type_id"]]

    df_fact = pd.concat([
        build_fact(df_aria, "incident_date", "employer", "hazard_class", None),
        build_fact(df_fatal, "date_of_incident", "employer", "hazard_description", "accident_type"),
        build_fact(df_work, "accident_date", "employer", "nature", "naturetitle"),
    ], ignore_index=True)

    cur.execute("DROP TABLE IF EXISTS fact_accidents")
    cur.execute("""
        CREATE TABLE fact_accidents (
            date_id INT,
            employer_id INT,
            location_id INT,
            hazard_id INT,
            accident_type_id INT
        );
    """)

    for _, r in df_fact.iterrows():
        cur.execute("""
            INSERT INTO fact_accidents (date_id, employer_id, location_id, hazard_id, accident_type_id)
            VALUES (%s, %s, %s, %s, %s)
        """, [
            None if pd.isna(r.date_id) else int(r.date_id),
            None if pd.isna(r.employer_id) else int(r.employer_id),
            None if pd.isna(r.location_id) else int(r.location_id),
            None if pd.isna(r.hazard_id) else int(r.hazard_id),
            None if pd.isna(r.accident_type_id) else int(r.accident_type_id),
        ])

    conn.commit()
    conn.close()

    print(f"✔ Star schema created — {len(df_fact)} fact rows")


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
