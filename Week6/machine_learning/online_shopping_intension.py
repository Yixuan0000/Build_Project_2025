from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timezone
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, DateTime
from sqlalchemy.sql import text
import csv, io, paramiko
from hashlib import md5

# --- Connection helpers ---
def get_engine():
    conn = BaseHook.get_connection("mysql_stage")
    url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port or 3306}/{conn.schema}"
    return create_engine(url, pool_pre_ping=True)

def fetch_from_sftp(remote_path: str) -> str:
    conn = BaseHook.get_connection("sftp_build")
    transport = paramiko.Transport((conn.host, conn.port or 22))
    transport.connect(username=conn.login, password=conn.password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        with sftp.file(remote_path, "rb") as f:
            return f.read().decode("utf-8")
    finally:
        sftp.close()
        transport.close()

# --- Helpers ---
def to_int(v): 
    try: return int(v) if v not in (None, "",) else None
    except: return None

def to_float(v): 
    try: return float(v) if v not in (None, "",) else None
    except: return None

def compute_md5(keys, row):
    # Stable hash across runs (business keys only, no timestamp)
    parts = [str((row.get(k) or "")).strip().lower() for k in keys]
    return md5("|".join(parts).encode("utf-8")).hexdigest()

# --- Table definitions ---
metadata = MetaData()

def make_columns(is_archive=False):
    return [
        Column("Administrative", Integer),
        Column("Administrative_Duration", Float),
        Column("Informational", Integer),
        Column("Informational_Duration", Float),
        Column("ProductRelated", Integer),
        Column("ProductRelated_Duration", Float),
        Column("BounceRates", Float),
        Column("ExitRates", Float),
        Column("PageValues", Float),
        Column("SpecialDay", Float),
        Column("Month", String(16)),
        Column("OperatingSystems", Integer),
        Column("Browser", Integer),
        Column("Region", Integer),
        Column("TrafficType", Integer),
        Column("VisitorType", String(32)),
        Column("Weekend", String(6)),
        Column("Revenue", String(6)),
        Column("md5_hash", String(64), primary_key=(not is_archive)), 
        Column("dv_load_timestamp", DateTime),
        Column("event_id", String(255)),
    ]
    return cols

STG  = Table("STG_online_shoppers_intention",  metadata, *make_columns())
FINAL = Table("DIM_online_shoppers_intention", metadata, *make_columns())
ARCH  = Table("ONLINE_SHOPPERS_INTENTION_DUPLICATE_ARCHIVE", metadata, *make_columns())

# --- Tasks ---
def fetch_task_callable(**_):
    return fetch_from_sftp("/upload/online_shoppers_intention.csv")

def load_to_stage_callable(ti, **_):
    csv_text = ti.xcom_pull(task_ids="fetch_csv")
    if not csv_text:
        return

    event_id = "online_shoppers_intention.csv"
    rows = list(csv.DictReader(io.StringIO(csv_text)))
    dv_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    processed_rows, dup_rows = [], []
    seen_hashes = set()

    for r in rows:
        # --- Normalize first ---
        r["Administrative"] = to_int(r.get("Administrative"))
        r["Administrative_Duration"] = to_float(r.get("Administrative_Duration"))
        r["Informational"] = to_int(r.get("Informational"))
        r["Informational_Duration"] = to_float(r.get("Informational_Duration"))
        r["ProductRelated"] = to_int(r.get("ProductRelated"))
        r["ProductRelated_Duration"] = to_float(r.get("ProductRelated_Duration"))
        r["BounceRates"] = to_float(r.get("BounceRates"))
        r["ExitRates"] = to_float(r.get("ExitRates"))
        r["PageValues"] = to_float(r.get("PageValues"))
        r["SpecialDay"] = to_float(r.get("SpecialDay"))
        r["OperatingSystems"] = to_int(r.get("OperatingSystems"))
        r["Browser"] = to_int(r.get("Browser"))
        r["Region"] = to_int(r.get("Region"))
        r["TrafficType"] = to_int(r.get("TrafficType"))

        # --- Add metadata ---
        r["dv_load_timestamp"] = dv_ts
        r["event_id"] = event_id
        r["md5_hash"] = compute_md5(
            ["Administrative","Administrative_Duration","Informational","Informational_Duration",
             "ProductRelated","ProductRelated_Duration","BounceRates","ExitRates","PageValues",
             "SpecialDay","Month","OperatingSystems","Browser","Region","TrafficType",
             "VisitorType","Weekend","Revenue"], r
        )

        # --- Dedup within batch ---
        if r["md5_hash"] not in seen_hashes:
            seen_hashes.add(r["md5_hash"])
            processed_rows.append(r)
        else:
            dup_rows.append(r)

    engine = get_engine()
    metadata.create_all(engine, tables=[STG, FINAL, ARCH], checkfirst=True)

    with engine.begin() as conn:
        # Always truncate staging before loading fresh
        conn.execute(text("TRUNCATE TABLE STG_online_shoppers_intention"))

        if processed_rows:
            conn.execute(STG.insert(), processed_rows)
        if dup_rows:
            conn.execute(ARCH.insert(), dup_rows)

def upsert_to_final_callable(**_):
    engine = get_engine()
    with engine.begin() as conn:
        # Insert only unique rows from staging into DIM
        conn.execute(text("""
            INSERT IGNORE INTO DIM_online_shoppers_intention
            SELECT * FROM STG_online_shoppers_intention
        """))

# --- DAG definition ---
with DAG(
    dag_id="sftp_online_shoppers_intention_to_stage_event",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["sftp", "online_shoppers_intention"],
) as dag:

    fetch = PythonOperator(task_id="fetch_csv", python_callable=fetch_task_callable)
    load = PythonOperator(task_id="load_to_staging", python_callable=load_to_stage_callable)
    insert = PythonOperator(task_id="insert_to_final", python_callable=upsert_to_final_callable)

    fetch >> load >> insert
