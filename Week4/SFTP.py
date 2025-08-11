# dags/sftp_feedback_to_stage.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from hashlib import md5
import io, csv, paramiko, os
from sqlalchemy import Table, Column, String, Integer, DateTime, MetaData

target_metadata = MetaData()

STG_FEEDBACK = Table(
    "STG_FEEDBACK", target_metadata,
    Column("feedback_id", String(32)),
    Column("order_id", String(32)),
    Column("feedback_score", Integer),
    Column("feedback_form_sent_date", DateTime),
    Column("feedback_answer_date", DateTime),
    Column("md5_hash", String(64), primary_key=True),
    Column("dv_load_timestamp", DateTime),
    Column("event_id", String(255)),  
)

FEEDBACK = Table(
    "FEEDBACK", target_metadata,
    Column("feedback_id", String(32)),
    Column("order_id", String(32)),
    Column("feedback_score", Integer),
    Column("feedback_form_sent_date", DateTime),
    Column("feedback_answer_date", DateTime),
    Column("md5_hash", String(64), primary_key=True),
    Column("dv_load_timestamp", DateTime),
    Column("event_id", String(255)),  
)

FEEDBACK_DUPLICATE_ARCHIVE = Table(
    "FEEDBACK_DUPLICATE_ARCHIVE", target_metadata,
    Column("feedback_id", String(32), primary_key=True),
    Column("order_id", String(32)),
    Column("feedback_score", Integer),
    Column("feedback_form_sent_date", DateTime),
    Column("feedback_answer_date", DateTime),
    Column("md5_hash", String(64)),
    Column("dv_load_timestamp", DateTime),
    Column("event_id", String(255)),  
)

SFTP_HOST = "34.16.77.121"
SFTP_PORT = 2222
SFTP_USER = "BuildProject"
SFTP_PASS = "student"
SFTP_REMOTE_PATH = "/upload/feedback_dataset.csv"

MYSQL_CONFIG = {
    'host': "34.121.169.125",
    'port': 3306,
    'db': "STAGELOAD",
    'user': "build2025yixuan",
    'password': "WAkuro2!googlecloud"
}

def compute_md5_business_keys(feedback_id: str, order_id: str, dv_ts_iso: str) -> str:
    s = f"{(feedback_id or '').strip().lower()}|{(order_id or '').strip().lower()}|{dv_ts_iso}"
    return md5(s.encode("utf-8")).hexdigest()

def fetch_csv_from_sftp_callable():
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        with sftp.file(SFTP_REMOTE_PATH, "rb") as f:
            data = f.read()  # bytes
        return data.decode("utf-8", errors="replace")
    finally:
        sftp.close()
        transport.close()

def _parse_date(s):
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None

def load_csv_to_staging_from_xcom(ti):
    csv_text = ti.xcom_pull(task_ids="fetch_csv_from_sftp")
    if not csv_text:
        return

    
    event_id = os.path.basename(SFTP_REMOTE_PATH)

   
    engine = create_engine(
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['db']}",
        pool_pre_ping=True,
    )
    target_metadata.create_all(engine, tables=[STG_FEEDBACK, FEEDBACK_DUPLICATE_ARCHIVE, FEEDBACK], checkfirst=True)

    # skip if this filename already loaded into staging
    with engine.begin() as conn:
        already = conn.execute(
            text("SELECT 1 FROM STG_FEEDBACK WHERE event_id = :eid LIMIT 1"),
            {"eid": event_id}
        ).first()
        if already:
            print(f"File '{event_id}' already processed; skipping load.")
            return

    # parse CSV
    buf = io.StringIO(csv_text)
    reader = csv.DictReader(buf)
    rows = list(reader)

    # transform
    dv_ts = datetime.now(timezone.utc).replace(tzinfo=None)  # store naive UTC for MySQL DATETIME
    dv_ts_iso = dv_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    for r in rows:
        r["feedback_id"] = (r.get("feedback_id") or "").strip()
        r["order_id"] = (r.get("order_id") or "").strip()
        r["feedback_score"] = int(r["feedback_score"]) if r.get("feedback_score") not in (None, "",) else None
        r["feedback_form_sent_date"] = _parse_date(r.get("feedback_form_sent_date"))
        r["feedback_answer_date"] = _parse_date(r.get("feedback_answer_date"))
        r["dv_load_timestamp"] = dv_ts
        r["md5_hash"] = compute_md5_business_keys(r["feedback_id"], r["order_id"], dv_ts_iso)
        r["event_id"] = event_id  # NEW

    # split new vs duplicates by FEEDBACK md5
    new_records, duplicates = [], []
    with engine.begin() as conn:
        existing = set(row[0] for row in conn.execute("SELECT md5_hash FROM FEEDBACK").fetchall())
        for r in rows:
            if r["md5_hash"] in existing:
                duplicates.append(r)
            else:
                new_records.append(r)

        if new_records:
            conn.execute(STG_FEEDBACK.insert(), new_records)
        if duplicates:
            conn.execute(FEEDBACK_DUPLICATE_ARCHIVE.insert().prefix_with("IGNORE"), duplicates)

def insert_non_duplicates_to_final():
    engine = create_engine(
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['db']}",
        pool_pre_ping=True,
    )

    
    insert_sql = text("""
        INSERT IGNORE INTO FEEDBACK
        SELECT * FROM STG_FEEDBACK;
    """)

    with engine.begin() as conn:
        result = conn.execute(insert_sql)
        print(f"Inserted {result.rowcount} new rows into FEEDBACK (duplicates skipped).")

default_args = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id="sftp_feedback_to_stage_event",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["sftp", "staging", "data-vault"],
) as dag:
    fetch_csv_from_sftp = PythonOperator(
        task_id="fetch_csv_from_sftp",
        python_callable=fetch_csv_from_sftp_callable,
    )
    load_to_staging = PythonOperator(
        task_id="load_csv_to_staging",
        python_callable=load_csv_to_staging_from_xcom,
    )
    insert_unique = PythonOperator(
        task_id="insert_non_duplicates_to_final",
        python_callable=insert_non_duplicates_to_final,
    )

    fetch_csv_from_sftp >> load_to_staging >> insert_unique
