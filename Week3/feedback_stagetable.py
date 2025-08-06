from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from sqlalchemy import Table, Column, String, Integer, DateTime, MetaData, create_engine
import pymysql
from hashlib import md5

# --- Table Definitions (Capitalized) ---
target_metadata = MetaData()

STG_FEEDBACK = Table(
    "STG_FEEDBACK",
    target_metadata,
    Column("feedback_id", String(32), primary_key=True),
    Column("order_id", String(32)),
    Column("feedback_score", Integer),
    Column("feedback_form_sent_date", DateTime),
    Column("feedback_answer_date", DateTime),
    Column("md5_hash", String(64)),
    Column("dv_load_timestamp", DateTime),
)

FEEDBACK = Table(
    "FEEDBACK",
    target_metadata,
    Column("feedback_id", String(32), primary_key=True),
    Column("order_id", String(32)),
    Column("feedback_score", Integer),
    Column("feedback_form_sent_date", DateTime),
    Column("feedback_answer_date", DateTime),
    Column("md5_hash", String(64)),
    Column("dv_load_timestamp", DateTime),
)

FEEDBACK_DUPLICATE_ARCHIVE = Table(
    "FEEDBACK_DUPLICATE_ARCHIVE",
    target_metadata,
    Column("feedback_id", String(32), primary_key=True),
    Column("order_id", String(32)),
    Column("feedback_score", Integer),
    Column("feedback_form_sent_date", DateTime),
    Column("feedback_answer_date", DateTime),
    Column("md5_hash", String(64)),
    Column("dv_load_timestamp", DateTime),
)

API_BASE_URL = "http://34.16.77.121:1515"
API_USERNAME = "student1"
API_PASSWORD = "pass123"
MYSQL_CONFIG = {
    'host': "34.121.169.125",
    'port': 3306,
    'db': "STAGELOAD",
    'user': "build2025yixuan",
    'password': "WAkuro2!googlecloud"
}

def compute_md5(row):
    row_string = "|".join(str(row.get(col, "")) for col in sorted(row.keys()) if col not in ["md5_hash", "dv_load_timestamp"])
    return md5(row_string.encode('utf-8')).hexdigest()

def format_datetime(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(value.strip(), fmt)
        except ValueError:
            continue
    return None

def fetch_data_from_api_callable():
    endpoint = f"{API_BASE_URL}/feedback/"
    print(f"Fetching data from: {endpoint}")
    response = requests.get(endpoint, auth=(API_USERNAME, API_PASSWORD))
    response.raise_for_status()
    return response.text

def load_data_to_staging(ti):
    fetched_data_json = ti.xcom_pull(task_ids='fetch_data_from_api_task')
    if not fetched_data_json:
        return
    data = json.loads(fetched_data_json)
    now = datetime.now()

    engine = create_engine(f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['db']}")
    target_metadata.create_all(engine, tables=[STG_FEEDBACK, FEEDBACK_DUPLICATE_ARCHIVE, FEEDBACK], checkfirst=True)

    with engine.begin() as conn:
        existing_hashes = set(row[0] for row in conn.execute("SELECT md5_hash FROM FEEDBACK").fetchall())
        new_records = []
        duplicates = []

        for row in data:
            row["feedback_form_sent_date"] = format_datetime(str(row.get("feedback_form_sent_date")))
            row["feedback_answer_date"] = format_datetime(str(row.get("feedback_answer_date")))
            row["md5_hash"] = compute_md5(row)
            row["dv_load_timestamp"] = now

            if row["md5_hash"] in existing_hashes:
                duplicates.append(row)
            else:
                new_records.append(row)

        if new_records:
            conn.execute(STG_FEEDBACK.insert(), new_records)
        if duplicates:
            conn.execute(FEEDBACK_DUPLICATE_ARCHIVE.insert().prefix_with("IGNORE"), duplicates)

    engine.dispose()
    print(f"Inserted {len(new_records)} new records into STG_FEEDBACK. Archived {len(duplicates)} duplicates.")

def insert_non_duplicates():
    engine = create_engine(f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['db']}")
    target_metadata.create_all(engine, tables=[FEEDBACK], checkfirst=True)
    insert_sql = """
        INSERT INTO FEEDBACK
        SELECT * FROM STG_FEEDBACK s
        WHERE NOT EXISTS (
            SELECT 1 FROM FEEDBACK o
            WHERE o.feedback_id = s.feedback_id OR o.md5_hash = s.md5_hash
        )
    """
    with engine.begin() as conn:
        conn.execute(insert_sql)
    engine.dispose()

with DAG(
    dag_id='enhanced_feedback_etl_2',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_data_from_api_task',
        python_callable=fetch_data_from_api_callable
    )

    load_to_staging = PythonOperator(
        task_id='load_data_to_staging',
        python_callable=load_data_to_staging
    )

    insert_unique = PythonOperator(
        task_id='insert_non_duplicates_to_final',
        python_callable=insert_non_duplicates
    )

    fetch_data >> load_to_staging >> insert_unique
