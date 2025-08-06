from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from sqlalchemy import Table, Column, String, DateTime, MetaData, create_engine
import pymysql
from hashlib import md5

# --- Table Definitions (Capitalized) ---
target_metadata = MetaData()

STG_ORDERS = Table(
    "STG_ORDERS",
    target_metadata,
    Column("order_id", String(32), primary_key=True),
    Column("user_name", String(64)),
    Column("order_status", String(64)),
    Column("order_date", DateTime),
    Column("order_approved_date", DateTime),
    Column("pickup_date", DateTime),
    Column("delivered_date", DateTime),
    Column("estimated_time_delivery", DateTime),
    Column("md5_hash", String(64)),
    Column("dv_load_timestamp", DateTime),
)

ORDERS = Table(
    "ORDERS",
    target_metadata,
    Column("order_id", String(32), primary_key=True),
    Column("user_name", String(64)),
    Column("order_status", String(64)),
    Column("order_date", DateTime),
    Column("order_approved_date", DateTime),
    Column("pickup_date", DateTime),
    Column("delivered_date", DateTime),
    Column("estimated_time_delivery", DateTime),
    Column("md5_hash", String(64)),
    Column("dv_load_timestamp", DateTime),
)

ORDERS_DUPLICATE_ARCHIVE = Table(
    "ORDERS_DUPLICATE_ARCHIVE",
    target_metadata,
    Column("order_id", String(32), primary_key=True),
    Column("user_name", String(64)),
    Column("order_status", String(64)),
    Column("order_date", DateTime),
    Column("order_approved_date", DateTime),
    Column("pickup_date", DateTime),
    Column("delivered_date", DateTime),
    Column("estimated_time_delivery", DateTime),
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
    try:
        return datetime.strptime(value, "%m/%d/%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(value, "%m/%d/%Y").strftime("%Y-%m-%d %H:%M:%S")
        except:
            return None

def fetch_data_from_api_callable():
    endpoint = f"{API_BASE_URL}/orders/"
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
    target_metadata.create_all(engine, tables=[STG_ORDERS, ORDERS_DUPLICATE_ARCHIVE], checkfirst=True)

    with engine.begin() as conn:
        existing_hashes = set(row[0] for row in conn.execute("SELECT md5_hash FROM ORDERS").fetchall())
        new_records = []
        duplicates = []

        for row in data:
            row["order_date"] = format_datetime(row.get("order_date"))
            row["order_approved_date"] = format_datetime(row.get("order_approved_date"))
            row["pickup_date"] = format_datetime(row.get("pickup_date"))
            row["delivered_date"] = format_datetime(row.get("delivered_date"))
            row["estimated_time_delivery"] = format_datetime(row.get("estimated_time_delivery"))
            row["md5_hash"] = compute_md5(row)
            row["dv_load_timestamp"] = now

            if row["md5_hash"] in existing_hashes:
                duplicates.append(row)
            else:
                new_records.append(row)

        if new_records:
            conn.execute(STG_ORDERS.insert(), new_records)
        if duplicates:
            conn.execute(ORDERS_DUPLICATE_ARCHIVE.insert().prefix_with("IGNORE"), duplicates)

    engine.dispose()
    print(f"Inserted {len(new_records)} new records into STG_ORDERS. Archived {len(duplicates)} duplicates.")

def insert_non_duplicates():
    engine = create_engine(f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['db']}")
    target_metadata.create_all(engine, tables=[ORDERS], checkfirst=True)
    insert_sql = """
        INSERT INTO ORDERS
        SELECT * FROM STG_ORDERS s
        WHERE NOT EXISTS (
            SELECT 1 FROM ORDERS o
            WHERE o.order_id = s.order_id OR o.md5_hash = s.md5_hash
        )
    """
    with engine.begin() as conn:
        conn.execute(insert_sql)
    engine.dispose()

with DAG(
    dag_id='api_orders_week3',
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
