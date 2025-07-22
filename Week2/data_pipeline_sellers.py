from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from sqlalchemy import Table, Column, String, MetaData, create_engine
import pymysql

# --- Table schema ---
target_metadata = MetaData()

sellers_table = Table(
    "SELLERS",
    target_metadata,
    Column("seller_id", String(64), primary_key=True),
    Column("seller_zip_code", String(20)),
    Column("seller_city", String(128)),
    Column("seller_state", String(128))
)

# --- API connection ---
API_BASE_URL = "http://34.16.77.121:1515"
API_USERNAME = "student1"
API_PASSWORD = "pass123"

# --- MySQL connection ---
MYSQL_HOST = "34.121.169.125"
MYSQL_PORT = 3306
MYSQL_DB_NAME = "STAGELOAD"
MYSQL_USERNAME = "build2025yixuan"
MYSQL_PASSWORD = "WAkuro2!googlecloud"

# --- Fetch from API ---
def fetch_data_from_api_callable():
    endpoint = f"{API_BASE_URL}/sellers/"
    print(f"Fetching data from: {endpoint}")
    response = requests.get(endpoint, auth=(API_USERNAME, API_PASSWORD))
    response.raise_for_status()
    return response.text

# --- Load into MySQL ---
def load_data_to_db(ti):
    fetched_data_json = ti.xcom_pull(task_ids='fetch_data_from_api_task')
    
    if not fetched_data_json:
        print("No data fetched. Exiting load process.")
        return

    data_to_load = json.loads(fetched_data_json)

    if not data_to_load:
        print("API returned empty data. Nothing to load.")
        return

    db_url = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}"
    engine = create_engine(db_url)

    target_metadata.create_all(engine, tables=[sellers_table], checkfirst=True)

    with engine.begin() as conn:
        conn.execute(sellers_table.insert(), data_to_load)

    engine.dispose()
    print(f"Loaded {len(data_to_load)} records into '{sellers_table.name}'.")

# --- DAG definition ---
with DAG(
    dag_id='api_to_db_pipeline_sellers',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['data_pipeline', 'api_integration', 'mysql'],
) as dag:

    fetch_data_from_api_task = PythonOperator(
        task_id='fetch_data_from_api_task',
        python_callable=fetch_data_from_api_callable,
    )

    load_data_to_db_task = PythonOperator(
        task_id='load_data_to_db_task',
        python_callable=load_data_to_db,
    )

    fetch_data_from_api_task >> load_data_to_db_task
