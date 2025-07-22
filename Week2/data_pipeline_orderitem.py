from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from sqlalchemy import Table, Column, String, Integer, Float, MetaData, create_engine, DateTime, PrimaryKeyConstraint
import pymysql

# --- Define your target table schema (edit this!) ---
target_metadata = MetaData()

order_items_table = Table(
    "ORDER_ITEMS",
    target_metadata,
    Column("order_id", String(64)),
    Column("order_item_id", Integer),
    Column("product_id", String(64)),
    Column("seller_id", String(64)),
    Column("pickup_limit_date", DateTime),
    Column("price", Float),
    Column("shipping_cost", Float),
    PrimaryKeyConstraint("order_id", "order_item_id")
)



# --- API Connection Details (edit these) ---
API_BASE_URL = "http://34.16.77.121:1515"
API_USERNAME = "student1"
API_PASSWORD = "pass123"

# --- MySQL Database Connection Details (edit these) ---
MYSQL_HOST = "34.121.169.125"            # Or your Docker service name if running via containers
MYSQL_PORT = 3306
MYSQL_DB_NAME = "STAGELOAD"  
MYSQL_USERNAME = "build2025yixuan"
MYSQL_PASSWORD = "WAkuro2!googlecloud"

# --- Function to fetch data from API ---
def fetch_data_from_api_callable():
    endpoint = f"{API_BASE_URL}/order_items/"  # Replace "some_endpoint" appropriately
    print(f"Fetching data from: {endpoint}")
    response = requests.get(endpoint, auth=(API_USERNAME, API_PASSWORD))
    response.raise_for_status()
    return response.text

# --- Function to format datetime strings ---
def format_datetime(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


# --- Function to load data into MySQL ---
def load_data_to_db(ti):
    fetched_data_json = ti.xcom_pull(task_ids='fetch_data_from_api_task')
    
    if not fetched_data_json:
        print("No data fetched. Exiting load process.")
        return

    data_to_load = json.loads(fetched_data_json)

    if not data_to_load:
        print("API returned empty data. Nothing to load.")
        return

    for row in data_to_load:
        row["pickup_limit_date"] = format_datetime(row.get("pickup_limit_date"))


    db_url = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}"
    engine = create_engine(db_url)

    target_metadata.create_all(engine, tables=[order_items_table], checkfirst=True)

    with engine.begin() as conn:
       conn.execute(order_items_table.insert(), data_to_load)

    engine.dispose()
    print(f"Loaded {len(data_to_load)} records into '{order_items_table.name}'.")

# --- Define the DAG ---
with DAG(
    dag_id='api_to_db_pipeline_order_items',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['data_pipeline', 'api_integration', 'mysql'],
    doc_md="""
    ### API to Database Data Pipeline Sample
    This DAG fetches data from an API and loads it into a MySQL database.
    
    **Instructions:**
    1. Replace placeholders for API and MySQL connection details.
    2. Adjust the table schema to match the API response.
    3. Modify endpoint in `fetch_data_from_api_callable`.
    """
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