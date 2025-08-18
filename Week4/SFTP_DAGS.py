from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timezone
from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Integer, Float, DateTime
)
from sqlalchemy.sql import text
import csv, io, os, paramiko
from hashlib import md5

# Connection helpers
def get_engine():
    conn = BaseHook.get_connection("mysql_stage")  # MySQL connection from Airflow UI
    url = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port or 3306}/{conn.schema}"
    return create_engine(url, pool_pre_ping=True)

def fetch_from_sftp(remote_path: str) -> str:
    conn = BaseHook.get_connection("sftp_build")  # SFTP connection from Airflow UI
    transport = paramiko.Transport((conn.host, conn.port or 22))
    transport.connect(username=conn.login, password=conn.password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        with sftp.file(remote_path, "rb") as f:
            return f.read().decode("utf-8")
    finally:
        sftp.close()
        transport.close()

# Utility functions
def parse_dt(s: str):
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None

def to_int(v): 
    try: return int(v) if v not in (None, "",) else None
    except: return None

def to_float(v): 
    try: return float(v) if v not in (None, "",) else None
    except: return None

def compute_md5(keys, row, dv_ts_iso):
    parts = [str((row.get(k) or "")).strip().lower() for k in keys]
    s = "|".join(parts + [dv_ts_iso])
    return md5(s.encode("utf-8")).hexdigest()


# Dataset configs
DATASETS = {
    "feedback": dict(
        sftp_path="/upload/feedback_dataset.csv",
        final="FEEDBACK", stage="STG_FEEDBACK", archive="FEEDBACK_DUPLICATE_ARCHIVE",
        business_keys=["feedback_id", "order_id"],
        columns=[
            ("feedback_id", String(32)),
            ("order_id", String(32)),
            ("feedback_score", Integer),
            ("feedback_form_sent_date", DateTime),
            ("feedback_answer_date", DateTime),
        ],
        dt_cols=["feedback_form_sent_date","feedback_answer_date"], int_cols=["feedback_score"], float_cols=[]
    ),
    "orders": dict(
        sftp_path="/upload/order_dataset.csv",
        final="ORDERS", stage="STG_ORDERS", archive="ORDERS_DUPLICATE_ARCHIVE",
        business_keys=["order_id"],
        columns=[
            ("order_id", String(32)),
            ("user_name", String(64)),
            ("order_status", String(64)),
            ("order_date", DateTime),
            ("order_approved_date", DateTime),
            ("pickup_date", DateTime),
            ("delivered_date", DateTime),
            ("estimated_time_delivery", DateTime),
        ],
        dt_cols=["order_date","order_approved_date","pickup_date","delivered_date","estimated_time_delivery"], int_cols=[], float_cols=[]
    ),
    "order_items": dict(
        sftp_path="/upload/order_item_dataset.csv",
        final="ORDER_ITEMS", stage="STG_ORDER_ITEMS", archive="ORDER_ITEMS_DUPLICATE_ARCHIVE",
        business_keys=["order_id","order_item_id"],
        columns=[
            ("order_id", String(64)),
            ("order_item_id", Integer),
            ("product_id", String(64)),
            ("seller_id", String(64)),
            ("pickup_limit_date", DateTime),
            ("price", Float),
            ("shipping_cost", Float),
        ],
        dt_cols=["pickup_limit_date"], int_cols=["order_item_id"], float_cols=["price","shipping_cost"]
    ),
    "payments": dict(
        sftp_path="/upload/payment_dataset.csv",
        final="PAYMENTS", stage="STG_PAYMENTS", archive="PAYMENTS_DUPLICATE_ARCHIVE",
        business_keys=["order_id","payment_sequential"],
        columns=[
            ("order_id", String(64)),
            ("payment_sequential", Integer),
            ("payment_type", String(64)),
            ("payment_installments", Integer),
            ("payment_value", Float),
        ],
        dt_cols=[], int_cols=["payment_sequential","payment_installments"], float_cols=["payment_value"]
    ),
    "products": dict(
        sftp_path="/upload/products_dataset.csv",
        final="PRODUCTS", stage="STG_PRODUCTS", archive="PRODUCTS_DUPLICATE_ARCHIVE",
        business_keys=["product_id"],
        columns=[
            ("product_id", String(64)),
            ("product_category", String(64)),
            ("product_name_length", Integer),
            ("product_description_length", Integer),
            ("product_photos_qty", Integer),
            ("product_weight_g", Integer),
            ("product_length_cm", Integer),
            ("product_height_cm", Integer),
            ("product_width_cm", Integer),
        ],
        dt_cols=[], int_cols=["product_name_length","product_description_length","product_photos_qty",
                              "product_weight_g","product_length_cm","product_height_cm","product_width_cm"], float_cols=[]
    ),
    "sellers": dict(
        sftp_path="/upload/seller_dataset.csv",
        final="SELLERS", stage="STG_SELLERS", archive="SELLERS_DUPLICATE_ARCHIVE",
        business_keys=["seller_id"],
        columns=[
            ("seller_id", String(64)),
            ("seller_zip_code", String(20)),
            ("seller_city", String(128)),
            ("seller_state", String(128)),
        ],
        dt_cols=[], int_cols=[], float_cols=[]
    ),
    "users": dict(
        sftp_path="/upload/user_dataset.csv",
        final="USERS", stage="STG_USERS", archive="USERS_DUPLICATE_ARCHIVE",
        business_keys=["user_name"],
        columns=[
            ("user_name", String(64)),
            ("customer_zip_code", String(20)),
            ("customer_city", String(128)),
            ("customer_state", String(128)),
        ],
        dt_cols=[], int_cols=[], float_cols=[]
    ),
}


# DAG 
def build_csv_sftp_to_stage_dag(dataset_name: str, cfg: dict) -> DAG:
    metadata = MetaData()

    def _mk_table(name: str, is_stage: bool):
        cols = [Column(cn, ct) for (cn, ct) in cfg["columns"]]
        if is_stage and dataset_name == "users":
            cols += [
                Column("md5_hash", String(64)),   # no PK in staging USERS
                Column("dv_load_timestamp", DateTime),
                Column("event_id", String(255)),
            ]
        else:
            cols += [
                Column("md5_hash", String(64), primary_key=True),
                Column("dv_load_timestamp", DateTime),
                Column("event_id", String(255)),
            ]
        return Table(name, metadata, *cols)

    STG = _mk_table(cfg["stage"], is_stage=True)
    FINAL = _mk_table(cfg["final"], is_stage=False)
    ARCH = _mk_table(cfg["archive"], is_stage=False)

    def fetch_task_callable(**_):
        return fetch_from_sftp(cfg["sftp_path"])

    def load_to_stage_callable(ti, **_):
        csv_text = ti.xcom_pull(task_ids="fetch_csv")
        if not csv_text:
            return

        # event_id = file name
        event_id = os.path.basename(cfg["sftp_path"])

        rows = list(csv.DictReader(io.StringIO(csv_text)))
        dv_ts = datetime.now(timezone.utc).replace(tzinfo=None)
        dv_ts_iso = dv_ts.strftime("%Y-%m-%dT%H:%M:%S")

        for r in rows:
            for c in cfg["dt_cols"]: r[c] = parse_dt(r.get(c))
            for c in cfg["int_cols"]: r[c] = to_int(r.get(c))
            for c in cfg["float_cols"]: r[c] = to_float(r.get(c))
            r["dv_load_timestamp"] = dv_ts
            r["md5_hash"] = compute_md5(cfg["business_keys"], r, dv_ts_iso)
            r["event_id"] = event_id

        engine = get_engine()
        metadata.create_all(engine, tables=[STG, FINAL, ARCH], checkfirst=True)

        with engine.begin() as conn:
            # skip if this file already processed
            exists = conn.execute(
                text(f"SELECT COUNT(*) FROM {cfg['final']} WHERE event_id = :event_id"),
                {"event_id": event_id}
            ).scalar()
            if exists and exists > 0:
                print(f"⚠️ Skipping load: event_id {event_id} already processed")
                return

            existing = set(x[0] for x in conn.execute(text(f"SELECT md5_hash FROM {cfg['final']}")))
            new_rows, dup_rows = [], []
            for r in rows:
                (dup_rows if r["md5_hash"] in existing else new_rows).append(r)

            if new_rows:
                conn.execute(STG.insert(), new_rows)
            if dup_rows:
                conn.execute(ARCH.insert(), dup_rows)

    def upsert_to_final_callable(**_):
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(f"INSERT IGNORE INTO {cfg['final']} SELECT * FROM {cfg['stage']}"))

    with DAG(
        dag_id=f"sftp_{dataset_name}_to_stage_event",
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["sftp", dataset_name],
    ) as dag:

        fetch = PythonOperator(task_id="fetch_csv", python_callable=fetch_task_callable)
        load = PythonOperator(task_id="load_to_staging", python_callable=load_to_stage_callable)
        insert = PythonOperator(task_id="insert_to_final", python_callable=upsert_to_final_callable)

        fetch >> load >> insert

    return dag


# Build all 7 DAGs
for name, cfg in DATASETS.items():
    globals()[f"dag_{name}"] = build_csv_sftp_to_stage_dag(name, cfg)
