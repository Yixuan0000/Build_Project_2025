from airflow import DAG
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime
import os

SQL_DIR = "/home/yy486/dags/sql"


with DAG(
    dag_id="dwh_load_from_sql",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["project5", "dwh"]
) as dag:

    start = DummyOperator(task_id="start")

    create_dwh_tables = MySqlOperator(
        task_id="create_tables",
        mysql_conn_id="mysql_stage",
        sql=os.path.join(SQL_DIR, "create_tables.sql"),
    )

    load_dim_users = MySqlOperator(
        task_id="load_dim_users",
        mysql_conn_id="mysql_stage",
        sql=os.path.join(SQL_DIR, "load_dim_users.sql"),
    )

    load_dim_sellers = MySqlOperator(
        task_id="load_dim_sellers",
        mysql_conn_id="mysql_stage",
        sql=os.path.join(SQL_DIR, "load_dim_sellers.sql"),
    )

    load_dim_products = MySqlOperator(
        task_id="load_dim_products",
        mysql_conn_id="mysql_stage",
        sql=os.path.join(SQL_DIR, "load_dim_products.sql"),
    )

    load_dim_orders = MySqlOperator(
        task_id="load_dim_orders",
        mysql_conn_id="mysql_stage",
        sql=os.path.join(SQL_DIR, "load_dim_orders.sql"),
    )

    load_dim_feedback = MySqlOperator(
        task_id="load_dim_feedback",
        mysql_conn_id="mysql_stage",
        sql=os.path.join(SQL_DIR, "load_dim_feedback.sql"),
    )

    load_dim_time = MySqlOperator(
        task_id="load_dim_time",
        mysql_conn_id="mysql_stage",
        sql=os.path.join(SQL_DIR, "load_dim_time.sql"),
    )

    load_dim_payments = MySqlOperator(
        task_id="load_dim_payments",
        mysql_conn_id="mysql_stage",
        sql=os.path.join(SQL_DIR, "load_dim_payment.sql"),
    )

    load_fact_sales = MySqlOperator(
        task_id="load_fact_sales",
        mysql_conn_id="mysql_stage",
        sql=os.path.join(SQL_DIR, "load_fact_sales.sql"),
    )

    dims_done = DummyOperator(task_id="dimensions_loaded")
    end = DummyOperator(task_id="end")

    start >> create_dwh_tables >> [
        load_dim_users,
        load_dim_sellers,
        load_dim_products,
        load_dim_orders,
        load_dim_feedback,
        load_dim_time,
        load_dim_payments
    ] >> dims_done >> load_fact_sales >> end
