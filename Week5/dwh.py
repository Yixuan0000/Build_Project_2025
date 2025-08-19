from airflow import DAG
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

with DAG(
    dag_id="dwh_load_from_stage",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["project5", "dwh"]
) as dag:

    start = DummyOperator(task_id="start")

    # === Create DWH Tables ===
    create_dwh_tables = MySqlOperator(
        task_id="create_dwh_tables",
        mysql_conn_id="mysql_stage",
        sql="""
        CREATE SCHEMA IF NOT EXISTS dwh;

        CREATE TABLE IF NOT EXISTS dwh.dim_users (
          userid VARCHAR(64) PRIMARY KEY,
          userzipcode VARCHAR(20),
          usercity VARCHAR(128),
          userstate VARCHAR(128)
        );

        CREATE TABLE IF NOT EXISTS dwh.dim_sellers (
          sellerid VARCHAR(64) PRIMARY KEY,
          sellerzipcode VARCHAR(20),
          sellercity VARCHAR(128),
          sellerstate VARCHAR(128)
        );

        CREATE TABLE IF NOT EXISTS dwh.dim_products (
          productid VARCHAR(64) PRIMARY KEY,
          productcategory VARCHAR(64),
          productnamelength INT,
          productdescriptionlength INT,
          productphotosqty INT,
          productweight_g INT,
          productlength_cm INT,
          productheight_cm INT,
          productwidth_cm INT
        );

        CREATE TABLE IF NOT EXISTS dwh.dim_orders (
          orderid VARCHAR(64) PRIMARY KEY,
          orderstatus VARCHAR(64),
          orderdate DATETIME,
          orderapproveddate DATETIME,
          pickupdate DATETIME,
          delivereddate DATETIME,
          estimatedtimedelivery DATETIME
        );

        CREATE TABLE IF NOT EXISTS dwh.dim_feedback (
          feedbackid VARCHAR(64) PRIMARY KEY,
          orderid VARCHAR(64),
          feedbackscore INT,
          feedbackformsentdate DATETIME,
          feedbackanswerdate DATETIME
        );

        CREATE TABLE IF NOT EXISTS dwh.dim_time (
          timekey INT PRIMARY KEY,
          fulldate DATE,
          year SMALLINT,
          quarter TINYINT,
          month TINYINT,
          day TINYINT,
          dayofweek TINYINT
        );

        CREATE TABLE IF NOT EXISTS dwh.dim_payments (
          paymentid BIGINT PRIMARY KEY,
          paymentsequential INT,
          paymenttype VARCHAR(64),
          paymentinstallments INT
        );

        CREATE TABLE IF NOT EXISTS dwh.fact_sales (
          orderid VARCHAR(64),
          productid VARCHAR(64),
          userid VARCHAR(64),
          sellerid VARCHAR(64),
          paymentid BIGINT,
          timekey INT,
          price DECIMAL(10,2),
          shippingcost DECIMAL(10,2),
          paymentvalue DECIMAL(10,2),
          KEY idx_time (timekey),
          KEY idx_order (orderid),
          KEY idx_product (productid),
          KEY idx_payment (paymentid)
        );
        """
    )

    # === Dimension Loads ===
    load_dim_users = MySqlOperator(
        task_id="load_dim_users",
        mysql_conn_id="mysql_stage",
        sql="""
        TRUNCATE TABLE dwh.dim_users;
        INSERT INTO dwh.dim_users (userid, userzipcode, usercity, userstate)
        SELECT user_name, customer_zip_code, UPPER(customer_city), UPPER(customer_state)
        FROM (
            SELECT user_name, customer_zip_code, customer_city, customer_state,
                   ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY user_name) AS rn
            FROM STAGELOAD.USERS
        ) t
        WHERE rn = 1;
        """
    )

    load_dim_sellers = MySqlOperator(
        task_id="load_dim_sellers",
        mysql_conn_id="mysql_stage",
        sql="""
        TRUNCATE TABLE dwh.dim_sellers;
        INSERT INTO dwh.dim_sellers (sellerid, sellerzipcode, sellercity, sellerstate)
        SELECT DISTINCT seller_id, seller_zip_code, UPPER(seller_city), UPPER(seller_state)
        FROM STAGELOAD.SELLERS;
        """
    )

    load_dim_products = MySqlOperator(
        task_id="load_dim_products",
        mysql_conn_id="mysql_stage",
        sql="""
        TRUNCATE TABLE dwh.dim_products;
        INSERT INTO dwh.dim_products (
            productid, productcategory, productnamelength, productdescriptionlength,
            productphotosqty, productweight_g, productlength_cm, productheight_cm, productwidth_cm
        )
        SELECT
            product_id,
            COALESCE(product_category, 'N/A'),
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        FROM STAGELOAD.PRODUCTS;
        """
    )

    load_dim_orders = MySqlOperator(
        task_id="load_dim_orders",
        mysql_conn_id="mysql_stage",
        sql="""
        TRUNCATE TABLE dwh.dim_orders;
        INSERT INTO dwh.dim_orders (
            orderid, orderstatus, orderdate, orderapproveddate,
            pickupdate, delivereddate, estimatedtimedelivery
        )
        SELECT
            order_id, order_status, order_date, order_approved_date,
            pickup_date, delivered_date, estimated_time_delivery
        FROM STAGELOAD.ORDERS;
        """
    )

    load_dim_feedback = MySqlOperator(
        task_id="load_dim_feedback",
        mysql_conn_id="mysql_stage",
        sql="""
        TRUNCATE TABLE dwh.dim_feedback;
        INSERT INTO dwh.dim_feedback (
            feedbackid, orderid, feedbackscore, feedbackformsentdate, feedbackanswerdate
        )
        SELECT feedback_id, order_id, feedback_score, feedback_form_sent_date, feedback_answer_date
        FROM (
            SELECT feedback_id, order_id, feedback_score, feedback_form_sent_date, feedback_answer_date,
                   ROW_NUMBER() OVER (PARTITION BY feedback_id ORDER BY feedback_id) AS rn
            FROM STAGELOAD.FEEDBACK
        ) t
        WHERE rn = 1;
        """
    )

    load_dim_time = MySqlOperator(
        task_id="load_dim_time",
        mysql_conn_id="mysql_stage",
        sql="""
        TRUNCATE TABLE dwh.dim_time;

        INSERT INTO dwh.dim_time (timekey, fulldate, year, quarter, month, day, dayofweek)
        SELECT
          CAST(DATE_FORMAT(CalendarDate, '%Y%m%d') AS UNSIGNED),
          CalendarDate,
          YEAR(CalendarDate),
          QUARTER(CalendarDate),
          MONTH(CalendarDate),
          DAY(CalendarDate),
          DAYOFWEEK(CalendarDate)
        FROM (
          SELECT DISTINCT DATE(order_date) AS CalendarDate FROM STAGELOAD.ORDERS WHERE order_date IS NOT NULL
          UNION SELECT DISTINCT DATE(order_approved_date) FROM STAGELOAD.ORDERS WHERE order_approved_date IS NOT NULL
          UNION SELECT DISTINCT DATE(pickup_date) FROM STAGELOAD.ORDERS WHERE pickup_date IS NOT NULL
          UNION SELECT DISTINCT DATE(delivered_date) FROM STAGELOAD.ORDERS WHERE delivered_date IS NOT NULL
        ) AS AllDates;
        """
    )

    load_dim_payments = MySqlOperator(
        task_id="load_dim_payments",
        mysql_conn_id="mysql_stage",
        sql="""
        DROP TABLE IF EXISTS temp_payment_mapping;
        CREATE TABLE temp_payment_mapping AS
        SELECT
          ROW_NUMBER() OVER (ORDER BY order_id, payment_sequential) AS paymentid,
          order_id,
          payment_sequential,
          REPLACE(payment_type, '_', ' ') AS payment_type_clean,
          payment_installments,
          payment_value
        FROM STAGELOAD.PAYMENTS;

        TRUNCATE TABLE dwh.dim_payments;
        INSERT INTO dwh.dim_payments (paymentid, paymentsequential, paymenttype, paymentinstallments)
        SELECT paymentid, payment_sequential, payment_type_clean, payment_installments
        FROM temp_payment_mapping;
        """
    )

    # === Fact Load ===
    load_fact_sales = MySqlOperator(
        task_id="load_fact_sales",
        mysql_conn_id="mysql_stage",
        sql="""
        TRUNCATE TABLE dwh.fact_sales;

        INSERT INTO dwh.fact_sales
          (orderid, productid, userid, sellerid, paymentid, timekey, price, shippingcost, paymentvalue)
        SELECT
          oi.order_id,
          oi.product_id,
          o.user_name AS userid,
          oi.seller_id,
          pm.paymentid,
          CAST(DATE_FORMAT(DATE(o.order_date), '%Y%m%d') AS UNSIGNED) AS timekey,
          oi.price,
          oi.shipping_cost,
          pm.payment_value
        FROM STAGELOAD.ORDER_ITEMS oi
        JOIN STAGELOAD.ORDERS o
          ON oi.order_id = o.order_id
        LEFT JOIN temp_payment_mapping pm
          ON oi.order_id = pm.order_id AND pm.payment_sequential = 1;

        DROP TABLE IF EXISTS temp_payment_mapping;
        """
    )

    dims_done = DummyOperator(task_id="dimensions_loaded")
    end = DummyOperator(task_id="end")

    # === Dependencies ===
    start >> create_dwh_tables >> [
        load_dim_users,
        load_dim_sellers,
        load_dim_products,
        load_dim_orders,
        load_dim_feedback,
        load_dim_time,
        load_dim_payments
    ] >> dims_done >> load_fact_sales >> end
