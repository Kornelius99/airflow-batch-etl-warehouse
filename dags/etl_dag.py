"""
Batch ELT DAG: extract orders from the source Postgres, load them raw into
the warehouse, run dbt to build staging/mart models, then run dbt tests.

This DAG intentionally uses plain psycopg2/pandas for the extract-load step
(so it is easy to follow for learners) and hands transformation logic over
to dbt, which is the pattern most production ELT stacks use in practice.
"""

from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SOURCE_CONN = dict(
    host="postgres-source", dbname="source_db", user="source_user", password="source_pass"
)
WAREHOUSE_CONN = dict(
    host="postgres-warehouse", dbname="warehouse", user="wh_user", password="wh_pass"
)

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def extract_and_load_raw(**context):
    """Extract all orders from the source DB and load them into a raw schema
    in the warehouse, replacing the table on each run (simple full-refresh)."""
    with psycopg2.connect(**SOURCE_CONN) as src_conn:
        df = pd.read_sql("SELECT * FROM orders", src_conn)

    with psycopg2.connect(**WAREHOUSE_CONN) as wh_conn:
        with wh_conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw.orders (
                    order_id INT PRIMARY KEY,
                    customer_id INT,
                    order_date DATE,
                    region VARCHAR(50),
                    product_category VARCHAR(50),
                    quantity INT,
                    unit_price NUMERIC(10, 2),
                    total_amount NUMERIC(10, 2)
                );
                """
            )
            cur.execute("TRUNCATE TABLE raw.orders;")
            for _, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO raw.orders
                        (order_id, customer_id, order_date, region, product_category,
                         quantity, unit_price, total_amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    tuple(row),
                )
        wh_conn.commit()

    context["ti"].xcom_push(key="rows_loaded", value=len(df))
    print(f"Loaded {len(df)} raw rows into warehouse.")


with DAG(
    dag_id="batch_etl_warehouse",
    description="Extract orders -> load raw -> dbt run -> dbt test",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["portfolio", "elt", "dbt"],
) as dag:

    extract_load = PythonOperator(
        task_id="extract_and_load_raw",
        python_callable=extract_and_load_raw,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt_warehouse && "
            "dbt run --profiles-dir /opt/airflow/dbt_warehouse"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt_warehouse && "
            "dbt test --profiles-dir /opt/airflow/dbt_warehouse"
        ),
    )

    extract_load >> dbt_run >> dbt_test
