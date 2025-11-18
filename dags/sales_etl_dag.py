from datetime import datetime
import os
import csv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Ustawienia DAG-a
default_args = {
    "owner": "filip",
    "start_date": datetime(2024, 11, 1),
}

with DAG(
    dag_id="sales_etl_dag",
    default_args=default_args,
    schedule_interval=None,  # uruchamiamy ręcznie
    catchup=False,
    description="ETL: CSV -> Postgres (raw + dzienne + dzienne po kategoriach)",
) as dag:

    # 1) Tworzenie tabel, jeśli nie istnieją
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgres_business",
        sql="""
        CREATE TABLE IF NOT EXISTS product_dim (
            product_name VARCHAR(50) PRIMARY KEY,
            category     VARCHAR(50) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sales_raw (
            id SERIAL PRIMARY KEY,
            sale_date DATE NOT NULL,
            product   VARCHAR(50) NOT NULL,
            amount    NUMERIC(10, 2) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sales_daily_agg (
            sale_date DATE PRIMARY KEY,
            total_amount NUMERIC(10, 2),
            avg_amount   NUMERIC(10, 2),
            transactions_count INT
        );

        CREATE TABLE IF NOT EXISTS sales_category_agg (
            sale_date DATE NOT NULL,
            category  VARCHAR(50) NOT NULL,
            total_amount NUMERIC(10, 2),
            transactions_count INT,
            PRIMARY KEY (sale_date, category)
        );
        """,
    )

    # 2) Ładowanie wymiaru produktów z CSV do product_dim
    def load_products_from_csv(**context):
        csv_path = os.path.join(
            os.path.dirname(__file__),
            "data",
            "products.csv",
        )

        hook = PostgresHook(postgres_conn_id="postgres_business")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Za każdym razem czyścimy wymiar i ładujemy na nowo (mały, referencyjny słownik)
        cursor.execute("TRUNCATE TABLE product_dim;")

        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute(
                    """
                    INSERT INTO product_dim (product_name, category)
                    VALUES (%s, %s)
                    """,
                    (row["product"], row["category"]),
                )

        conn.commit()
        cursor.close()
        conn.close()

    load_product_dim = PythonOperator(
        task_id="load_product_dim_from_csv",
        python_callable=load_products_from_csv,
    )

    # 3) Ładowanie danych sprzedażowych z CSV do sales_raw
    def load_sales_from_csv(**context):
        csv_path = os.path.join(
            os.path.dirname(__file__),
            "data",
            "sales_data.csv",
        )

        hook = PostgresHook(postgres_conn_id="postgres_business")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Czyścimy tabelę, żeby ładowanie było powtarzalne
        cursor.execute("TRUNCATE TABLE sales_raw;")

        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute(
                    """
                    INSERT INTO sales_raw (sale_date, product, amount)
                    VALUES (%s, %s, %s)
                    """,
                    (row["sale_date"], row["product"], row["amount"]),
                )

        conn.commit()
        cursor.close()
        conn.close()

    load_raw = PythonOperator(
        task_id="load_raw_from_csv",
        python_callable=load_sales_from_csv,
    )

    # 4) Agregacja dzienna (bez kategorii)
    aggregate_daily = PostgresOperator(
        task_id="aggregate_daily_sales",
        postgres_conn_id="postgres_business",
        sql="""
        INSERT INTO sales_daily_agg (sale_date, total_amount, avg_amount, transactions_count)
        SELECT
            sale_date,
            SUM(amount) AS total_amount,
            AVG(amount) AS avg_amount,
            COUNT(*)    AS transactions_count
        FROM sales_raw
        GROUP BY sale_date
        ON CONFLICT (sale_date) DO UPDATE
        SET
            total_amount       = EXCLUDED.total_amount,
            avg_amount         = EXCLUDED.avg_amount,
            transactions_count = EXCLUDED.transactions_count;
        """,
    )

    # 5) Agregacja dzienna po kategoriach (JOIN z product_dim)
    aggregate_by_category = PostgresOperator(
        task_id="aggregate_daily_sales_by_category",
        postgres_conn_id="postgres_business",
        sql="""
        INSERT INTO sales_category_agg (sale_date, category, total_amount, transactions_count)
        SELECT
            sr.sale_date,
            p.category,
            SUM(sr.amount) AS total_amount,
            COUNT(*)       AS transactions_count
        FROM sales_raw sr
        JOIN product_dim p
          ON p.product_name = sr.product
        GROUP BY sr.sale_date, p.category
        ON CONFLICT (sale_date, category) DO UPDATE
        SET
            total_amount       = EXCLUDED.total_amount,
            transactions_count = EXCLUDED.transactions_count;
        """,
    )

    # Kolejność tasków:
    # 1) Tworzymy tabele
    # 2) Ładujemy wymiar produktów
    # 3) Ładujemy dane sprzedaży
    # 4) Liczymy agregat dzienny
    # 5) Liczymy agregat dzienny po kategoriach
    create_tables >> load_product_dim >> load_raw >> aggregate_daily >> aggregate_by_category
