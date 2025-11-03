from __future__ import annotations
import os
import shutil
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt

from airflow import DAG
from airflow.sdk import task, TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import Error as DatabaseError

# -----------------------
# Constants
# -----------------------
OUTPUT_DIR = "/opt/airflow/data"
CUSTOMERS_FILE = "customers.csv"
ORDERS_FILE = "orders.csv"
MERGED_FILE = "merged_orders_customers.csv"
MERGED_TABLE = "orders_customers"
SCHEMA = "week8_demo"

default_args = {
    "owner": "IDS706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# -----------------------
# DAG Definition
# -----------------------
with DAG(
    dag_id="customer_orders_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
) as dag:

    # -----------------------
    # Ingestion TaskGroup
    # -----------------------
    with TaskGroup("ingestion") as ingestion:

        @task(task_id="generate_customers")
        def generate_customers(output_dir: str = OUTPUT_DIR, quantity: int = 50) -> str:
            os.makedirs(output_dir, exist_ok=True)
            data = [
                {
                    "customer_id": i + 1,
                    "name": f"Customer_{i + 1}",
                    "age": 20 + (i % 50),
                    "country": ["USA", "UK", "Canada", "Germany"][i % 4],
                }
                for i in range(quantity)
            ]
            path = os.path.join(output_dir, CUSTOMERS_FILE)
            pd.DataFrame(data).to_csv(path, index=False)
            return path

        @task(task_id="generate_orders")
        def generate_orders(output_dir: str = OUTPUT_DIR, quantity: int = 100) -> str:
            os.makedirs(output_dir, exist_ok=True)
            data = [
                {
                    "order_id": i + 100,
                    "customer_id": (i % 50) + 1,
                    "amount": round(50 + (i * 5.3) % 500, 2),
                    "order_date": f"2025-10-{(i % 28) + 1:02d}",
                }
                for i in range(quantity)
            ]
            path = os.path.join(output_dir, ORDERS_FILE)
            pd.DataFrame(data).to_csv(path, index=False)
            return path

        customers_file = generate_customers()
        orders_file = generate_orders()

    # -----------------------
    # Transformation TaskGroup
    # -----------------------
    with TaskGroup("transform") as transform:

        @task(task_id="merge_customers_orders")
        def merge_customers_orders(customers_path: str, orders_path: str, output_dir: str = OUTPUT_DIR) -> str:
            df_customers = pd.read_csv(customers_path)
            df_orders = pd.read_csv(orders_path)
            df_merged = df_orders.merge(df_customers, on="customer_id", how="left")
            merged_path = os.path.join(output_dir, MERGED_FILE)
            df_merged.to_csv(merged_path, index=False)
            return merged_path

        merged_file = merge_customers_orders(customers_file, orders_file)

    # -----------------------
    # Load into PostgreSQL
    # -----------------------
    @task(task_id="load_to_pg")
    def load_to_pg(csv_path: str, table: str = MERGED_TABLE, conn_id: str = "Postgres") -> int:
        if not os.path.exists(csv_path):
            print(f"CSV file {csv_path} does not exist!")
            return 0

        df = pd.read_csv(csv_path)
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        inserted = 0

        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")

                # define all columns as TEXT for simplicity
                columns = ", ".join([f"{col} TEXT" for col in df.columns])
                cur.execute(f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{table} ({columns});")

                cur.execute(f"DELETE FROM {SCHEMA}.{table};")

                for row in df.itertuples(index=False, name=None):
                    placeholders = ", ".join(["%s"] * len(row))
                    cur.execute(f"INSERT INTO {SCHEMA}.{table} VALUES ({placeholders});", row)

                conn.commit()
                inserted = len(df)
                print(f"Inserted {inserted} rows into {SCHEMA}.{table}")

        except DatabaseError as e:
            print(f"Database error: {e}")
            conn.rollback()

        finally:
            conn.close()

        return inserted

    load_db = load_to_pg(merged_file)

    # -----------------------
    # Analysis TaskGroup
    # -----------------------
    with TaskGroup("analysis") as analysis:

        @task(task_id="analyze_orders")
        def analyze_orders(conn_id: str = "Postgres") -> str:
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            hook = PostgresHook(postgres_conn_id=conn_id)
            df = hook.get_pandas_df(f"SELECT * FROM {SCHEMA}.{MERGED_TABLE};")

            # Convert amount to numeric
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
            df = df.dropna(subset=["amount"])

            if df.empty:
                print("No valid numeric data to plot.")
                return "no_data"

            summary = df.groupby("country")["amount"].sum()
            plot_path = os.path.join(OUTPUT_DIR, "orders_per_country.png")

            summary.plot(kind="bar", title="Total Orders per Country")
            plt.ylabel("Total Amount")
            plt.tight_layout()
            plt.savefig(plot_path)
            plt.close()

            print(f"✅ Analysis plot saved to {plot_path}")
            return plot_path

        analysis_file = analyze_orders()

    # -----------------------
    # Cleanup
    # -----------------------
    @task(task_id="cleanup")
    def cleanup(folder_path: str = OUTPUT_DIR) -> None:
        if not os.path.exists(folder_path):
            return
        for f in os.listdir(folder_path):
            path = os.path.join(folder_path, f)
            if os.path.isfile(path):
                os.remove(path)
        print("Cleanup complete.")

    clean = cleanup()

    # -----------------------
    # DAG Dependencies
    # -----------------------
    ingestion >> transform >> load_db >> analysis >> clean
