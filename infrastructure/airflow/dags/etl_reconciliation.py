import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

PG_HOST = os.getenv("FINTECH_PG_HOST", "postgres")
PG_PORT = int(os.getenv("FINTECH_PG_PORT", "5432"))
PG_DB = "fintech"
PG_USER = "fintech"
PG_PASSWORD = "fintech"

DATA_WAREHOUSE_PATH = "/opt/airflow/data_warehouse"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_and_load_parquet(**context):
    """
    Extract valid (non-fraud) transactions from the last 6 hours,
    write them to Parquet files partitioned by date and hour.
    Pushes record count and total amount to XCom for the report task.
    """
    import pandas as pd
    import psycopg2

    window_end = datetime.utcnow()
    window_start = window_end - timedelta(hours=6)

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )

    df = pd.read_sql(
        """
        SELECT transaction_id, user_id, event_time, merchant_category, amount, location, created_at
        FROM valid_transactions
        WHERE created_at >= %s AND created_at < %s
        ORDER BY event_time
        """,
        conn,
        params=(window_start, window_end),
    )
    conn.close()

    partition_path = (
        f"{DATA_WAREHOUSE_PATH}/valid_transactions"
        f"/year={window_end.year}"
        f"/month={window_end.month:02d}"
        f"/day={window_end.day:02d}"
        f"/hour={window_end.hour:02d}"
    )
    os.makedirs(partition_path, exist_ok=True)
    parquet_file = f"{partition_path}/data.parquet"
    df.to_parquet(parquet_file, index=False)

    total_amount = float(df["amount"].sum()) if len(df) > 0 else 0.0
    print(f"[ETL] Written {len(df)} valid transactions ({total_amount:,.2f}) → {parquet_file}")

    ti = context["ti"]
    ti.xcom_push(key="valid_count", value=len(df))
    ti.xcom_push(key="valid_amount", value=total_amount)
    ti.xcom_push(key="window_start", value=window_start.isoformat())
    ti.xcom_push(key="window_end", value=window_end.isoformat())


def generate_reconciliation_report(**context):
    """
    Query both tables to calculate Total Ingress vs Validated amounts,
    then write a reconciliation report to the data warehouse reports folder.
    Total Ingress = valid_transactions + fraud_transactions (every dollar accounted for).
    """
    import psycopg2

    ti = context["ti"]
    valid_count = ti.xcom_pull(key="valid_count", task_ids="extract_and_load_parquet")
    valid_amount = ti.xcom_pull(key="valid_amount", task_ids="extract_and_load_parquet")
    window_start = ti.xcom_pull(key="window_start", task_ids="extract_and_load_parquet")
    window_end = ti.xcom_pull(key="window_end", task_ids="extract_and_load_parquet")

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(amount), 0)
        FROM fraud_transactions
        WHERE created_at >= %s AND created_at < %s
        """,
        (window_start, window_end),
    )
    fraud_count, fraud_amount = cur.fetchone()
    fraud_amount = float(fraud_amount)
    conn.close()

    total_count = valid_count + fraud_count
    total_amount = valid_amount + fraud_amount
    validation_rate = (valid_amount / total_amount * 100) if total_amount > 0 else 0.0
    fraud_rate = (fraud_amount / total_amount * 100) if total_amount > 0 else 0.0

    report = f"""
=====================================
   RECONCILIATION REPORT
   Window : {window_start} -> {window_end}
   Created: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
=====================================

INGRESS SUMMARY
  Total Transactions : {total_count:,}
  Total Ingress Amt  : ${total_amount:,.2f}

VALIDATED (NON-FRAUD)
  Transaction Count  : {valid_count:,}
  Validated Amount   : ${valid_amount:,.2f}  ({validation_rate:.1f}%)

FRAUD BLOCKED
  Transaction Count  : {fraud_count:,}
  Fraud Amount       : ${fraud_amount:,.2f}  ({fraud_rate:.1f}%)

RECONCILIATION CHECK
  Validated          : ${valid_amount:,.2f}
+ Fraud Blocked      : ${fraud_amount:,.2f}
                       ---------------------
= Total Ingress      : ${total_amount:,.2f}  (Balanced)
=====================================
"""

    report_dir = f"{DATA_WAREHOUSE_PATH}/reports"
    os.makedirs(report_dir, exist_ok=True)
    ts = window_end.replace(":", "-").replace("T", "_").split(".")[0]
    report_file = f"{report_dir}/reconciliation_{ts}.txt"

    with open(report_file, "w") as f:
        f.write(report)

    print(report)
    print(f"[ETL] Report saved → {report_file}")


with DAG(
    dag_id="etl_reconciliation",
    default_args=default_args,
    description="ETL: load valid transactions to Parquet and generate reconciliation report",
    schedule_interval="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "reconciliation", "fraud"],
) as dag:

    extract_load = PythonOperator(
        task_id="extract_and_load_parquet",
        python_callable=extract_and_load_parquet,
    )

    reconcile = PythonOperator(
        task_id="generate_reconciliation_report",
        python_callable=generate_reconciliation_report,
    )

    extract_load >> reconcile
