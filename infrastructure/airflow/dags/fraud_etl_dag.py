from __future__ import annotations
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    from kafka import KafkaConsumer
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import psycopg2
except Exception:
    # When Airflow runs without dependencies installed, tasks will fail with clear message
    KafkaConsumer = None
    pd = None
    pq = None
    psycopg2 = None


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DEFAULT_WAREHOUSE = Path(os.getenv("AIRFLOW_WAREHOUSE", "/opt/airflow/warehouse"))
DEFAULT_REPORTS = Path(os.getenv("AIRFLOW_REPORTS", "/opt/airflow/reports"))
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "fintech")
PG_USER = os.getenv("PG_USER", "fintech")
PG_PASSWORD = os.getenv("PG_PASSWORD", "fintech")


def ensure_dirs():
    DEFAULT_WAREHOUSE.mkdir(parents=True, exist_ok=True)
    DEFAULT_REPORTS.mkdir(parents=True, exist_ok=True)


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _filter_window(frame, start: str, end: str):
    if frame.empty or "timestamp" not in frame.columns:
        return frame

    window_start = _parse_iso(start)
    window_end = _parse_iso(end)
    frame = frame.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce", utc=True)
    return frame[(frame["timestamp"] >= window_start) & (frame["timestamp"] < window_end)]


def consume_topic_to_parquet(topic: str, filename: str, window_start: str, window_end: str, max_messages: int = 10000, timeout: int = 30):
    if KafkaConsumer is None or pd is None or pq is None:
        raise RuntimeError("Missing dependencies: install kafka-python, pandas, pyarrow in Airflow environment")

    ensure_dirs()
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=timeout * 1000,
    )

    rows: List[dict] = []
    for i, msg in enumerate(consumer):
        rows.append(msg.value)
        if i + 1 >= max_messages:
            break

    consumer.close()

    df = pd.DataFrame(rows)
    df = _filter_window(df, window_start, window_end)
    out_path = DEFAULT_WAREHOUSE / filename
    if df.empty:
        # write an empty parquet with schema if possible
        df = pd.DataFrame(columns=["transaction_id", "user_id", "timestamp", "merchant_category", "amount", "location"])

    table = pa.Table.from_pandas(df)
    pq.write_table(table, str(out_path))


def compute_reconciliation(ingress_file: str, validated_file: str, report_name: str, window_start: str, window_end: str):
    if pd is None or pq is None:
        raise RuntimeError("Missing dependencies: install pandas, pyarrow in Airflow environment")

    ingress_path = DEFAULT_WAREHOUSE / ingress_file
    validated_path = DEFAULT_WAREHOUSE / validated_file

    ingress_df = pd.DataFrame()
    validated_df = pd.DataFrame()

    if ingress_path.exists():
        ingress_df = pq.read_table(str(ingress_path)).to_pandas()
        ingress_df = _filter_window(ingress_df, window_start, window_end)

    if validated_path.exists():
        validated_df = pq.read_table(str(validated_path)).to_pandas()
        validated_df = _filter_window(validated_df, window_start, window_end)

    total_ingress = float(ingress_df["amount"].sum()) if not ingress_df.empty else 0.0
    total_validated = float(validated_df["amount"].sum()) if not validated_df.empty else 0.0

    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "total_ingress": total_ingress,
        "total_validated": total_validated,
        "difference": total_ingress - total_validated,
    }

    out_path = DEFAULT_REPORTS / report_name
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)


def fraud_by_merchant(report_name: str, window_start: str, window_end: str):
    if psycopg2 is None or pd is None:
        raise RuntimeError("Missing dependencies: install psycopg2-binary and pandas in Airflow environment")

    ensure_dirs()
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    query = """
        SELECT merchant_category, COUNT(*) AS fraud_attempts
        FROM fraud_transactions
        WHERE event_time >= %s
          AND event_time < %s
        GROUP BY merchant_category
        ORDER BY fraud_attempts DESC, merchant_category ASC;
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(query, (_parse_iso(window_start), _parse_iso(window_end)))
            rows = cur.fetchall()

    conn.close()

    counts = {row[0] or "UNKNOWN": int(row[1]) for row in rows}

    out_path = DEFAULT_REPORTS / report_name
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump({"generated_at": datetime.utcnow().isoformat() + "Z", "fraud_by_merchant": counts}, fh, indent=2)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fraud_etl_every_6h",
    default_args=default_args,
    description="ETL for validated transactions and reconciliation every 6 hours",
    schedule_interval="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="extract_validated_to_parquet",
        python_callable=consume_topic_to_parquet,
        op_kwargs={
            "topic": "valid-transactions",
            "filename": "validated_{{ ds_nodash }}.parquet",
            "window_start": "{{ data_interval_start.isoformat() }}",
            "window_end": "{{ data_interval_end.isoformat() }}",
            "timeout": 20,
        },
    )

    t1_ingress = PythonOperator(
        task_id="extract_ingress_to_parquet",
        python_callable=consume_topic_to_parquet,
        op_kwargs={
            "topic": "transactions",
            "filename": "ingress_{{ ds_nodash }}.parquet",
            "window_start": "{{ data_interval_start.isoformat() }}",
            "window_end": "{{ data_interval_end.isoformat() }}",
            "timeout": 20,
        },
    )

    t2 = PythonOperator(
        task_id="compute_reconciliation",
        python_callable=compute_reconciliation,
        op_kwargs={
            "ingress_file": "ingress_{{ ds_nodash }}.parquet",
            "validated_file": "validated_{{ ds_nodash }}.parquet",
            "report_name": "reconciliation_{{ ds_nodash }}.json",
            "window_start": "{{ data_interval_start.isoformat() }}",
            "window_end": "{{ data_interval_end.isoformat() }}",
        },
    )

    t3 = PythonOperator(
        task_id="fraud_by_merchant_report",
        python_callable=fraud_by_merchant,
        op_kwargs={
            "report_name": "fraud_by_merchant_{{ ds_nodash }}.json",
            "window_start": "{{ data_interval_start.isoformat() }}",
            "window_end": "{{ data_interval_end.isoformat() }}",
        },
    )

    [t1, t1_ingress] >> t2 >> t3
