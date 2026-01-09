import json
import os
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")  # Use localhost for local runs
TOPIC = os.getenv("KAFKA_TOPIC", "fraud-alerts")

PG_HOST = os.getenv("PG_HOST", "localhost")  # Use localhost for local runs
PG_PORT = int(os.getenv("PG_PORT", "5433"))  # Use 5433 as we changed it
PG_DB = os.getenv("PG_DB", "fintech")
PG_USER = os.getenv("PG_USER", "fintech")
PG_PASSWORD = os.getenv("PG_PASSWORD", "fintech")


def get_db_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )


def insert_fraud(cur, msg: dict):
    # Spark fraud message should contain these fields if you used the earlier script
    tx_id = msg.get("transaction_id")
    user_id = msg.get("user_id")
    ts = msg.get("timestamp")  # ISO string
    merchant = msg.get("merchant_category")
    amount = msg.get("amount")
    location = msg.get("location")
    fraud_type = msg.get("fraud_type")
    fraud_reason = msg.get("fraud_reason")

    event_time = None
    if ts:
        try:
            event_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            event_time = None

    cur.execute(
        """
        INSERT INTO fraud_transactions (
          transaction_id, user_id, event_time, merchant_category,
          amount, location, fraud_type, fraud_reason, raw_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (transaction_id) DO NOTHING;
        """,
        (tx_id, user_id, event_time, merchant, amount, location, fraud_type, fraud_reason, Json(msg)),
    )


def main():
    print(f"[consumer] Kafka: {KAFKA_BOOTSTRAP}, topic: {TOPIC}")
    print(f"[consumer] Postgres: {PG_HOST}:{PG_PORT}/{PG_DB} user={PG_USER}")

    # Connect DB (retry loop)
    while True:
        try:
            conn = get_db_conn()
            conn.autocommit = False
            break
        except Exception as e:
            print("[consumer] Waiting for Postgres...", e)
            time.sleep(2)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="fraud-db-writer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print("[consumer] Started. Waiting for messages...")

    try:
        with conn:
            with conn.cursor() as cur:
                for record in consumer:
                    msg = record.value
                    try:
                        insert_fraud(cur, msg)
                        conn.commit()
                        print(f"[consumer] inserted tx={msg.get('transaction_id')}")
                    except Exception as e:
                        conn.rollback()
                        print("[consumer] insert failed:", e, "msg=", msg)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
