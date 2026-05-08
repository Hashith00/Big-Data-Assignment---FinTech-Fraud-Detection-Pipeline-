import json
import os
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "valid-transactions")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DB = os.getenv("PG_DB", "fintech")
PG_USER = os.getenv("PG_USER", "fintech")
PG_PASSWORD = os.getenv("PG_PASSWORD", "fintech")


def get_db_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )


def validate_db(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'valid_transactions');"
        )
        exists = cur.fetchone()[0]
    if not exists:
        raise RuntimeError(
            "Table 'valid_transactions' does not exist in the database. "
            "Run init.sql or create it manually before starting the consumer."
        )


def insert_valid(cur, msg: dict):
    tx_id = msg.get("transaction_id")
    user_id = msg.get("user_id")
    ts = msg.get("timestamp")
    merchant = msg.get("merchant_category")
    amount = msg.get("amount")
    location = msg.get("location")

    event_time = None
    if ts:
        try:
            event_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            event_time = None

    cur.execute(
        """
        INSERT INTO valid_transactions (
          transaction_id, user_id, event_time, merchant_category,
          amount, location, raw_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (transaction_id) DO NOTHING;
        """,
        (tx_id, user_id, event_time, merchant, amount, location, Json(msg)),
    )


def main():
    print(f"[valid-consumer] Kafka: {KAFKA_BOOTSTRAP}, topic: {TOPIC}")
    print(f"[valid-consumer] Postgres: {PG_HOST}:{PG_PORT}/{PG_DB} user={PG_USER}")

    while True:
        try:
            conn = get_db_conn()
            conn.autocommit = False
            validate_db(conn)
            print("[valid-consumer] DB connection OK, table 'valid_transactions' found.")
            break
        except RuntimeError as e:
            print(f"[valid-consumer] Schema error: {e}")
            conn.close()
            raise
        except Exception as e:
            print("[valid-consumer] Waiting for Postgres...", e)
            time.sleep(2)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="valid-db-writer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print("[valid-consumer] Started. Waiting for messages...")

    try:
        with conn:
            with conn.cursor() as cur:
                for record in consumer:
                    msg = record.value
                    try:
                        insert_valid(cur, msg)
                        conn.commit()
                        print(f"[valid-consumer] inserted tx={msg.get('transaction_id')}")
                    except Exception as e:
                        conn.rollback()
                        print("[valid-consumer] insert failed:", e, "msg=", msg)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--validate":
        print(f"[valid-consumer] Testing connection to {PG_HOST}:{PG_PORT}/{PG_DB} user={PG_USER}")
        try:
            conn = get_db_conn()
            validate_db(conn)
            conn.close()
            print("[valid-consumer] OK — connection and schema are valid.")
        except Exception as e:
            print(f"[valid-consumer] FAILED — {e}")
            sys.exit(1)
    else:
        main()
