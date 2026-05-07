run spark job

1. Go inside spark container

```bash
docker compose exec spark-master bash
```

2. Run the Job

```bash
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 1g \
    --executor-memory 1g \
    --conf spark.jars.ivy=/tmp/ivy-cache \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /opt/spark-app/fraud_detection.py
```

# How to Run full system

# 1. Start everything

```bash
cd infrastructure
docker compose up --build

```

# 2. Wait ~2 min for airflow-init to complete, then open:

# Airflow UI → http://localhost:8080 (admin / admin)

# Kafka UI → http://localhost:8090

# Spark UI → http://localhost:8081

# 3. Run the producer (separate terminal)

```bash
python Data/data-ingestion.py
```

# 4. Submit the Spark fraud detection job (as before, from commands.md)

# 5. The DAG runs automatically every 6 hours.

# To trigger it manually: Airflow UI → etl_reconciliation → Trigger DAG
