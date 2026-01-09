run spark job

1. Go inside spark container

```bash
docker compose exec spark-master bash
```

2. Run the Job

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/ivy-cache \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-app/fraud_detection.py
```
