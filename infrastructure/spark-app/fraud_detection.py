from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, approx_count_distinct, lit, struct, to_json
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BOOTSTRAP = "kafka:9092"
SOURCE_TOPIC = "transactions"        # <- your current topic
FRAUD_TOPIC = "fraud-alerts"
VALID_TOPIC = "valid-transactions"

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),          # ISO string
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("location", StringType(), True),
])

spark = SparkSession.builder.appName("FinTechFraudDetection").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

tx = (
    raw.select(from_json(col("value").cast("string"), schema).alias("d"))
       .select("d.*")
       .withColumn("event_time", to_timestamp(col("timestamp")))
       .filter(col("event_time").isNotNull())
)

# ---------------------------
# Rule A: High-value fraud
# ---------------------------
fraud_high_value = (
    tx.filter(col("amount") > 5000)
      .withColumn("fraud_type", lit("HIGH_VALUE"))
      .withColumn("fraud_reason", lit("amount > 5000"))
)

# ---------------------------
# Rule B: Impossible travel (2 countries in 10 minutes)
# We detect windows where user has >1 distinct location in 10 min (event time)
# Then join back to tag transactions that fall in those windows
# ---------------------------

travel_windows = (
    tx.withWatermark("event_time", "30 minutes")
      .groupBy(col("user_id"), window(col("event_time"), "10 minutes"))
      .agg(approx_count_distinct("location").alias("distinct_locations"))
      .filter(col("distinct_locations") > 1)
)

fraud_travel = (
    tx.join(travel_windows, on="user_id")
      .filter((col("event_time") >= col("window.start")) & (col("event_time") < col("window.end")))
      .drop("window", "distinct_locations")
      .withColumn("fraud_type", lit("IMPOSSIBLE_TRAVEL"))
      .withColumn("fraud_reason", lit("2 countries within 10 minutes"))
)

# Combine fraud streams (remove duplicates if any)
fraud = fraud_high_value.unionByName(fraud_travel).dropDuplicates(["transaction_id", "fraud_type"])

# VALID stream (simple approach): everything that is not high-value.
# For stricter correctness, anti-join against fraud (more complex in streaming).
valid = tx.filter(col("amount") <= 5000)

# ---------------------------
# Output 1: Console (easy demo)
# ---------------------------
fraud_console = (
    fraud.select("transaction_id", "user_id", "timestamp", "merchant_category", "amount", "location", "fraud_type", "fraud_reason")
        .writeStream.format("console")
        .option("truncate", "false")
        .outputMode("append")
        .start()
)

# ---------------------------
# Output 2 (Optional but great): Write to Kafka topics
# ---------------------------
fraud_to_kafka = (
    fraud.select(to_json(struct(*[col(c) for c in fraud.columns])).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", FRAUD_TOPIC)
        .option("checkpointLocation", "/tmp/checkpoints/fraud")
        .outputMode("append")
        .start()
)

valid_to_kafka = (
    valid.select(to_json(struct(*[col(c) for c in valid.columns])).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", VALID_TOPIC)
        .option("checkpointLocation", "/tmp/checkpoints/valid")
        .outputMode("append")
        .start()
)

spark.streams.awaitAnyTermination()
