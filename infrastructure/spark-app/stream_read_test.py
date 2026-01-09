from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaReadTest").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "transactions")   # <-- change if your topic name differs
    .option("startingOffsets", "latest")
    .load()
)

# Print raw Kafka messages to console
q = (
    df.selectExpr("CAST(value AS STRING) as json")
      .writeStream.format("console")
      .option("truncate", "false")
      .outputMode("append")
      .start()
)

q.awaitTermination()
