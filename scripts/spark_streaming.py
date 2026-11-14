
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, MapType
)

print("=" * 60)
print("ECOMMERCE - SPARK STRUCTURED STREAMING")
print("=" * 60)

# 1. Create Spark session with Kafka connector
spark = (
    SparkSession.builder
    .appName("EcommerceKafkaStreaming")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
    )
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Define incoming JSON schema
schema = StructType([
    StructField("session_id", StringType()),
    StructField("event_name", StringType()),
    StructField("event_time", StringType()),
    StructField("event_id", StringType()),
    StructField("traffic_source", StringType()),
    StructField("event_metadata", MapType(StringType(), StringType())),
    StructField("event_metadata_parsed", MapType(StringType(), StringType())),
    StructField("product_id", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("item_price", DoubleType()),
    StructField("ingestion_timestamp", StringType())
])

# 3. Read streaming data from Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecommerce_clickstream")
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Convert Kafka JSON messages into structured columns
parsed_stream = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
)

# 5. Display processed streaming events
query = (
    parsed_stream
    .select(
        "session_id",
        "event_name",
        "event_time",
        "traffic_source",
        "product_id",
        "quantity",
        "item_price"
    )
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        "data/checkpoints/kafka_streaming"
    )
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("\nSTREAMING JOB COMPLETED")
spark.stop()
