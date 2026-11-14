
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    MapType
)


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent

SILVER_DIR = PROJECT_ROOT / "data" / "delta" / "silver" / "silver_clickstream"
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "checkpoints" / "silver_clickstream"

# Use the active virtual environment's Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

print("=" * 60)
print("ECOMMERCE - KAFKA TO SILVER DELTA LAKE")
print("=" * 60)

# JSON schema matching the Kafka producer
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

# Configure Spark, Delta Lake and Kafka connector
builder = (
    SparkSession.builder
    .appName("Ecommerce-Kafka-Silver")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.13:4.0.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
    )
    .config("spark.sql.shuffle.partitions", "3")
)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read events from Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecommerce_clickstream")
    .option("startingOffsets", "earliest")
    .load()
)

# Parse JSON and retain Kafka lineage information
parsed_stream = (
    kafka_stream
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    )
    .select("data.*", "kafka_partition", "kafka_offset", "kafka_timestamp")
)

# Write the stream to Silver Delta Lake
SILVER_DIR.parent.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

query = (
    parsed_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", str(CHECKPOINT_DIR))
    .trigger(availableNow=True)
    .start(str(SILVER_DIR))
)

query.awaitTermination()

print("\nSILVER STREAMING WRITE COMPLETED")

# Verify the Silver table
silver_df = spark.read.format("delta").load(str(SILVER_DIR))

print("\nSILVER TABLE SCHEMA:")
silver_df.printSchema()

print(f"\nTOTAL SILVER RECORDS: {silver_df.count():,}")
print("\nSAMPLE SILVER RECORDS:")
silver_df.select(
    "session_id",
    "event_name",
    "traffic_source",
    "kafka_partition",
    "kafka_offset"
).show(10, truncate=False)

print(f"\nSilver Delta location: {SILVER_DIR}")

spark.stop()
