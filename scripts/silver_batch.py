
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_timestamp,
    current_timestamp
)
from delta import configure_spark_with_delta_pip

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "delta" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "delta" / "silver"

# Configure Python environment
python_path = str(
    Path(os.environ["VIRTUAL_ENV"]) / "Scripts" / "python.exe"
)
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# Spark + Delta configuration
builder = (
    SparkSession.builder
    .appName("Ecommerce-Silver-Batch")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 55)
print("ECOMMERCE - SILVER BATCH PROCESSING")
print("=" * 55)

# Read complete Bronze clickstream table
bronze_path = BRONZE_DIR / "bronze_clickstream"
df = spark.read.format("delta").load(str(bronze_path))

print(f"\nBronze records: {df.count():,}")

# Standardize event names
df = df.withColumn(
    "event_name",
    upper(trim(col("event_name")))
)

# Parse event timestamps
df = df.withColumn(
    "event_time",
    to_timestamp(col("event_time"))
)

# Remove duplicate event IDs
df = df.dropDuplicates(["event_id"])

# Remove records with essential missing fields
df = df.filter(
    col("event_id").isNotNull()
    & col("session_id").isNotNull()
    & col("event_name").isNotNull()
    & col("event_time").isNotNull()
)

# Validate quantities and prices when present
df = df.filter(
    (col("quantity").isNull() | (col("quantity") > 0))
    & (col("item_price").isNull() | (col("item_price") > 0))
)

# ADD_TO_CART events must have product IDs
df = df.filter(
    (col("event_name") != "ADD_TO_CART")
    | col("product_id").isNotNull()
)

# Add Silver processing timestamp
df = df.withColumn(
    "silver_processing_timestamp",
    current_timestamp()
)

# Write to a separate Silver Batch Delta table
target_path = SILVER_DIR / "silver_clickstream_batch"

df.write.format("delta").mode("overwrite").save(str(target_path))

# Verify output
silver_df = spark.read.format("delta").load(str(target_path))

print("\n" + "=" * 55)
print("SILVER BATCH VERIFICATION")
print("=" * 55)

print(f"Silver records: {silver_df.count():,}")
print(f"Silver columns: {len(silver_df.columns)}")

print("\nEvent distribution:")
silver_df.groupBy("event_name").count().orderBy("event_name").show(
    truncate=False
)

print("Sample Silver records:")
silver_df.show(5, truncate=False)

print(f"\nSilver Delta location: {target_path}")
print("\nSILVER BATCH PROCESSING COMPLETED")

spark.stop()
