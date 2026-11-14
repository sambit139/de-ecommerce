
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    coalesce,
    lit,
    current_timestamp
)
from delta import configure_spark_with_delta_pip

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "delta" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "delta" / "silver"

# Python environment
python_path = str(
    Path(os.environ["VIRTUAL_ENV"]) / "Scripts" / "python.exe"
)
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# Spark + Delta
builder = (
    SparkSession.builder
    .appName("Ecommerce-Silver-Dimensions")
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

SILVER_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 55)
print("SILVER DIMENSION TABLE PROCESSING")
print("=" * 55)

# ---------------- PRODUCTS ----------------

print("\nProcessing Silver Products...")

products = (
    spark.read.format("delta")
    .load(str(BRONZE_DIR / "bronze_products"))
)

products = products.dropDuplicates(["id"])

# Standardize categorical fields
for column in [
    "gender",
    "mastercategory",
    "subcategory",
    "articletype",
    "basecolour",
    "season",
    "usage"
]:
    products = products.withColumn(
        column,
        upper(trim(col(column)))
    )

# Replace missing categorical values
for column in ["basecolour", "season", "usage"]:
    products = products.withColumn(
        column,
        coalesce(col(column), lit("UNKNOWN"))
    )

# Preserve missing year and product display name as NULL
products = products.withColumn(
    "silver_processing_timestamp",
    current_timestamp()
)

product_path = SILVER_DIR / "silver_products"

products.write.format("delta").mode("overwrite").save(
    str(product_path)
)

print("Silver Products:", products.count())
print("Saved:", product_path)

# ---------------- CUSTOMERS ----------------

print("\nProcessing Silver Customers...")

customers = (
    spark.read.format("delta")
    .load(str(BRONZE_DIR / "bronze_customers"))
)

customers = customers.dropDuplicates(["customer_id"])

# Standardize selected categorical fields
for column in ["gender", "device_type", "home_country"]:
    customers = customers.withColumn(
        column,
        upper(trim(col(column)))
    )

customers = customers.withColumn(
    "silver_processing_timestamp",
    current_timestamp()
)

customer_path = SILVER_DIR / "silver_customers"

customers.write.format("delta").mode("overwrite").save(
    str(customer_path)
)

print("Silver Customers:", customers.count())
print("Saved:", customer_path)

print("\nSILVER DIMENSION PROCESSING COMPLETED")

spark.stop()
