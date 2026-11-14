
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    avg,
    min as spark_min,
    max as spark_max,
    when,
    lit,
    coalesce,
    regexp_extract,
    upper
)
from delta import configure_spark_with_delta_pip

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_DIR = PROJECT_ROOT / "data" / "delta" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "delta" / "gold"

# Python environment
python_path = str(
    Path(os.environ["VIRTUAL_ENV"]) / "Scripts" / "python.exe"
)
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# Spark + Delta
builder = (
    SparkSession.builder
    .appName("Ecommerce-Gold-Analytics")
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

GOLD_DIR.mkdir(parents=True, exist_ok=True)

# Read Silver tables
events = spark.read.format("delta").load(
    str(SILVER_DIR / "silver_clickstream_batch")
)

products = spark.read.format("delta").load(
    str(SILVER_DIR / "silver_products")
)

print("=" * 60)
print("ECOMMERCE - GOLD ANALYTICS")
print("=" * 60)


def save_gold(df, table_name):
    """Write a Gold DataFrame as a Delta table and verify it."""
    target = GOLD_DIR / table_name

    df.write.format("delta").mode("overwrite").save(str(target))

    saved = spark.read.format("delta").load(str(target))

    print(f"\n{table_name}: {saved.count():,} rows")
    print(f"Saved to: {target}")


# --------------------------------------------------
# 1. EVENT SUMMARY
# --------------------------------------------------

print("\n1. Building Gold Event Summary...")

gold_event_summary = (
    events.groupBy("event_name")
    .agg(
        count("*").alias("total_events"),
        countDistinct("session_id").alias("unique_sessions")
    )
    .orderBy("event_name")
)

save_gold(gold_event_summary, "gold_event_summary")


# --------------------------------------------------
# 2. CATEGORY-WISE CART ANALYSIS
# --------------------------------------------------

print("\n2. Building Gold Category Cart Analysis...")

cart_events = (
    events.filter(col("event_name") == "ADD_TO_CART")
    .alias("c")
)

product_dimension = products.alias("p")

cart_with_products = (
    cart_events.join(
        product_dimension,
        col("c.product_id").cast("long") == col("p.id"),
        "left"
    )
    .select(
        col("c.event_id"),
        col("c.session_id"),
        col("c.quantity"),
        col("c.item_price"),
        coalesce(col("p.mastercategory"), lit("UNKNOWN"))
            .alias("mastercategory"),
        coalesce(col("p.subcategory"), lit("UNKNOWN"))
            .alias("subcategory"),
        coalesce(col("p.articletype"), lit("UNKNOWN"))
            .alias("articletype")
    )
)

gold_category_cart_analysis = (
    cart_with_products
    .groupBy("mastercategory", "subcategory", "articletype")
    .agg(
        count("*").alias("cart_events"),
        countDistinct("session_id").alias("unique_cart_sessions"),
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("item_price").alias("observed_cart_item_price_sum"),
        avg("item_price").alias("average_cart_event_item_price")
    )
    .orderBy(col("cart_events").desc())
)

save_gold(
    gold_category_cart_analysis,
    "gold_category_cart_analysis"
)

print("\nCart-to-product join verification:")
print("Cart events:", cart_events.count())
print("Joined cart events:", cart_with_products.count())


# --------------------------------------------------
# 3. BOOKING ANALYSIS
# --------------------------------------------------

print("\n3. Building Gold Booking Analysis...")

bookings = (
    events.filter(col("event_name") == "BOOKING")
    .withColumn(
        "payment_status",
        regexp_extract(
            col("event_metadata"),
            r"'payment_status'\s*:\s*'([^']+)'",
            1
        )
    )
    .withColumn(
        "payment_status",
        when(
            col("payment_status") == "",
            lit("UNKNOWN")
        ).otherwise(upper(col("payment_status")))
    )
)

gold_booking_analysis = (
    bookings.groupBy("payment_status")
    .agg(
        count("*").alias("total_bookings"),
        countDistinct("session_id").alias("unique_booking_sessions")
    )
)

save_gold(gold_booking_analysis, "gold_booking_analysis")


# --------------------------------------------------
# 4. SESSION SUMMARY
# --------------------------------------------------

print("\n4. Building Gold Session Summary...")

gold_session_summary = (
    events.groupBy("session_id")
    .agg(
        count("*").alias("total_events"),
        spark_sum(
            when(col("event_name") == "ADD_TO_CART", 1).otherwise(0)
        ).alias("cart_event_count"),
        spark_sum(
            when(col("event_name") == "BOOKING", 1).otherwise(0)
        ).alias("booking_event_count"),
        spark_max(
            when(col("event_name") == "ADD_TO_CART", 1).otherwise(0)
        ).alias("has_cart_event"),
        spark_max(
            when(col("event_name") == "BOOKING", 1).otherwise(0)
        ).alias("has_booking_event"),
        spark_min("event_time").alias("first_event_time"),
        spark_max("event_time").alias("last_event_time")
    )
)

save_gold(gold_session_summary, "gold_session_summary")


# --------------------------------------------------
# FINAL VERIFICATION
# --------------------------------------------------

print("\n" + "=" * 60)
print("GOLD ANALYTICS VERIFICATION")
print("=" * 60)

print("\nEvent Summary:")
gold_event_summary.show(truncate=False)

print("\nCategory Cart Analysis:")
gold_category_cart_analysis.show(10, truncate=False)

print("\nBooking Analysis:")
gold_booking_analysis.show(truncate=False)

print("\nSession Summary Sample:")
gold_session_summary.show(5, truncate=False)

print("\nGOLD ANALYTICS PIPELINE COMPLETED")

spark.stop()
