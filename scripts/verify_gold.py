import os
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

root = Path(__file__).resolve().parent.parent
silver = root / "data" / "delta" / "silver"
gold = root / "data" / "delta" / "gold"

p = str(Path(os.environ["VIRTUAL_ENV"]) / "Scripts" / "python.exe")
os.environ["PYSPARK_PYTHON"] = p
os.environ["PYSPARK_DRIVER_PYTHON"] = p

builder = (
    SparkSession.builder
    .appName("Verify-Gold")
    .master("local[*]")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

events = spark.read.format("delta").load(
    str(silver / "silver_clickstream_batch")
)
products = spark.read.format("delta").load(
    str(silver / "silver_products")
)
sessions = spark.read.format("delta").load(
    str(gold / "gold_session_summary")
)

cart = events.filter("event_name = 'ADD_TO_CART'")

unmatched = cart.join(
    products,
    cart.product_id.cast("long") == products.id,
    "left_anti"
).count()

print("UNMATCHED CART PRODUCT IDs:", unmatched)

print("\nEVENTS PER SESSION:")
sessions.groupBy("total_events").count().orderBy(
    "total_events"
).show()

print("TOTAL SESSIONS:", sessions.count())
print("MAX EVENTS IN ONE SESSION:", sessions.agg(
    {"total_events": "max"}
).first()[0])

spark.stop()