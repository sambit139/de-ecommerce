
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from delta import configure_spark_with_delta_pip

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "ingested"
OUTPUT_DIR = PROJECT_ROOT / "data" / "delta" / "bronze"

datasets = {
    "prepared_clickstream": "bronze_clickstream",
    "prepared_customer": "bronze_customers",
    "prepared_product": "bronze_products",
}
os.environ["PYSPARK_PYTHON"] = str(
    Path(os.environ["VIRTUAL_ENV"]) / "Scripts" / "python.exe"
)
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]
builder = (
    SparkSession.builder
    .appName("Ecommerce-Bronze-Delta")
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

os.environ["PYSPARK_PYTHON"] = str(
    Path(os.environ["VIRTUAL_ENV"]) / "Scripts" / "python.exe"
)
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]

print("=" * 55)
print("ECOMMERCE - BRONZE DELTA LAKE")
print("=" * 55)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

for source_name, table_name in datasets.items():
    print(f"\nProcessing: {table_name}")

    source_path = INPUT_DIR / f"{source_name}.parquet"
    target_path = OUTPUT_DIR / table_name

    if not source_path.exists():
        print(f"ERROR: Source file not found: {source_path}")
        continue

    try:
        df = spark.read.parquet(str(source_path))

        df = df.withColumn(
            "bronze_ingestion_timestamp",
            current_timestamp()
        )

        df.write.format("delta").mode("overwrite").save(str(target_path))

        count = spark.read.format("delta").load(str(target_path)).count()

        print(f"SUCCESS: {table_name}")
        print(f"Records: {count:,}")
        print(f"Delta location: {target_path}")

    except Exception as error:
        print(f"ERROR: {error}")

print("\n" + "=" * 55)
print("BRONZE DELTA INGESTION FINISHED")
print("=" * 55)

spark.stop()
