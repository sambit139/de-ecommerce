from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("EcommerceDeltaTest")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = [(1, "Test Product"), (2, "Test Customer")]
df = spark.createDataFrame(data, ["id", "name"])

df.write.format("delta").mode("overwrite").save("data/delta_test")

spark.read.format("delta").load("data/delta_test").show()

spark.stop()