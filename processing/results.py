from pyspark.sql import SparkSession


from delta import configure_spark_with_delta_pip


# ======================================
# Spark Session (Delta-enabled)
# ======================================
builder = (
    SparkSession.builder
    .appName("MarketDataSilver")
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
# spark.read.format("delta").load("tmp/market/silver_prices").show(10)

# spark.read.format("delta").load("/tmp/market/silver_prices").printSchema()

print(spark.read.format("delta").load("/tmp/market/silver_prices").count())