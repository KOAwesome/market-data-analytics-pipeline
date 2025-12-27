# ======================================
# Imports
# ======================================
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, stddev, avg
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


# ======================================
# Read Bronze Delta Table
# ======================================
bronze_path = "/tmp/market/bronze_prices"
bronze_df = spark.read.format("delta").load(bronze_path)


# ======================================
# Deduplication (Silver responsibility)
# ======================================
dedup_df = bronze_df.dropDuplicates(
    ["symbol", "trade_date", "close"]
)


# ======================================
# Window definitions
# ======================================
base_window = (
    Window
    .partitionBy("symbol")
    .orderBy("trade_date")
)

vol_7d_window = base_window.rowsBetween(-6, 0)
vol_30d_window = base_window.rowsBetween(-29, 0)

ma_7d_window = base_window.rowsBetween(-6, 0)
ma_30d_window = base_window.rowsBetween(-29, 0)


# ======================================
# Silver transformations
# ======================================
silver_df = (
    dedup_df
    .withColumn("prev_close", lag("close").over(base_window))
    .withColumn(
        "daily_return",
        (col("close") - col("prev_close")) / col("prev_close")
    )
    .withColumn(
        "volatility_7d",
        stddev("daily_return").over(vol_7d_window)
    )
    .withColumn(
        "volatility_30d",
        stddev("daily_return").over(vol_30d_window)
    )
    .withColumn(
        "ma_7d",
        avg("close").over(ma_7d_window)
    )
    .withColumn(
        "ma_30d",
        avg("close").over(ma_30d_window)
    )
    .select(
        "symbol",
        "trade_date",
        "close",
        "daily_return",
        "volatility_7d",
        "volatility_30d",
        "ma_7d",
        "ma_30d"
    )
)


# ======================================
# Write Silver Delta Table (Schema Evolution)
# ======================================
silver_path = "/tmp/market/silver_prices"

silver_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("symbol") \
    .save(silver_path)

print("✅ Silver Delta table with volatility + moving averages written successfully")