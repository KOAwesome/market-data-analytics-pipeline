# ======================================
# Imports
# ======================================
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, stddev, avg
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


# ======================================
# Spark Session (Delta-enabled)
# ======================================
builder = (
    SparkSession.builder
    .appName("MarketDataSilverMerge")
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
# Read Bronze
# ======================================
bronze_path = "/tmp/market/bronze_prices"
bronze_df = spark.read.format("delta").load(bronze_path)


# ======================================
# Deduplication
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

vol_7d = base_window.rowsBetween(-6, 0)
vol_30d = base_window.rowsBetween(-29, 0)


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
    .withColumn("volatility_7d", stddev("daily_return").over(vol_7d))
    .withColumn("volatility_30d", stddev("daily_return").over(vol_30d))
    .withColumn("ma_7d", avg("close").over(vol_7d))
    .withColumn("ma_30d", avg("close").over(vol_30d))
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
# Delta MERGE (UPSERT)
# ======================================
silver_path = "/tmp/market/silver_prices"

if DeltaTable.isDeltaTable(spark, silver_path):
    silver_table = DeltaTable.forPath(spark, silver_path)

    (
        silver_table.alias("t")
        .merge(
            silver_df.alias("s"),
            """
            t.symbol = s.symbol
            AND t.trade_date = s.trade_date
            AND t.close = s.close
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:
    silver_df.write.format("delta") \
        .mode("overwrite") \
        .save(silver_path)

print("✅ Silver Delta table MERGE completed successfully")