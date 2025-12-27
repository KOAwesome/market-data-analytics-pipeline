# ======================================
# Imports
# ======================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    max,
    count
)
from delta import configure_spark_with_delta_pip


# ======================================
# Spark Session (Delta-enabled)
# ======================================
builder = (
    SparkSession.builder
    .appName("MarketDataGold")
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
# Read Silver Delta Table
# ======================================
silver_path = "/tmp/market/silver_prices"
silver_df = spark.read.format("delta").load(silver_path)


# ======================================
# Gold Aggregations
# ======================================
gold_df = (
    silver_df
    .groupBy("symbol")
    .agg(
        avg("daily_return").alias("avg_daily_return"),
        avg("volatility_30d").alias("avg_30d_volatility"),
        max("trade_date").alias("latest_trade_date"),
        count("trade_date").alias("trading_days")
    )
)


# ======================================
# Join latest snapshot metrics
# ======================================
latest_df = (
    silver_df
    .select(
        "symbol",
        col("trade_date").alias("latest_trade_date"),
        col("close").alias("latest_close"),
        col("ma_7d").alias("latest_ma_7d"),
        col("ma_30d").alias("latest_ma_30d")
    )
)

gold_final_df = (
    gold_df
    .join(
        latest_df,
        on=["symbol", "latest_trade_date"],
        how="left"
    )
)


# ======================================
# Write Gold Delta Table
# ======================================
gold_path = "/tmp/market/gold_prices"

gold_final_df.write.format("delta") \
    .mode("overwrite") \
    .save(gold_path)

print("✅ Gold Delta table written successfully")