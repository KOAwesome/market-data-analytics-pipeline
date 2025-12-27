# ======================================
# Imports (ORDER IS IMPORTANT)
# ======================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

import yfinance as yf
import pandas as pd


# ======================================
# Spark Session (Delta ENABLED – FINAL)
# ======================================
builder = (
    SparkSession.builder
    .appName("MarketDataIngestion")
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

# ---- Diagnostics (KEEP THESE) ----
print("Spark extensions:", spark.conf.get("spark.sql.extensions"))
print("Spark catalog:", spark.conf.get("spark.sql.catalog.spark_catalog"))


# ======================================
# Market Data Download (Pandas)
# ======================================
symbols = ["AAPL", "MSFT", "GOOG"]
all_data = []

for symbol in symbols:
    df = yf.download(
        symbol,
        start="2023-01-01",
        end="2024-01-01",
        progress=False
    )

    # Flatten MultiIndex columns (yfinance quirk)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.reset_index(inplace=True)
    df["symbol"] = symbol
    all_data.append(df)

# Combine all symbols into one Pandas DF
pdf = pd.concat(all_data, ignore_index=True)


# ======================================
# Pandas → Spark (Bronze Ingestion)
# ======================================
spark_df = spark.createDataFrame(pdf)

bronze_df = spark_df.select(
    col("symbol"),
    col("Date").alias("trade_date"),
    col("Open").alias("open"),
    col("High").alias("high"),
    col("Low").alias("low"),
    col("Close").alias("close"),
    col("Volume").alias("volume")
)


# ======================================
# Write Bronze Delta Table
# ======================================
output_path = "tmp/market/bronze_prices"

bronze_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("trade_date") \
    .save(output_path)

print(f"✅ Bronze Delta table written to {output_path}")