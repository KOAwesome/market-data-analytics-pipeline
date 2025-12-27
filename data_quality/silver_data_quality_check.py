from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from data_quality.checks import (
    check_not_null,
    check_positive,
    check_duplicates,
    check_key_completeness
)


# ======================================
# Spark Session
# ======================================
builder = (
    SparkSession.builder
    .appName("SilverDataQualityChecks")
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
# Load Tables
# ======================================
silver_df = spark.read.format("delta").load("/tmp/market/silver_prices")
bronze_df = spark.read.format("delta").load("/tmp/market/bronze_prices")


# ======================================
# Run Checks
# ======================================
check_not_null(
    silver_df,
    ["symbol", "trade_date", "close"],
    "❌ NULL values found in critical Silver columns"
)

check_positive(
    silver_df,
    "close",
    "❌ Non-positive close price detected in Silver"
)

check_duplicates(
    silver_df,
    ["symbol", "trade_date", "close"],
    "❌ Duplicate records detected in Silver"
)

check_key_completeness(
    silver_df,
    bronze_df,
    ["symbol", "trade_date"],
    "❌ Silver introduced unexpected business keys"
)

print("✅ All Silver data quality checks passed successfully")