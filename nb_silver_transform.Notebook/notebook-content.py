# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2eaddb7b-d98c-4e57-bcf0-3bec72762ee9",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "4db246e1-0a39-4ba9-a437-cf43cb1ec668",
# META       "known_lakehouses": [
# META         {
# META           "id": "2eaddb7b-d98c-4e57-bcf0-3bec72762ee9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

load_mode='INCREMENTAL'
watermark_date='2023-01-01'

BRONZE_TABLE          = "lh_bronze.dbo.bronze_stock_prices"
SILVER_PRICES_TABLE   = "lh_silver.dbo.silver_stock_prices"
SILVER_MAVG_TABLE     = "lh_silver.dbo.silver_moving_averages"

TICKERS = ["MSFT", "GOOGL", "AMZN", "NVDA", "META"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Load mode      : {load_mode}")
print(f"Watermark date : {watermark_date}")
print(f"Tickers        : {TICKERS}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Read Bronze with Watermark

# CELL ********************

from datetime import datetime, timedelta

moving_avg_max_window=30 #days

# Lookback start = watermark_date - 30 days
# This ensures we have enough history for the 30-day moving average
watermark_dt=datetime.strptime(watermark_date,'%Y-%m-%d')
lookback_dt=watermark_dt-timedelta(days=moving_avg_max_window+5)
lookback_str=lookback_dt.strftime("%Y-%m-%d")


# Read all Bronze rows within the lookback window
# (includes both old rows needed for window context + new rows to write)
bronze_df = spark.table(BRONZE_TABLE) \
    .filter(col("ticker").isin(TICKERS)) \
    .filter(col("price_date") >= lit(lookback_str).cast(DateType()))

# Deduplicate Bronze (same date may appear multiple times from re-runs)
# Keep the most recently ingested row per (ticker, price_date)
w_dedup=Window.partitionBy('ticker','price_date').orderBy(col('ingested_at').desc())

bronze_deduped = bronze_df \
    .withColumn("_rn", row_number().over(w_dedup)) \
    .filter(col("_rn") == 1) \
    .drop("_rn", "load_mode", "pipeline_run_ts")

total = bronze_deduped.count()
print(f"[BRONZE READ] {total} deduplicated rows from {lookback_str} onward")
bronze_deduped.groupBy("ticker").count().orderBy("ticker").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Compute Moving Averages

# CELL ********************

# ─────────────────────────────────────────────────────
# CELL 3: Compute moving averages using Spark window
#         functions over the ordered ticker partition
#
# Window spec: PARTITION BY ticker ORDER BY price_date
#   rowsBetween(-N+1, 0) = trailing N rows inclusive
#   of current row — the standard "trailing N-day avg"
#
# IMPORTANT: rowsBetween uses ROW offsets, not DATE
# offsets. This is correct for stock data because
# markets are closed weekends — we want N trading days,
# not N calendar days.
# ─────────────────────────────────────────────────────

# Base window — partition per ticker, ordered by date
w_base = Window.partitionBy("ticker").orderBy("price_date")

# Trailing windows for moving averages
w_7d  = w_base.rowsBetween(-6,  0)   # 7 trading days  (current + 6 prior)
w_14d = w_base.rowsBetween(-13, 0)   # 14 trading days
w_30d = w_base.rowsBetween(-29, 0)   # 30 trading days

# Additional analytics windows
w_full = w_base.rowsBetween(Window.unboundedPreceding, 0)  # all-time trailing

prices_with_avgs = bronze_deduped \
    .withColumn("ma_7d",   round(avg("close_price").over(w_7d),  4)) \
    .withColumn("ma_14d",  round(avg("close_price").over(w_14d), 4)) \
    .withColumn("ma_30d",  round(avg("close_price").over(w_30d), 4)) \
    .withColumn("vol_ma_7d", round(avg("volume").over(w_7d),  0)) \
    .withColumn("daily_return_pct",
        round(
            (col("close_price") - lag("close_price", 1).over(w_base))
            / lag("close_price", 1).over(w_base) * 100,
            4
        )
    ) \
    .withColumn("price_vs_ma30",
        when(col("close_price") > col("ma_30d"), "ABOVE")
        .when(col("close_price") < col("ma_30d"), "BELOW")
        .otherwise("AT")
    ) \
    \
    .withColumn("row_count_in_window",
        count("close_price").over(w_7d)
    ) \
    \
    .withColumn("silver_load_ts", current_timestamp())

print("[MOVING AVGS] Sample output:")
prices_with_avgs \
    .filter(col("ticker") == "MSFT") \
    .orderBy(col("price_date").desc()) \
    .select("ticker", "price_date", "close_price", "ma_7d", "ma_14d", "ma_30d",
            "daily_return_pct", "price_vs_ma30") \
    .show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Filter to New Rows Only for MERGE

# CELL ********************

# ─────────────────────────────────────────────────────
# CELL 4: After computing moving averages over the full
#         lookback window, filter back to ONLY the rows
#         newer than the watermark for MERGE into Silver.
#
# This is the critical pattern:
#   READ wide (30-day lookback) for correct window math
#   WRITE narrow (new rows only) for incremental MERGE
# ─────────────────────────────────────────────────────

if load_mode == "INCREMENTAL":
    new_rows_df = prices_with_avgs.filter(
        col("price_date") > lit(watermark_date).cast(DateType())
    )
    print(f"[FILTER] New rows to MERGE (price_date > {watermark_date}): {new_rows_df.count()}")
else:
    new_rows_df = prices_with_avgs
    print(f"[FULL] All rows to write: {new_rows_df.count()}")

# Drop helper column
new_rows_df = new_rows_df.drop("row_count_in_window")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### MERGE into silver_stock_prices

# CELL ********************

# ─────────────────────────────────────────────────────
# CELL 5: MERGE new rows into silver_stock_prices
#
# Merge key: (ticker, price_date) — composite unique key
# Update condition: always update (stock prices can be
#   restated by Alpha Vantage for recent dates)
# ─────────────────────────────────────────────────────

SILVER_COLS = [
    "ticker", "price_date",
    "open_price", "high_price", "low_price", "close_price", "volume",
    "ma_7d", "ma_14d", "ma_30d", "vol_ma_7d",
    "daily_return_pct", "price_vs_ma30",
    "ingested_at", "silver_load_ts"
]

silver_df = new_rows_df.select(SILVER_COLS)

table_exists = spark.catalog.tableExists(SILVER_PRICES_TABLE)

if not table_exists or load_mode == "FULL":
    print(f"[WRITE] Overwriting {SILVER_PRICES_TABLE}")
    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(SILVER_PRICES_TABLE)
else:
    print(f"[MERGE] Upserting into {SILVER_PRICES_TABLE}")
    target = DeltaTable.forName(spark, SILVER_PRICES_TABLE)

    target.alias("tgt").merge(
        silver_df.alias("src"),
        "tgt.ticker = src.ticker AND tgt.price_date = src.price_date"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

total_silver = spark.table(SILVER_PRICES_TABLE).count()
print(f"[DONE] {SILVER_PRICES_TABLE} — {total_silver:,} total rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### MERGE into silver_moving_averages (separate summary table)

# CELL ********************

# ─────────────────────────────────────────────────────
# CELL 6: Write a separate moving averages summary table
#         This is what Dataflows Gen2 and Power BI read.
#         Keeps the BI layer thin — no window functions
#         at query time.
# ─────────────────────────────────────────────────────

mavg_df = new_rows_df.select(
    "ticker",
    "price_date",
    "close_price",
    "ma_7d",
    "ma_14d",
    "ma_30d",
    "vol_ma_7d",
    "daily_return_pct",
    "price_vs_ma30",
    "silver_load_ts"
)

mavg_table_exists = spark.catalog.tableExists(SILVER_MAVG_TABLE)

if not mavg_table_exists or load_mode == "FULL":
    mavg_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(SILVER_MAVG_TABLE)
else:
    target_mavg = DeltaTable.forName(spark, SILVER_MAVG_TABLE)

    target_mavg.alias("tgt").merge(
        mavg_df.alias("src"),
        "tgt.ticker = src.ticker AND tgt.price_date = src.price_date"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

print(f"[DONE] {SILVER_MAVG_TABLE} — {spark.table(SILVER_MAVG_TABLE).count():,} total rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Idempotency Check

# CELL ********************

# ─────────────────────────────────────────────────────
# CELL 7: Idempotency validation
#
# Re-running the same pipeline twice for the same date
# range must produce identical results. This cell
# verifies no duplicate (ticker, price_date) rows exist
# in Silver — which would indicate a broken MERGE.
# ─────────────────────────────────────────────────────

from pyspark.sql.functions import count as spark_count

print("=" * 55)
print("IDEMPOTENCY CHECK — silver_stock_prices")
print("=" * 55)

silver = spark.table(SILVER_PRICES_TABLE)
total  = silver.count()

# Count distinct composite keys
distinct = silver.select("ticker", "price_date").distinct().count()
duplicates = total - distinct

if duplicates > 0:
    print(f"❌ DUPLICATES DETECTED: {duplicates} extra rows")
    silver \
        .groupBy("ticker", "price_date") \
        .agg(spark_count("*").alias("cnt")) \
        .filter(col("cnt") > 1) \
        .orderBy(col("cnt").desc()) \
        .show(10)
else:
    print(f"✅ No duplicates — {total:,} rows, all unique on (ticker, price_date)")

# Latest date per ticker
print("\nLatest loaded date per ticker:")
silver \
    .groupBy("ticker") \
    .agg(
        max("price_date").alias("latest_date"),
        spark_count("*").alias("total_rows")
    ) \
    .orderBy("ticker") \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
