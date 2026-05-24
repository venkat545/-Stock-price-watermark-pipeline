# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ba94505f-b767-4217-baaf-90eba5a8fb75",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "4db246e1-0a39-4ba9-a437-cf43cb1ec668",
# META       "known_lakehouses": [
# META         {
# META           "id": "ba94505f-b767-4217-baaf-90eba5a8fb75"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
#DJ77SC1NH6YDCP6Y

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json 
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime, date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

ticker='MSFT'
watermark_date='2023-01-01'
load_mode='INCREMENTAL'
api_response={}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Ticker         : {ticker}")
print(f"Watermark date : {watermark_date}")
print(f"Load mode      : {load_mode}")
print(f"API response   : {api_response[:120]}...") 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

api_url='https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MSFT&outputsize=compact&apikey=DJ77SC1NH6YDCP6Y&datatype=json'

response= requests.get(api_url)

if response.status_code==200:
    api_data=response.json()
    print(api_data)
else:
    None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

def parse_alphavantage_response(raw_json,ticker):

    try:
        data=json.loads(raw_json)
        print(data)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON from API for {ticker}: {e}")
    
    if "Time Series (Daily)" not in data:
        error_msg = data.get("Note", data.get("Information", "Unknown API error"))
        raise ValueError(f"API error for {ticker}: {error_msg}")
    
    time_series=data['Time Series (Daily)']
    rows=[]
    for date_str, metrics in time_series.items():
        rows.append(Row(
            ticker= ticker,
            price_date=date_str,
            open_price  = float(metrics["1. open"]),
            high_price  = float(metrics["2. high"]),
            low_price   = float(metrics["3. low"]),
            close_price = float(metrics["4. close"]),
            volume      = float(metrics["5. volume"]),
            ingested_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        ))
    print(f"[PARSE] {ticker}: {len(rows)} trading days found in response")
    return rows

rows=parse_alphavantage_response(api_response,ticker)

raw_schema = StructType([
    StructField("ticker",      StringType(),  False),
    StructField("price_date",  StringType(),  False),
    StructField("open_price",  DoubleType(),  True),
    StructField("high_price",  DoubleType(),  True),
    StructField("low_price",   DoubleType(),  True),
    StructField("close_price", DoubleType(),  True),
    StructField("volume",      DoubleType(),  True),
    StructField("ingested_at", StringType(),  True),
])

raw_df = spark.createDataFrame(rows, schema=raw_schema) \
    .withColumn("price_date",  to_date(col("price_date"),  "yyyy-MM-dd")) \
    .withColumn("ingested_at", col("ingested_at").cast("timestamp"))

print(f"[DATAFRAME] Schema:")
raw_df.printSchema()
print(f"[DATAFRAME] Sample rows:")
raw_df.orderBy(col("price_date").desc()).show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ### Apply Watermark Filter

# CELL ********************

total_api_rows = raw_df.count()

if load_mode=='INCREMENTAL':
    watermark=datetime.strptime(watermark_date,"%Y-%m-%d").date()

    filtered_df=raw_df.filter(col('price_date')>lit(watermark).cast(DateType()))

    new_rows=filtered_df.count()

    print(f"[WATERMARK] Filtering price_date > {watermark_date}")
    print(f"[WATERMARK] API returned {total_api_rows} rows total")
    print(f"[WATERMARK] New rows after watermark filter: {new_rows}")

    if new_rows == 0:
        print(f"[SKIP] No new data for {ticker} since {watermark_date}. Exiting.")
        notebookutils.notebook.exit(f"SKIPPED:{ticker}:{new_rows}")

    bronze_df = filtered_df
else:
    print(f"[FULL LOAD] Loading all {total_api_rows} rows for {ticker}")
    bronze_df = raw_df

print(f"[READY] {bronze_df.count()} rows to write to Bronze")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write to Bronze (Append)

# CELL ********************

BRONZE_TABLE = "lh_bronze.dbo.bronze_stock_prices"

bronze_write_df=bronze_df.withColumn('load_mode',lit(load_mode)).withColumn('pipline_run_ts',current_timestamp())

bronze_write_df.write.format('delta').mode('append').option("mergeSchema", "true").saveAsTable(BRONZE_TABLE)

written_count = bronze_write_df.count()
print(f"[BRONZE] Appended {written_count} rows to {BRONZE_TABLE}")
print(f"[BRONZE] Total rows now: {spark.table(BRONZE_TABLE).filter(col('ticker') == ticker).count()}")

# Signal success back to Pipeline via notebook exit value
notebookutils.notebook.exit(f"SUCCESS:{ticker}:{written_count}")  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
