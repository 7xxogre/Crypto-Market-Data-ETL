import argparse
import json
import pickle
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import from_json, col, current_date, Window, last, \
                                    col, first, last, when, floor
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType
from datetime import timedelta
from google.cloud import storage

def load_schema(topic_name: str) -> StructType:
    if topic_name not in ['upbit_trade', 'upbit_orderbook']:
        raise Exception("topic name이 적절하지 않습니다. [upbit_trade, upbit_orderbook] 중에 있어야합니다.")
    if "upbit_trade" == topic_name:
        return StructType([
            StructField("type", StringType(), True),
            StructField("code", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("trade_date", StringType(), True),
            StructField("trade_time", StringType(), True),
            StructField("trade_timestamp", LongType(), True),
            StructField("trade_price", DoubleType(), True),
            StructField("trade_volume", DoubleType(), True),
            StructField("ask_bid", StringType(), True),
            StructField("prev_closing_price", DoubleType(), True),
            StructField("change", StringType(), True),
            StructField("change_price", DoubleType(), True),
            StructField("sequential_id", LongType(), True),
            StructField("stream_type", StringType(), True),
            StructField("arrive_time", DoubleType(), True)
        ])
    
    if "upbit_orderbook" == topic_name:
        upbitOrderbookUnitSchema = StructType([
            StructField("ask_price", DoubleType(), True),
            StructField("bid_price", DoubleType(), True),
            StructField("ask_size", DoubleType(), True),
            StructField("bid_size", DoubleType(), True),
        ])
        
        return StructType([
            StructField("type", StringType(), True),
            StructField("code", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("total_ask_size", DoubleType(), True),
            StructField("total_bid_size", DoubleType(), True),
            StructField("orderbook_units", ArrayType(upbitOrderbookUnitSchema), True),
            StructField("stream_type", StringType(), True),
            StructField("level", IntegerType(), True),
            StructField("arrive_time", DoubleType(), True)
        ])

parser = argparse.ArgumentParser(description='Spark job arguments')
parser.add_argument('--kafka-bootstrap-server-list-file-name', required=True, type=str, help='Kafka broker ip list file')
parser.add_argument('--execution-date', required=True, type=str, help='Airflow task execution date')
parser.add_argument('--gcs-name', required=True, type=str, help='Google Cloud Storage name')
parser.add_argument('--gcs-save-path', required=True, type=str, help='Google Cloud Storage save path')
parser.add_argument('--app-name', required=True, type=str, help='Spark app name')
parser.add_argument('--dollar-bar-size', required=True, type=int, help='sampling dollar bar size')
args = parser.parse_args()

spark = SparkSession.builder.appName(args.app_name) \
                .getOrCreate()
orderbook_schema = load_schema("upbit_orderbook")
trade_schema = load_schema("upbit_trade")

orderbook_gcs_path = f"gs://{args.gcs_name}/raw-data/{args.gcs_save_path}/orderbook/processing_date={args.execution_date}/**/*.json"
trade_gcs_path = f"gs://{args.gcs_name}/raw-data/{args.gcs_save_path}/trade/processing_date={args.execution_date}/**/*.json"

orderbook_df = spark.read.schema(orderbook_schema).json(orderbook_gcs_path)

trade_df = spark.read.schema(trade_schema).json(trade_gcs_path)
trade_dollar_df = trade_df.withColumn("trade_dollar",
                                      col("trade_volume") * col("trade_price")) \
                        .orderBy(col("code"), col("timestamp"))
window = Window.partitionBy("code").orderBy("timestamp")
trade_cumsum_df = trade_dollar_df.withColumn("cumsum", 
                                             sum("trade_dollar").over(window))
trade_dollar_bar_df = trade_cumsum_df.withColumn("dollar_bar",
                                        col("cumsum") // args.dollar_bar_size)
trade_sampled_by_dollar_bar_df = \
                    trade_dollar_bar_df.groupBy("code", "dollar_bar_num").agg(
                        func.last("timestamp").alias("timestamp"),
                        func.first("trade_price").alias("open"),
                        func.max("trade_price").alias("high"),
                        func.min("trade_price").alias("low"),
                        func.last("trade_price").alias("close"),
                        func.sum("trade_dollar").alias("trade_dollar"),
                        func.sum("trade_volume").alias("trade_volume")
                    )

