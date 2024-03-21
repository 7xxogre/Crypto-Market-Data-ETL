from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, from_json, col, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType

import threading
# 아래 명령어처럼 kafka를 추가해 스파크를 실행해 주어야함.
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 kafka_spark_consumer.py
kafka_bootstrap_servers = "kafka_brokder1:9092,kafka_brokder2:9092,kafka_brokder3:9092"
upbit_orderbook_topic = "upbit_orderbook"
upbit_trade_topic = "upbit_trade"

spark = SparkSession.builder.appName("kafka-consume").getOrCreate()
orderbook_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", upbit_orderbook_topic) \
        .load()
trade_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", upbit_trade_topic) \
        .load()


tradeSchema = StructType([
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
    StructField("arrive_time", LongType(), True)
])

orderbookUnitSchema = StructType([
    StructField("ask_price", DoubleType(), True),
    StructField("bid_price", DoubleType(), True),
    StructField("ask_size", DoubleType(), True),
    StructField("bid_size", DoubleType(), True),
])
orderbookSchema = StructType([
    StructField("type", StringType(), True),
    StructField("code", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("total_ask_size", DoubleType(), True),
    StructField("total_bid_size", DoubleType(), True),
    StructField("orderbook_units", ArrayType(orderbookUnitSchema), True),
    StructField("stream_type", StringType(), True),
    StructField("level", IntegerType(), True),
    StructField("arrive_time", LongType(), True)
])

transformed_orderbook_df = orderbook_df.selectExpr("CAST(value AS STRING)") \
                    .select(from_json(col("value"), orderbookSchema).alias("data")) \
                    .select("data.type", "data.code", "data.timestamp", "data.total_ask_size", "data.total_bid_size", "data.orderbook_units", "data.arrive_time")

transformed_trade_df = trade_df.selectExpr("CAST(value AS STRING)") \
                    .select(from_json(col("value"), tradeSchema).alias("data")) \
                    .select("data.type", "data.code", "data.timestamp", "data.trade_timestamp", "data.trade_price", "data.trade_volume", "data.ask_bid", "data.arrive_time")

date_orderbook_df = transformed_orderbook_df.withColumn("processing_date", current_date())
date_trade_df = transformed_trade_df.withColumn("processing_date", current_date())

# checkpoint는 다르게 해야해!!! 같이 하면 나중꺼밖에 저장이 안됨
# gcs 이름 설정해주기!!!
query1 = date_orderbook_df.writeStream \
                    .format("json") \
                    .option("checkpointLocation", "gs://gcs-name/checkpoints/upbit/orderbook/") \
                    .option("path", "gs://gcs-name/raw-data/upbit/orderbook/") \
                    .partitionBy("processing_date", "code") \
                    .trigger(processingTime='30 minutes') \
                    .start()

query2 = date_trade_df.writeStream \
                    .format("json") \
                    .option("checkpointLocation", "gs://gcs-name/checkpoints/upbit/trade/") \
                    .option("path", "gs://gcs-name/raw-data/upbit/trade/") \
                    .partitionBy("processing_date", "code") \
                    .trigger(processingTime='30 minutes') \
                    .start()


# query1.awaitTermination()
# query2.awaitTermination()
def awaitTermination(query):
    query.awaitTermination()

thread1 = threading.Thread(target = awaitTermination, args=(query1,))
thread2 = threading.Thread(target = awaitTermination, args=(query2,))

thread1.start()
thread2.start()
thread1.join()
thread2.join()