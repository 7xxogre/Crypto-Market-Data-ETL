from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import from_json, col, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType

import threading

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 kafka_spark_consumer.py
kafka_bootstrap_servers = "34.64.107.102:9092,34.22.77.51:9092,34.64.167.7:9092"
upbit_orderbook_topic = "upbit_orderbook"
upbit_trade_topic = "upbit_trade"

spark = SparkSession.builder.appName("kafka-to-kafka-streaming").getOrCreate()

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
    StructField("stream_type", StringType(), True)
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
    StructField("level", IntegerType(), True)
])

transformed_orderbook_df = orderbook_df.selectExpr("CAST(value AS STRING)") \
                                        .select(from_json(col("value"), orderbookSchema).alias("data")) \
                                        .select("data.type", "data.code", "data.timestamp", "data.total_ask_size", "data.total_bid_size", "data.orderbook_units")
transformed_trade_df = trade_df.selectExpr("CAST(value AS STRING)") \
                                .select(from_json(col("value"), tradeSchema).alias("data")) \
                                .select("data.type", "data.code", "data.timestamp", "data.trade_timestamp", "data.trade_price", "data.trade_volume", "data.ask_bid")

date_orderbook_df = transformed_orderbook_df.withColumn("processing_date", current_date())
date_trade_df = transformed_trade_df.withColumn("processing_date", current_date())

processed_ob_df = date_orderbook_df.groupBy("code").agg( 
                            func.last(col("data.code")).alias("code"), 
                            func.last(col("data.orderbook_units[0].ask_price")).alias("ask_price"), 
                            func.last(col("data.orderbook_units[0].bid_price")).alias("bid_price"), 
                            func.last(col("data.timestamp")).alias("upbit_server_time"), 
                            func.last(col("processing_date")).alias("our_time") 
                        )
processed_tr_df = date_trade_df.groupBy("code").agg(
                            func.first(col("data.trade_price")).alias("open"),
                            func.max(col("data.trade_price")).alias("high"),
                            func.min(col("data.trade_price")).alias("low"),
                            func.last(col("data.trade_price")).alias("close"),
                            func.last(col("data.timestamp")).alias("upbit_server_time"),
                            func.last(col("data.trade_timestamp")).alias("trade_time"),
                            func.sum(col("data.trade_volume")).alias("total_trade_volume"),
                            func.sum(func.when(col("ask_bid") == "ASK", col("trade_volume")).otherwise(0)).alias("total_ask_volume"),
                            func.sum(func.when(col("ask_bid") == "BID", col("trade_volume")).otherwise(0)).alias("total_bid_volume")
                        )



ob_query = processed_ob_df.writeStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                        .option("topic", "processed_upbit_orderbook") \
                        .option("checkpointLocation", "/path/to/checkpoint/upbit_ob") \
                        .trigger(processingTime = "2 seconds") \
                        .start()

tr_query = processed_tr_df.writeStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                        .option("topic", "processed_upbit_trade") \
                        .option("checkpointLocation", "/path/to/checkpoint/upbit_tr") \
                        .trigger(processingTime = "2 seconds") \
                        .start()



def awaitTermination(query):
    query.awaitTermination()

thread1 = threading.Thread(target = awaitTermination, args=(ob_query,))
thread2 = threading.Thread(target = awaitTermination, args=(tr_query,))

thread1.start()
thread2.start()
thread1.join()
thread2.join()