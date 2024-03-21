from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import from_json, col, to_timestamp, from_unixtime, window
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
                                        .select("data.code", "data.timestamp", "data.total_ask_size", "data.total_bid_size", "data.orderbook_units", "data.arrive_time")
transformed_trade_df = trade_df.selectExpr("CAST(value AS STRING)") \
                                .select(from_json(col("value"), tradeSchema).alias("data")) \
                                .select("data.code", "data.timestamp", "data.trade_timestamp", "data.trade_price", "data.trade_volume", "data.ask_bid", "data.arrive_time")

date_orderbook_df = transformed_orderbook_df.withColumn("time_diff", col("arrive_time") - col("timestamp") / 1000) \
                                            .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp") / 1000)))
date_trade_df = transformed_trade_df.withColumn("time_diff", col("arrive_time") - col("timestamp") / 1000) \
                                    .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp") / 1000)))

processed_ob_df = date_orderbook_df.withWatermark("timestamp", "10 second") \
                                    .groupBy(window(col("timestamp"), "10 second"), "code").agg(
                                        func.expr("last(orderbook_units[0].ask_price) as ask_price"),
                                        func.expr("last(orderbook_units[0].bid_price) as bid_price"),
                                        func.last(col("timestamp")).alias("upbit_server_time"),
                                        func.last(col("arrive_time")).alias("arrive_time"),
                                        func.mean(col("time_diff")).alias("time_diff")
                                    )
processed_tr_df = date_trade_df.withWatermark("timestamp", "10 second") \
                                .groupBy(window(col("timestamp"), "10 second"), "code").agg(
                                    func.first(col("trade_price")).alias("open"),
                                    func.max(col("trade_price")).alias("high"),
                                    func.min(col("trade_price")).alias("low"),
                                    func.last(col("trade_price")).alias("close"),
                                    func.last(col("timestamp")).alias("upbit_server_time"),
                                    func.last(col("trade_timestamp")).alias("trade_time"),
                                    func.sum(col("trade_volume")).alias("total_trade_volume"),
                                    func.sum(func.when(col("ask_bid") == "ASK", col("trade_volume")).otherwise(0)).alias("total_ask_volume"),
                                    func.sum(func.when(col("ask_bid") == "BID", col("trade_volume")).otherwise(0)).alias("total_bid_volume"),
                                    func.last(col("arrive_time")).alias("arrive_time"),
                                    func.mean(col("time_diff")).alias("time_diff")
                                )


json_ob_df = processed_ob_df.select(func.to_json(func.struct(*processed_ob_df.columns)) \
                                    .alias('value'))
json_tr_df = processed_tr_df.select(func.to_json(func.struct(*processed_tr_df.columns)) \
                                    .alias('value'))

ob_query = json_ob_df.writeStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                        .option("topic", "processed_upbit_orderbook") \
                        .option("checkpointLocation", "/path/to/checkpoint/upbit_ob") \
                        .trigger(processingTime = "10 seconds") \
                        .start()

tr_query = json_tr_df.writeStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                        .option("topic", "processed_upbit_trade") \
                        .option("checkpointLocation", "/path/to/checkpoint/upbit_tr") \
                        .trigger(processingTime = "10 seconds") \
                        .start()



def awaitTermination(query):
    query.awaitTermination()

thread1 = threading.Thread(target = awaitTermination, args=(ob_query,))
thread2 = threading.Thread(target = awaitTermination, args=(tr_query,))

thread1.start()
thread2.start()
thread1.join()
thread2.join()