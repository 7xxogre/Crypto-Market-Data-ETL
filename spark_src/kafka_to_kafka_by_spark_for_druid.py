from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import from_json, col, to_timestamp, from_unixtime, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType

import threading

""" 
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.initialExecutors=1 \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=3
kafka_to_kafka_by_spark_for_druid.py
"""
with open("./kafka_broker_ips.txt", 'r') as f:
    kafka_bootstrap_servers = [s.replace('\n', '') for s in f.readlines()]
    kafka_bootstrap_servers = ','.join(kafka_bootstrap_servers)
print(f"kafka bootstrap server ips: {kafka_bootstrap_servers}")
upbit_orderbook_topic = "upbit_orderbook"
upbit_trade_topic = "upbit_trade"

spark = SparkSession.builder.appName("kafka-to-kafka-streaming").getOrCreate()

ob_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", upbit_orderbook_topic) \
        .load()
tr_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", upbit_trade_topic) \
        .load()

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
    StructField("arrive_time", DoubleType(), True)
])

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
    StructField("arrive_time", DoubleType(), True)
])


transformed_ob_df = ob_df.selectExpr("CAST(value AS STRING)") \
                        .select(from_json(col("value"), orderbookSchema).alias("data")) \
                        .select("data.code", "data.timestamp", 
                                "data.total_ask_size", "data.total_bid_size", 
                                "data.orderbook_units", "data.arrive_time")

transformed_tr_df = tr_df.selectExpr("CAST(value AS STRING)") \
                        .select(from_json(col("value"), tradeSchema).alias("data")) \
                        .select("data.code", "data.timestamp", "data.trade_timestamp", 
                                "data.trade_price", "data.trade_volume", 
                                "data.ask_bid", "data.arrive_time")

date_ob_df = transformed_ob_df.withColumn("time_diff", 
                                          col("arrive_time") - col("timestamp") / 1000) \
                        .withColumn("server_datetime", 
                                    to_timestamp(from_unixtime(col("timestamp") / 1000)))
date_tr_df = transformed_tr_df.withColumn("time_diff", 
                                          col("arrive_time") - col("timestamp") / 1000) \
                        .withColumn("server_datetime",
                                     to_timestamp(from_unixtime(col("timestamp") / 1000)))

processed_ob_df = date_ob_df.withWatermark("server_datetime", "10 second") \
                .groupBy(window(col("server_datetime"), "10 second"), "code") \
                .agg(
                    func.expr("last(orderbook_units[0].ask_price) as ask_price"),
                    func.expr("last(orderbook_units[0].bid_price) as bid_price"),
                    func.last(col("server_datetime")).alias("server_datetime"),
                    func.last(col("timestamp")).alias("server_time"),
                    func.last(col("arrive_time")).alias("arrive_time"),
                    func.mean(col("time_diff")).alias("time_diff")
                )
processed_tr_df = date_tr_df.withWatermark("server_datetime", "10 second") \
                .groupBy(window(col("server_datetime"), "10 second"), "code") \
                .agg(
                    func.first(col("trade_price")).alias("open"),
                    func.max(col("trade_price")).alias("high"),
                    func.min(col("trade_price")).alias("low"),
                    func.last(col("trade_price")).alias("close"),
                    func.last(col("server_datetime")).alias("server_datetime"),
                    func.last(col("timestamp")).alias("server_time"),
                    func.last(col("trade_timestamp")).alias("trade_time"),
                    func.sum(col("trade_volume")).alias("total_trade_volume"),
                    func.sum(
                        func.when(
                            col("ask_bid") == "ASK", col("trade_volume")
                        ).otherwise(0)
                    ).alias("total_ask_volume"),
                    func.sum(
                        func.when(
                            col("ask_bid") == "BID", col("trade_volume")
                        ).otherwise(0)
                    ).alias("total_bid_volume"),
                    func.last(col("arrive_time")).alias("arrive_time"),
                    func.mean(col("time_diff")).alias("time_diff")
                )

def make_json_df(df):
    return df.select(
                func.to_json(func.struct(*df.columns)
            ).alias('value'))

def make_send_data_to_kafka_query(df, kafka_bootstrap_servers: list, 
                                  send_topic: str, checkpoint_loc: str, 
                                  processing_interval_second: int):
    json_df = make_json_df(df)
    query = json_df.writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("topic", send_topic) \
                .option("checkpointLocation", checkpoint_loc) \
                .trigger(processingTime=f"{processing_interval_second} second") \
                .start()
    return query


ob_processed_topic = "processed_upbit_orderbook"
ob_chackpoint_loc = "/path/to/checkpoint/upbit_ob_with_arrive_time"
ob_query = make_send_data_to_kafka_query(processed_ob_df, kafka_bootstrap_servers,
                                         ob_processed_topic, ob_chackpoint_loc,
                                         10)

tr_processed_topic = "processed_upbit_trade"
tr_chackpoint_loc = "/path/to/checkpoint/upbit_tr_with_arrive_time"
tr_query = make_send_data_to_kafka_query(processed_tr_df, kafka_bootstrap_servers,
                                         tr_processed_topic, tr_chackpoint_loc,
                                         10)


def awaitTermination(query):
    query.awaitTermination()

thread1 = threading.Thread(target = awaitTermination, args=(ob_query,))
thread2 = threading.Thread(target = awaitTermination, args=(tr_query,))

thread1.start()
thread2.start()
thread1.join()
thread2.join()