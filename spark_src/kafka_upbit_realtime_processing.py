from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import from_json, col, to_timestamp, from_unixtime, \
                                window, pandas_udf, PandasUDFType, Window
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, \
                                IntegerType, LongType, ArrayType
import threading

""" 
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
--conf spark.shuffle.service.enabled=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.initialExecutors=1 \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.maxExecutors=3
kafka_upbit_realtime_processing.py
"""
# 아래 형태로 kafka_broker_ips에 존재해야함.
kafka_bootstrap_servers = "kafka_brokder1:9092,kafka_brokder2:9092,kafka_brokder3:9092"
with open("./kafka_broker_ips.txt", 'r') as f:
    kafka_bootstrap_servers = [s.replace('\n', '') for s in f.readlines()]
    kafka_bootstrap_servers = ','.join(kafka_bootstrap_servers)
print(f"kafka bootstrap server ips: {kafka_bootstrap_servers}")
orderbook_topic = "upbit_orderbook"
trade_topic = "upbit_trade"

spark = SparkSession.builder.appName("kafka-upbit-realtime-processing")\
                    .getOrCreate()

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

@pandas_udf("double", PandasUDFType.GROUPED_AGG)
def calculate_ewma(value_series):
    alpha = 0.8 
    return value_series.ewm(alpha=alpha, adjust=False).mean().iloc[-1]

################### orderbook ###################
ob_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", orderbook_topic) \
            .load()

transformed_ob_df = ob_df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), orderbookSchema).alias("data")) \
                .select("data.code", "data.timestamp", 
                        "data.total_ask_size", "data.total_bid_size", 
                        "data.orderbook_units", "data.arrive_time")

date_ob_df = transformed_ob_df.withColumn("time_diff", 
                                col("arrive_time") - (col("timestamp") / 1000)) \
                            .withColumn("server_datetime", 
                                to_timestamp(from_unixtime(col("timestamp") / 1000))) \
                            .withColumn("obi", 
                                        transformed_ob_df["orderbook_units"][0]["bid_size"] / 
                                        transformed_ob_df["orderbook_units"][0]["ask_size"]) \
                            .withWatermark("server_datetime", "15 minute")

windowSpec = Window.partitionBy("code").orderBy("server_datetime") \
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)

ewma_ob_df = date_ob_df.withColumn("ewma_obi", 
                                    calculate_ewma(date_ob_df["obi"]).over(windowSpec)
                                )

processed_ob_df = ewma_ob_df \
                .groupby(window(col("server_datetime"), "15 minute"), "code") \
                .agg(
                    func.expr("last(orderbook_units[0].ask_price) as ask_price"),
                    func.expr("last(orderbook_units[0].bid_price) as bid_price"),
                    func.last(col("ewma_obi")).alias("ewma_obi"),
                    func.last(col("server_datetime")).alias("server_datetime"),
                    func.last(col("timestamp")).alias("server_time"),
                    func.last(col("arrive_time")).alias("arrive_time"),
                    func.mean(col("time_diff")).alias("time_diff")
                )

json_ob_df = processed_ob_df.select(
                    func.to_json(func.struct(*processed_ob_df.columns)
                 ).alias('value'))

ob_processed_topic = "processed_upbit_orderbook"
ob_chackpoint_loc = "/path/to/checkpoint/upbit_ob_realtime_processing"
ob_query = json_ob_df.writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("topic", ob_processed_topic) \
                .option("checkpointLocation", ob_chackpoint_loc) \
                .trigger(processingTime = "15 seconds") \
                .start()


#################### trade ####################
tr_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", trade_topic) \
            .load()

transformed_tr_df = tr_df.selectExpr("CAST(value AS STRING)") \
                    .select(from_json(col("value"), tradeSchema).alias("data")) \
                    .select("data.code", "data.timestamp", "data.trade_timestamp", 
                            "data.trade_price", "data.trade_volume", "data.ask_bid",
                            "data.arrive_time")

date_tr_df = transformed_tr_df.withColumn("time_diff", 
                                col("arrive_time") - (col("timestamp") / 1000)) \
                            .withColumn("server_datetime", 
                                to_timestamp(from_unixtime(col("timestamp") / 1000)))

processed_tr_df = date_tr_df.withWatermark("server_datetime", "15 minute") \
                .groupby(window(col("server_datetime"), "15 minute"), "code") \
                .agg(
                    func.first(col("trade_price")).alias("open"),
                    func.max(col("trade_price")).alias("high"),
                    func.min(col("trade_price")).alias("low"),
                    func.last(col("trade_price")).alias("close"),
                    func.last(col("server_datetime")).alias("server_datetime"),
                    func.last(col("timestamp")).alias("server_time"),
                    func.last(col("trade_timestamp")).alias("trade_time"),
                    func.sum(col("trade_volume")).alias("total_trade_volume"),
                    func.stddev(col("trade_price")).alias("volatility"),
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
                    func.sum(
                        func.when(
                            col("ask_bid") == "BID", col("trade_volume")
                        ).otherwise(-col("trade_volume"))
                    ).alias("tfi"),
                    func.last(col("arrive_time")).alias("arrive_time"),
                    func.mean(col("time_diff")).alias("time_diff"),
                )

json_tr_df = processed_tr_df.select(
                    func.to_json(func.struct(*processed_tr_df.columns)
                 ).alias('value'))

tr_processed_topic = "processed_upbit_trade"
tr_chackpoint_loc = "/path/to/checkpoint/upbit_tr_realtime_processing"
tr_query = json_tr_df.writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("topic", tr_processed_topic) \
                .option("checkpointLocation", tr_chackpoint_loc) \
                .trigger(processingTime = "15 seconds") \
                .start()
def awaitTermination(query):
    query.awaitTermination()

thread1 = threading.Thread(target = awaitTermination, args=(ob_query,))
thread2 = threading.Thread(target = awaitTermination, args=(tr_query,))

thread1.start()
thread2.start()
thread1.join()
thread2.join()