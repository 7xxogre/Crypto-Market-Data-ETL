"""
    간단히 Spark Structured Streaming을 사용해
    콘솔창에 받은 데이터를 뿌리는 형식

    테스트 구현임을 감안해 Upbit에 대해서만 적용

    spark-submit 시 콘솔에 아래 명령어처럼 kafka를 패키지로 추가해줘야함
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 kafka_spark_consumer_test_code.py
    
    이때 spark 버전과 scala 버전에 주의
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType


kafka_bootstrap_servers = "34.64.98.119:9092,34.47.82.250:9092,34.22.79.101:9092"
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
                    .select("data.code", "data.timestamp", "data.total_ask_size", "data.total_bid_size", "data.orderbook_units")

transformed_trade_df = trade_df.selectExpr("CAST(value AS STRING)") \
                    .select(from_json(col("value"), tradeSchema).alias("data")) \
                    .select("data.code", "data.timestamp", "data.trade_timestamp", "data.trade_price", "data.trade_volume", "data.ask_bid")


query1 = transformed_orderbook_df.writeStream.outputMode("append") \
    .format("console") \
    .trigger(processingTime='10 seconds') \
    .start()

query2 = transformed_trade_df.writeStream.outputMode("append") \
    .format("console") \
    .trigger(processingTime='10 seconds') \
    .start()


query1.awaitTermination()
query2.awaitTermination()