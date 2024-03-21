import argparse
import json
import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType
from kafka import KafkaConsumer, TopicPartition
from datetime import timedelta
from google.cloud import storage

def download_blob(bucket_name, source_blob_name):
    """GCS에서 파일 내용을 다운로드하고 출력합니다."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    
    # Blob의 내용을 문자열로 다운로드
    contents = blob.download_as_string()
    
    return contents.decode("utf-8").split()

def get_kafka_offset(bootstrap_servers, topic_name, num_partition, execution_date, interval_hours):
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000
    )

    prev_execution_date = execution_date - timedelta(hours = interval_hours)
    topic_partitions = TopicPartition(topic_name, num_partition)

    start_offsets = consumer.offsets_for_times({
        tp: int(prev_execution_date.timestamp() * 1000) for tp in topic_partitions
    })
    end_offsets = consumer.offsets_for_times({
        tp: int(execution_date.timestamp() * 1000) for tp in topic_partitions
    })
    return {"start_offset": start_offsets[topic_partitions[0]].offset, 
            "end_offset": end_offsets[topic_partitions[0]].offset}

def load_schema(topic_name: str) -> StructType:
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
            StructField("stream_type", StringType(), True)
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
            StructField("level", IntegerType(), True)
        ])


parser = argparse.ArgumentParser(description='Spark job arguments')
parser.add_argument('--kafka-bootstrap-server-list-file-name', required=True, type=str, help='Kafka broker ip list file')
parser.add_argument('--topic-name', required=True, type=str, help='Kafka topic name')
parser.add_argument('--num-partitions', required=True, type=int, help='Number of partitions')
parser.add_argument('--execution_date', required=True, type=str, help='Airflow task execution date')
parser.add_argument('--gcs-name', required=True, type=str, help='Google Cloud Storage name')
parser.add_argument('--gcs-save-path', required=True, type=str, help='Google Cloud Storage save path')
parser.add_argument('--app-name', required=True, type=str, help='Spark app name')
args = parser.parse_args()

kafka_bootstrap_servers = download_blob(args.gcs_name, args.kafka_bootstrap_server_list_file_name)
kafka_bootstrap_servers_str = ",".join(kafka_bootstrap_servers)
offsets = get_kafka_offset(kafka_bootstrap_servers_str, args.topic_name, args.num_partition, )
start_offset, end_offset = offsets['start_offset'], offsets['end_offset']

if start_offset < end_offset:
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    # schema load
    schema = load_schema(args.topic_name)
    df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers_str) \
            .option("subscribe", args.topic_name) \
            .option("startingOffsets", f"""{{"{args.topic_name}":{{"{args.num_partition}":{start_offset}}}}}""") \
            .option("endingOffsets", f"""{{"{args.topic_name}":{{"{args.num_partition}":{end_offset}}}}}""") \
            .load()

    transformed_df = df.selectExpr("CAST(value AS STRING)") \
                        .select(from_json(col("value"), schema).alias("data"))
    date_df = transformed_df.withColumn("start_offset", start_offset).withColumn("end_offset", end_offset).withColumn("processing_date", current_date())

    date_df.write \
        .format("json") \
        .option("checkpointLocation", f"gs://{args.gcs_name}/checkpoints/{args.gcs_save_path}/") \
        .option("path", f"gs://{args.gcs_name}/raw-data/{args.gcs_save_path}/") \
        .partitionBy("processing_date", "code") \
        .save()
    spark.stop()
