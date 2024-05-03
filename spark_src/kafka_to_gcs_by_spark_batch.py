import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType, IntegerType, ArrayType
from google.cloud import storage

def download_blob(bucket_name, source_blob_name):
    """GCS에서 파일 내용을 다운로드하고 출력합니다."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    
    # Blob의 내용을 문자열로 다운로드
    contents = blob.download_as_string()
    
    return contents.decode("utf-8").split()

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
            StructField("stream_type", StringType(), True),
            StructField("arrive_time", DoubleType(), True)
        ])
    
    elif "upbit_orderbook" == topic_name:
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
    else:
        raise Exception(f"{topic_name}에 맞는 schema를 찾지 못했습니다.")

parser = argparse.ArgumentParser(description='Spark job arguments')
parser.add_argument('--kafka-bootstrap-server-list-file-name', required=True, type=str, help='Kafka broker ip list path')
parser.add_argument('--topic-name', required=True, type=str, help='Kafka topic name')
parser.add_argument('--execution-date', required=True, type=str, help='Airflow task execution date')
parser.add_argument('--gcs-name', required=True, type=str, help='Google Cloud Storage name')
parser.add_argument('--gcs-save-path', required=True, type=str, help='Google Cloud Storage save path')
parser.add_argument('--app-name', required=True, type=str, help='Spark app name')
parser.add_argument('--kafka-start-offset', required=True, type=str, help='kafka start offset')
parser.add_argument('--kafka-end-offset', required=True, type=str, help='kafka end offset')
args = parser.parse_args()

kafka_bootstrap_servers = download_blob(args.gcs_name, args.kafka_bootstrap_server_list_file_name)
kafka_bootstrap_servers_str = ",".join(kafka_bootstrap_servers)

# schema load
schema = load_schema(args.topic_name)
start_offsets, end_offsets = int(args.kafka_start_offset), int(args.kafka_end_offset)
print("start_offset: ", start_offsets)
print("end_offset: ", end_offsets)

spark = SparkSession.builder.appName(args.app_name).getOrCreate()
df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers_str) \
        .option("subscribe", args.topic_name) \
        .option("startingOffsets", f"""{{"{args.topic_name}":{{"0":{start_offsets}}}}}""") \
        .option("endingOffsets", f"""{{"{args.topic_name}":{{"0":{end_offsets}}}}}""") \
        .load()

transformed_df = df.selectExpr("CAST(value AS STRING)") \
                    .select(from_json(col("value"), schema).alias("data"))
date_df = transformed_df.withColumn("processing_date", lit(args.execution_date)).withColumn("code", col("data.code"))

date_df.printSchema()
date_df.write \
    .format("json") \
    .option("path", f"gs://{args.gcs_name}/raw-data/{args.gcs_save_path}/") \
    .partitionBy("processing_date", "code").mode("append") \
    .save()
spark.stop()
