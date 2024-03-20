import argparse
import json
import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_date
from kafka import KafkaConsumer, TopicPartition
from datetime import timedelta

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

parser = argparse.ArgumentParser(description='Spark job arguments')
parser.add_argument('--kafka-bootstrap-server-list-path', required=True, type=str, help='Kafka broker ip list path')
parser.add_argument('--topic-name', required=True, type=str, help='Kafka topic name')
parser.add_argument('--num-partitions', required=True, type=int, help='Number of partitions')
parser.add_argument('--execution_date', required=True, type=str, help='Airflow task execution date')
parser.add_argument('--schema-path', required=True, type=str, help='Data schema path')
parser.add_argument('--gcs-name', required=True, type=str, help='Google Cloud Storage name')
parser.add_argument('--gcs-save-path', required=True, type=str, help='Google Cloud Storage save path')
parser.add_argument('--app-name', required=True, type=str, help='Spark app name')
args = parser.parse_args()



with open(args.kafka_bootstrap_server_list_path, 'r') as f:
    kafka_bootstrap_servers = [line.strip() for line in f.readlines()]
    if len(kafka_bootstrap_servers) < 1:
        raise Exception(f"kafka bootstrap server path is not exists!!!! {args.kafka_bootstrap_server_list_path}")

offsets = get_kafka_offset(kafka_bootstrap_servers, args.topic_name, args.num_partition, )
start_offset, end_offset = offsets['start_offset'], offsets['end_offset']

if start_offset < end_offset:
    # schema load
    with open(args.schema_path, 'rb') as f:
        schema = pickle.load(f)

    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
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
