import argparse
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as func
from pyspark.sql.functions import col, floor, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, \
                                IntegerType, DoubleType, ArrayType
"""
    GCS의 RAW orderbook과 trade 데이터를 전처리해 GCS에 저장하는 코드
    Airflow의 DAG를 사용하여 스케쥴링
"""
def load_schema(schema_name: str) -> StructType:
    """ topic 이름에 맞는 schema를 리턴
    
    parameter:
        schema_name: 스키마의 이름 (String)

    return:
        Pyspark Schema: 정의된 스키마 (StructType)
    """
    if schema_name not in ['upbit_trade', 'upbit_orderbook']:
        raise Exception("topic name이 적절하지 않습니다. \
                        [upbit_trade, upbit_orderbook] 중에 있어야합니다.")
    
    if "upbit_trade" == schema_name:
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
    
    if "upbit_orderbook" == schema_name:
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
            StructField("orderbook_units", 
                        ArrayType(upbitOrderbookUnitSchema), True),
            StructField("stream_type", StringType(), True),
            StructField("level", IntegerType(), True),
            StructField("arrive_time", DoubleType(), True)
        ])

def get_raw_data_df_from_gcs(schema_name: str, args: argparse.Namespace, folder_name: str, spark):
    """ gcs의 raw 데이터를 spark df로 로드하는 함수

    parameter:
        schema_name: 스키마 이름
        args: 제공된 argment parser
        folder_name: args.gcs_save_path 이하 폴더 이름 (orderbook 또는 trade)
        spark: 빌드된 SparkSession

    return: pyspark dataframe
    """
    if folder_name not in ['orderbook', 'trade']:
        raise Exception("folder_name이 적절치 않습니다. \
                        folder_name은 ['orderbook', 'trade'] 내에 속해야 합니다.")
    
    schema = load_schema(schema_name)
    wrapped_schema = StructType([
        StructField("data", schema, True)
    ])
    gcs_path = f"gs://{args.gcs_name}/raw-data/{args.gcs_save_path}/{folder_name}/processing_date={args.execution_date}/**/*.json"
    df = spark.read.schema(wrapped_schema).json(gcs_path)
    return df


parser = argparse.ArgumentParser(description='Spark job arguments')
parser.add_argument('--kafka-bootstrap-server-list-file-name', required=True, type=str, help='Kafka broker ip list file')
parser.add_argument('--execution-date', required=True, type=str, help='Airflow task execution date')
parser.add_argument('--gcs-name', required=True, type=str, help='Google Cloud Storage name')
parser.add_argument('--gcs-save-path', required=True, type=str, help='Google Cloud Storage save path')
parser.add_argument('--app-name', required=True, type=str, help='Spark app name')
parser.add_argument('--dollar-bar-size', required=True, type=int, help='sampling dollar bar size')
args = parser.parse_args()
print("execution_date: ", args.execution_date)

spark = SparkSession.builder.appName(args.app_name).getOrCreate()

orderbook_df = get_raw_data_df_from_gcs("upbit_orderbook", args, "orderbook", spark) \
                    .select("data.code", "data.timestamp", "data.orderbook_units") \
                    .withColumnRenamed("timestamp", "ob_timestamp")

trade_df = get_raw_data_df_from_gcs("upbit_trade", args, "trade", spark)\
                    .select("data.code", "data.timestamp", "data.trade_price", 
                            "data.trade_volume", "data.ask_bid")

trade_dollar_df = trade_df.withColumn("trade_dollar",
                                      col("trade_volume") * col("trade_price")) \
                        .orderBy(col("code"), col("timestamp"))


window = Window.partitionBy("code").orderBy("timestamp")

trade_cumsum_df = trade_dollar_df.withColumn("cumsum", 
                                             func.sum("trade_dollar").over(window))

trade_dollar_bar_df = trade_cumsum_df.withColumn("dollar_bar_num",
                                        floor(col("cumsum") / args.dollar_bar_size).cast(IntegerType())) \
                                    .drop("cumsum")

tr_sampled_by_dollar_bar_df = \
                    trade_dollar_bar_df.groupBy("code", "dollar_bar_num").agg(
                        func.last("timestamp").alias("timestamp"),
                        func.first("trade_price").alias("open"),
                        func.max("trade_price").alias("high"),
                        func.min("trade_price").alias("low"),
                        func.last("trade_price").alias("close"),
                        func.sum("trade_dollar").alias("trade_dollar"),
                        func.sum("trade_volume").alias("trade_volume"),
                        func.sum(
                            func.when(
                                col("ask_bid") == "ASK", col("trade_volume")
                            ).otherwise(0)
                        ).alias("ask_trade_volume"),
                        func.sum(
                            func.when(
                                col("ask_bid") == "BID", col("trade_volume")
                            ).otherwise(0)
                        ).alias("bid_trade_volume")
                    )

join_condition = [
    orderbook_df.code == tr_sampled_by_dollar_bar_df.code,
    orderbook_df.ob_timestamp <= tr_sampled_by_dollar_bar_df.timestamp,
    orderbook_df.ob_timestamp >= tr_sampled_by_dollar_bar_df.timestamp - 1000*10
]


tr_ob_joined_df = tr_sampled_by_dollar_bar_df.join(orderbook_df, 
                                                      join_condition, "left") \
                                              .drop(orderbook_df["code"])

window_spec = Window.partitionBy("code", "dollar_bar_num") \
                    .orderBy(col("ob_timestamp").desc())

final_joined_df = tr_ob_joined_df.withColumn("row_num", func.row_number().over(window_spec)) \
                                .filter(col("row_num") == 1) \
                                .drop("row_num", "ob_timestamp") \
                                .withColumn("processing_date", lit(args.execution_date))

final_joined_df.write \
            .format("json") \
            .option("path", f"gs://{args.gcs_name}/processed_data/{args.gcs_save_path}/") \
            .partitionBy("processing_date", "code").mode("append").save()

spark.stop()
