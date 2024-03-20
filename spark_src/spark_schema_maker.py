from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType
import os
def save_schema(schema: StructType, file_name: str):
    # schemas 폴더 경로 설정
    folder_path = './schemas'
    
    # schemas 폴더가 존재하지 않는 경우 생성
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    file_path = os.path.join(folder_path, file_name)

    with open(file_path, 'w') as file:
        # StructType의 json() 메서드를 사용하여 스키마를 JSON 문자열로 변환
        json_schema = schema.json()
        file.write(json_schema)

if __name__ == "__main__":


    upbitTradeSchema = StructType([
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
    save_schema(upbitTradeSchema, f"upbit_trade_schema.json")

    upbitOrderbookUnitSchema = StructType([
        StructField("ask_price", DoubleType(), True),
        StructField("bid_price", DoubleType(), True),
        StructField("ask_size", DoubleType(), True),
        StructField("bid_size", DoubleType(), True),
    ])
    save_schema(upbitOrderbookUnitSchema, f"upbit_orderbook_unit.json")
    
    upbitOrderbookSchema = StructType([
        StructField("type", StringType(), True),
        StructField("code", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("total_ask_size", DoubleType(), True),
        StructField("total_bid_size", DoubleType(), True),
        StructField("orderbook_units", ArrayType(upbitOrderbookUnitSchema), True),
        StructField("stream_type", StringType(), True),
        StructField("level", IntegerType(), True)
    ])
    save_schema(upbitOrderbookSchema, f"upbit_orderbook.json")
