from kafka import KafkaConsumer, TopicPartition
import datetime

# Kafka 서버와 토픽 설정
bootstrap_servers = "34.64.107.102:9092,34.22.77.51:9092,34.64.167.7:9092"
topic_name = 'upbit_orderbook'

# Kafka Consumer 초기화
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
)

# 토픽의 파티션 정보 가져오기
partitions = consumer.partitions_for_topic(topic_name)
topic_partitions = [TopicPartition(topic_name, p) for p in partitions]

# execution_date 및 prev_execution_date에 해당하는 타임스탬프로 오프셋 조회
execution_date = datetime.datetime(2024, 3, 20, 9)
prev_execution_date = execution_date - datetime.timedelta(hours=1)

# 타임스탬프로 오프셋 조회
start_offsets = consumer.offsets_for_times({
    tp: int(prev_execution_date.timestamp() * 1000) for tp in topic_partitions
})
end_offsets = consumer.offsets_for_times({
    tp: int(execution_date.timestamp() * 1000) for tp in topic_partitions
})

print("start_offsets:", start_offsets)
print("end_offsets:", end_offsets)
print(end_offsets[topic_partitions[0]].offset)