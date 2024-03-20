from kafka import KafkaConsumer, TopicPartition

# Kafka 서버 설정
bootstrap_servers = ['kafka-server:9092']
topic_name = 'test_topic'
partition = 0

# KafkaConsumer 인스턴스 생성
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # 이 설정은 오프셋 조회에는 영향을 미치지 않습니다.
    consumer_timeout_ms=1000
)

# 특정 토픽의 파티션에 대한 마지막 오프셋 조회
consumer.assign([TopicPartition(topic_name, partition)])

# 파티션의 마지막 오프셋을 가져옵니다.
consumer.seek_to_end()
last_offset = consumer.position(TopicPartition(topic_name, partition)) - 1
print(f"Last offset for topic '{topic_name}', partition {partition}: {last_offset}")

consumer.close()
