from kafka import KafkaConsumer
from json import loads
import time

topic_name = "testtopic2"
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=["34.64.98.119:9092", "34.64.145.172:9092", "34.64.252.120:9092"],
    api_version=(0, 10, 12),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    consumer_timeout_ms=1000,
    value_deserializer=lambda x: loads(x.decode('utf-8'))  
)

start = time.time()
print(f'[begin] Topic {topic_name} 으로 consumer 가 메시지 받아옴')
result = []
for message in consumer:
    result.append(message.value)
    print(f'Partition {message.partition}, Offset: {message.offset}, Value: {message.value}')

print(f'[end] 걸린시간:', {time.time() - start})
print(len(result))