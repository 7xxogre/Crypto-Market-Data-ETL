from kafka import KafkaProducer
from json import dumps
import time

topic_name = "testtopic2"

producer = KafkaProducer(
    bootstrap_servers=["34.64.98.119:9092", "34.64.145.172:9092", "34.64.252.120:9092"],
    api_version=(0, 10, 12),
    acks=0,
    compression_type='gzip',
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

start = time.time()
print('[begin] producer 가 메시지 전송을 시작합니다.')

for i in range(10000):
    data = {'str': f'result {str(i)}'}
    print(f'메시지 전송중... {data["str"]}')
    producer.send(topic_name, value=data)
    producer.flush()

print(f'[end] 걸린시간: {time.time() - start}')