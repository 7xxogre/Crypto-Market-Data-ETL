from kafka import KafkaProducer
from json import dumps

class KafkaProducerWrapper():
    def __init__(self, broker_ip_lists, topic_name):
        self.ip_lists = broker_ip_lists
        self.topic_name = topic_name
        print(f"kafka producer start: {topic_name}")
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=broker_ip_lists,
            api_version=(0, 10, 12),
            acks=0,
            compression_type='gzip',
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )

    def send_message(self, message):
        self.kafka_producer.send(self.topic_name, message)

    def flush(self):
        self.kafka_producer.flush()
        