from utils.config import load_kafka_config
from confluent_kafka import Producer
import socket

class ProducerNode:
    def __init__(self, topic):
        kafka_config = load_kafka_config()
        self.producer = Producer(kafka_config)
        self.topic = topic
        hostname = socket.gethostname()
        self.ip_address = socket.gethostbyname(hostname)