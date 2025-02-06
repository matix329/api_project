from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import logging
from config import KAFKA_CONFIG, TOPICS
from db import Database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConsumerService:
    def __init__(self):
        self.db = Database()
        self.db.init_db()
        self.consumer = Consumer(KAFKA_CONFIG)
        self.consumer.subscribe(TOPICS)
        logger.info("Kafka consumer run.")

    def consume_messages(self):
        try:
            while True:
                message = self.consumer.poll(timeout=1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consument error: {message.error()}")
                        continue

                msg_value = json.loads(message.value().decode('utf-8'))
                topic = message.topic()

                self.db.save_message(topic, msg_value["message"])
        except KafkaException as e:
            logger.error(f"afka error: {e}")
        finally:
            self.consumer.close()
            self.db.close()
            logger.info("Kafka Consumer closed.")

if __name__ == "__main__":
    consumer_service = KafkaConsumerService()
    consumer_service.consume_messages()