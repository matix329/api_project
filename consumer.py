from confluent_kafka import Consumer, KafkaError, KafkaException
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer_config = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['test'])

print("Consumer is ready to consume messages...")

try:
    while True:
        message = consumer.poll(timeout=1.0)
        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(f"Consumer error: {message.error()}")
                continue
        logger.info(f"Received message: {message.value().decode('utf-8')}")
except KafkaException as e:
    logger.error(f"Kafka error: {e}")
finally:
    consumer.close()
    logger.info("Consumer closed.")