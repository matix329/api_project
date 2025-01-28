from confluent_kafka import Consumer

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
            print(f"Consumer error: {message.error()}")
            continue
        print(f"Received message: {message.value().decode('utf-8')}")
finally:
    consumer.close()