import json
import random
import string
from confluent_kafka import Producer
import time

producer_config = {
    'bootstrap.servers': 'localhost:29092'
}
producer = Producer(producer_config)

def generate_random_message():
    key = random.choice(['topic_1', 'topic_2'])
    message = ''.join(random.choices(string.ascii_letters + string.digits, k=50))
    return key, message

def send_messages(num_messages=10000):
    for i in range(num_messages):
        key, message = generate_random_message()
        topic = key
        print(f"Sending message {i + 1}/{num_messages} to {topic} with key {key}")
        producer.produce(topic, key=key, value=json.dumps({'message': message}))

    producer.flush()

if __name__ == "__main__":
    start_time = time.time()
    send_messages(10000)
    end_time = time.time()

    print(f"\n100 messages sent in {end_time - start_time:.2f} seconds.")