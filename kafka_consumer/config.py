from dotenv import load_dotenv
import os

load_dotenv()

DB_PARAMS = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}

TOPICS = ['topic_1', 'topic_2']