import psycopg2
import logging
from config import DB_PARAMS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.conn = None
        self.connect()

    def connect(self):
        try:
            self.conn = psycopg2.connect(**DB_PARAMS)
            logger.info("Connected to the database.")
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            self.conn = None

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Closed database connection.")

    def execute(self, query, params=None, commit=False, fetch=False):
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                if commit:
                    self.conn.commit()
                if fetch:
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            self.conn.rollback()

    def init_db(self):
        self.execute('CREATE SCHEMA IF NOT EXISTS kafka;', commit=True)

        self.execute('''
            CREATE TABLE IF NOT EXISTS kafka.topic_1 (
                id SERIAL PRIMARY KEY,
                message TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''', commit=True)

        self.execute('''
            CREATE TABLE IF NOT EXISTS kafka.topic_2 (
                id SERIAL PRIMARY KEY,
                message TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''', commit=True)

        logger.info("Database initialized.")

    def save_message(self, topic, message):
        try:
            self.execute(f"INSERT INTO kafka.{topic} (message) VALUES (%s);", (message,), commit=True)
            logger.info(f"Message saved to {topic}.")
        except Exception as e:
            logger.error(f"Database error: {e}")