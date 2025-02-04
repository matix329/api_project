from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer
import json
import logging
import platform
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

producer_config = {
    'bootstrap.servers': 'localhost:29092'
}
producer = Producer(producer_config)

class Data(BaseModel):
    title: str
    message: str

@app.get("/")
async def read_root():
    logger.info("Root endpoint accessed.")
    system_info = {
        "platform": platform.system(),
        "platform_version": platform.version(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "server_time": time.time()
    }
    return system_info

@app.post("/data")
async def send_data(data: Data):
    try:
        if data.title == "topic_1":
            producer.produce('topic_1', key=data.title, value=json.dumps(data.model_dump()))
        elif data.title == "topic_2":
            producer.produce('topic_2', key=data.title, value=json.dumps(data.model_dump()))
        else:
            producer.produce('default_topic', key=data.title, value=json.dumps(data.model_dump()))
        producer.flush()
        return {"message": "Data sent to Kafka", "data": data.model_dump()}
    except Exception as e:
        return {"message": "An error occurred", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)