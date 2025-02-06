from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, model_validator
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

    @model_validator(mode="before")
    @classmethod
    def check_title(cls, values):
        if values.get('title') not in ['topic_1', 'topic_2']:
            raise ValueError("Title must be either 'topic_1' or 'topic_2'")
        return values

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
            producer.produce('topic_1', key=data.title, value=json.dumps(data.dict()))
        elif data.title == "topic_2":
            producer.produce('topic_2', key=data.title, value=json.dumps(data.dict()))
        else:
            producer.produce('default_topic', key=data.title, value=json.dumps(data.dict()))

        producer.flush()
        return {"message": "Data sent to Kafka", "data": data.dict()}

    except Exception as e:
        logger.error(f"Error sending data: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)