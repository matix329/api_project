from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer
import json

app = FastAPI()

producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

class Data(BaseModel):
    key: str
    value: str

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/data")
async def send_data(data: Data):
    try:
        producer.produce('test', key=data.key, value=json.dumps(data.dict()))
        producer.flush()
        return {"message": "Data sent to Kafka", "data": data.dict()}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)