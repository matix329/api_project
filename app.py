from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Data(BaseModel):
    key: str
    value: str

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/data")
async def send_data(data: Data):
    return {"message": "Data received", "data": data.dict()}