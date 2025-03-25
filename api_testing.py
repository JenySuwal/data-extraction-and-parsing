from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import uuid
import redis
import json
from celery_tasks_for_scraping import process_batch
from table_scraping import fetch_tables_html

app = FastAPI()
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

class Schema(BaseModel):
    type: str
    columns: list[str] = []

class RequestData(BaseModel):
    urls: list[str]
    data_schema: Schema

@app.post("/process-data/")
async def process_data(request: Request, request_data: RequestData):
    body = await request.body()
    # print("Received JSON:", body.decode("utf-8"))

    urls = request_data.urls
    schema = request_data.data_schema

    valid_data_schemas = ["table", "image", "html", "pdf"]
    if schema.type not in valid_data_schemas:
        raise HTTPException(status_code=400, detail="Invalid data_schema type")

    urls_with_crawl_ids = []
    for url in urls:
        crawl_id = str(uuid.uuid4())
        urls_with_crawl_ids.append({
            "url": url,
            "crawl_id": crawl_id,
            "schema": {"type": schema.type}
        })

        redis_client.set(f"crawl_{crawl_id}", json.dumps({
            "url": url,
            "schema": {"type": schema.type}
        }))

    # Store batch in Redis
    batch_key = str(uuid.uuid4())  # Unique batch key
    redis_client.setex(batch_key, 300, json.dumps(urls_with_crawl_ids))  # Expire in 5 min
    # print(f"Stored batch in Redis: Key={batch_key}, Data={json.dumps(urls_with_crawl_ids)}")

    # Retrieve batch data
    batch_data = redis_client.get(batch_key)
    # print(f"Retrieved batch from Redis: {batch_data}")

    # Ensure batch_data exists
    if not batch_data:
        raise HTTPException(status_code=400, detail="Batch data not found in Redis")

    batch = json.loads(batch_data)
    print(f"Redis batch data: {batch}")

    crawl_id = batch[0]["crawl_id"]  # Extract crawl_id from first item in batch

    # Send task to Celery
    process_batch.apply_async((batch_key, crawl_id), queue="default")

    return {
        "status": "Batch processing started",
        "urls_with_crawl_ids": urls_with_crawl_ids
    }

