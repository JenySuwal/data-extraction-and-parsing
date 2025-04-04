from fastapi import FastAPI, HTTPException
import redis
import json
import uuid
from pydantic import BaseModel
from working_celery_tasks_for_scraping import process_batch
from fastapi.responses import JSONResponse, FileResponse

app = FastAPI()
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
# Define Schema model
class Schema(BaseModel):
    type: str
    columns: list[str] = []

# Define RequestData model
class RequestData(BaseModel):
    urls: list[str]
    data_schema: Schema
@app.post("/process-data/")
async def process_data(request_data: RequestData):
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
            "schema": {"type": schema.type},
            "progress": 0
        }), ex=3600)  

    batch_key = str(uuid.uuid4())
    redis_client.setex(batch_key, 300, json.dumps(urls_with_crawl_ids))  

    process_batch.apply_async((batch_key,), queue="default")

    return {"status": "Batch processing started", "urls_with_crawl_ids": urls_with_crawl_ids}

@app.get("/get-progress/{crawl_id}")
async def get_progress(crawl_id: str):
    data = redis_client.get(f"crawl_{crawl_id}")
    if not data:
        raise HTTPException(status_code=404, detail="Task not found")
    data = json.loads(data)
    return {"progress": data.get("progress", 0)}

@app.get("/download/{crawl_id}")
async def download_data(crawl_id: str):
    file_path = f"results/{crawl_id}.json"  # Adjust based on storage path
    try:
        return FileResponse(file_path, filename=f"{crawl_id}.json", media_type="application/json")
    except Exception:
        raise HTTPException(status_code=404, detail="File not found")
