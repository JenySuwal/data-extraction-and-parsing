from fastapi import FastAPI, HTTPException
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
import hashlib
import uuid
from pydantic import HttpUrl
import redis
import json
from celery_tasks_for_scraping import celery_app, process_batch
from table_parsing import parse_task
from typing import List
from fastapi.middleware.cors import CORSMiddleware
from download_from_s3 import download_entire_parsed_data_as_zip
from scrape_links import scrape_links_from_url

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins (for development only)
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)


redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

STATUS_SCRAPING = "scraping"
STATUS_SCRAPING = "scraping"
STATUS_SCRAPED = "scraped"
STATUS_UPLOADING = "uploading"
STATUS_UPLOADED = "uploaded"
STATUS_PARSING = "parsing"
STATUS_PARSED = "parsed"
STATUS_FAILED = "failed"

class Schema(BaseModel):
    type: str
    columns: List[str] = []

class RequestData(BaseModel):
    urls: List[str]
    data_schema: Schema

def generate_crawl_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()

def update_status(crawl_id: str, status: str, error_message: str = None):
    """Update the status of a crawl in Redis."""
    status_data = {"status": status}
    if error_message:
        status_data["error"] = error_message
    redis_client.set(f"status_{crawl_id}", json.dumps(status_data))
INPUT_BUCKET = "scraped-unstructured-data"

@app.get("/scrape-links", response_model=List[str])
def scrape_links(url: HttpUrl = Query(..., description="URL to scrape")):
    links = scrape_links_from_url(str(url))
    
    if links:
        output_file = "scraped_links.txt"
        with open(output_file, "w") as f:
            for link in links:
                f.write(f'"{link}",\n')
        print(f"\n🔗 Saved to {output_file}")
    
    return links
import logging
logging.basicConfig(level=logging.INFO)
@app.post("/scraping-data/")
async def process_data(request_data: RequestData):
    logging.info("Received API request for scraping")
    urls = request_data.urls
    schema = request_data.data_schema

    valid_data_schemas = ["table", "image", "html", "pdf"]
    if schema.type not in valid_data_schemas:
        raise HTTPException(status_code=400, detail="Invalid data_schema type")

    batch_size = 30  
    batches = []
    current_batch = []
    response_data = []

    for url in urls:
        crawl_id = generate_crawl_id(url)

        failed_data = redis_client.get(f"failed_{crawl_id}")
        if failed_data:
            failed_info = json.loads(failed_data)
            print(f"Skipping {url}, it previously failed with error: {failed_info['error']}")
            continue  

        
        update_status(crawl_id, STATUS_SCRAPING)

        response_data.append({"url": url, "crawl_id": crawl_id})

        current_batch.append(url)
        if len(current_batch) == batch_size:
            batches.append(current_batch)
            current_batch = []

    if current_batch:
        batches.append(current_batch)

    if not batches:
        return {"status": "No valid URLs to process (all failed previously)"}

    first_batch_key = None  
    previous_batch_key = None  

    for batch in batches:
        batch_key = str(uuid.uuid4())  
        urls_with_crawl_ids = []

        for url in batch:
            crawl_id = generate_crawl_id(url)
            urls_with_crawl_ids.append({
                "url": url,
                "crawl_id": crawl_id,
                "schema": {"type": schema.type}
            })

            
            if not redis_client.exists(f"crawl_{crawl_id}"):
                redis_client.set(f"crawl_{crawl_id}", json.dumps({
                    "url": url,
                    "schema": {"type": schema.type}
                }))
            
            update_status(crawl_id, STATUS_SCRAPING)

        redis_client.setex(batch_key, 300, json.dumps(urls_with_crawl_ids))  

        if first_batch_key is None:
            first_batch_key = batch_key  

        if previous_batch_key:
            redis_client.set(f"next_batch_{previous_batch_key}", batch_key)  

        previous_batch_key = batch_key  

    logging.info(f"Submitting batch task with key {first_batch_key}")
    process_batch.apply_async((first_batch_key, INPUT_BUCKET), queue="scraping_queue")

    # process_batch.apply_async((first_batch_key,), queue="scraping_queue")  

    return {"status": "Batch processing started", "first_batch_key": first_batch_key, "crawl_ids": response_data}

@app.post("/start-parse/{bucket_name}/{file_key}")
def start_parse(bucket_name: str, file_key: str):
    task = parse_task.apply_async(args=[bucket_name, file_key], queue="parsing_queue")
    return {"message": "Parse task started", "task_id": task.id}


@app.post("/retry-failed/")  
async def retry_failed_urls():
    failed_urls = []
    for key in redis_client.scan_iter("failed_*"):
        failed_data = redis_client.get(key)
        if failed_data:
            failed_info = json.loads(failed_data)
            url = failed_info["url"]
            crawl_id = key.split("_")[-1]  
            
            failed_urls.append(url)
            retry_crawl_id = crawl_id
            schema_type = failed_info.get("schema_type", "html")  
            redis_client.delete(key)  

            redis_client.set(f"crawl_{retry_crawl_id}", json.dumps({
                "url": url,
                "schema": {"type": schema_type}
            }))

            update_status(retry_crawl_id, STATUS_SCRAPING)

            process_batch.apply_async((retry_crawl_id,), queue="scraping_queue")  

    return {"status": "Retry initiated", "failed_urls": failed_urls}

@app.get("/status/{crawl_id}")
async def check_status(crawl_id: str):
    status_data = redis_client.get(f"status_{crawl_id}")
    if status_data:
        return json.loads(status_data)
    return {"status": "unknown", "message": "Crawl ID not found"}

@app.get("/download-zip")
def download_zip(
    bucket_name: str = Query("parsed-structured-data", description="S3 bucket name"),
    prefix: str = Query("Round-head6/", description="S3 prefix path"),
    zip_file_name: str = Query("Round-head6.zip", description="Name for the output zip file"),
):
    local_path = './S3_downloads/'
    zip_path = download_entire_parsed_data_as_zip(bucket_name, prefix, zip_file_name, local_path)
    return FileResponse(path=zip_path, filename=zip_file_name, media_type='application/zip')