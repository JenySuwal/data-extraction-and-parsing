import json
import redis
import time
import os
from celery import Celery
import asyncio
from html_scraping import scrape_html
from pdf_scraping import scrape_pdfs
from image_scraping import scrape_images
from table_scraping import fetch_tables_html

# Initialize Redis
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# Initialize Celery
celery_app = Celery(
    "tasks",
    broker="redis://localhost:6379/1",
    backend="redis://localhost:6379/2"
)

# Scraping Task Functions
@celery_app.task
def scrape_tables_task(url, crawl_id):
    fetch_tables_html(url)
    return {"url": url, "status": "completed", "crawl_id": crawl_id}

@celery_app.task
def scrape_pdfs_task(url, output_folder, crawl_id):
    asyncio.run(scrape_pdfs(url, output_folder))
    return {"url": url, "status": "completed", "crawl_id": crawl_id}

@celery_app.task
def scrape_images_task(url, output_folder, crawl_id):
    asyncio.run(scrape_images(url, output_folder))
    return {"url": url, "status": "completed", "crawl_id": crawl_id}

@celery_app.task
def scrape_html_task(url, output_folder, crawl_id):
    asyncio.run(scrape_html(url, output_folder))
    return {"url": url, "status": "completed", "crawl_id": crawl_id}

# Batch Processing
@celery_app.task
def process_batch(batch_key, crawl_id, next_batch_key=None):
    """Process one batch from Redis and schedule the next batch after 30 minutes, ensuring each task is linked to a crawl_id."""
    
    batch_data = redis_client.get(batch_key)
    if not batch_data:
        return {"error": "Batch not found in Redis"}

    batch = json.loads(batch_data)
    results = []
    
    output_folder = f"output/{crawl_id}"  # Store results per crawl_id
    os.makedirs(output_folder, exist_ok=True)  # Ensure the folder exists

    for item in batch:
        url = item["url"]
        schema_type = item["schema"]["type"]

        # Call the appropriate scraping task while passing the crawl_id
        if schema_type == "table":
            result = scrape_tables_task.apply_async((url, crawl_id), task_id=crawl_id)
        elif schema_type == "pdf":
            result = scrape_pdfs_task.apply_async((url, output_folder, crawl_id), task_id=crawl_id)
        elif schema_type == "image":
            result = scrape_images_task.apply_async((url, output_folder, crawl_id), task_id=crawl_id)
        elif schema_type == "html":
            result = scrape_html_task.apply_async((url, output_folder, crawl_id), task_id=crawl_id)
        else:
            result = {"url": url, "error": "Invalid schema type"}

        results.append({"url": url, "crawl_id": crawl_id, "task_id": result.id if isinstance(result, object) else "N/A"})

    # Schedule the next batch after 30 minutes (if it exists)
    if next_batch_key:
        process_batch.apply_async((next_batch_key, crawl_id), countdown=30 * 60)  # Wait 30 mins

    return {"batch_processed": batch_key, "crawl_id": crawl_id, "results": results}
