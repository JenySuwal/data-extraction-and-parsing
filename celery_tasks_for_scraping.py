from celery import Celery
import asyncio
import json
import redis
from urllib.parse import urlparse
from html_scraping import scrape_html
from pdf_scraping import scrape_pdfs
from image_scraping import scrape_images
from table_scraping import fetch_tables_html
from table_parsing import parse_task
  

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

celery_app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/2"
)
celery_app.conf.task_routes = {
    "tasks.scrape_tables_task": {"queue": "scraping_queue"},
    "tasks.scrape_pdfs_task": {"queue": "scraping_queue"},
    "tasks.scrape_images_task": {"queue": "scraping_queue"},
    "tasks.scrape_html_task": {"queue": "scraping_queue"},
    "tasks.process_batch": {"queue": "scraping_queue"},
    "tasks.parse_task": {"queue": "parsing_queue"},
}


@celery_app.task(soft_time_limit=300, time_limit=600)
def scrape_tables_task(url, crawl_id):
    print(f"Scraping table data from: {url}")  
    try:
        fetch_tables_html(url,crawl_id)  
        return {"url": url, "status": "completed", "crawl_id": crawl_id}
    except Exception as e:
        print(f"Error: {str(e)}") 
        return {"url": url, "status": "failed", "error": str(e), "crawl_id": crawl_id}


@celery_app.task
def scrape_pdfs_task(url, crawl_id):
    print(f"Scraping PDFs from: {url}")
    try:
        scrape_pdfs(url)
        return {"url": url, "status": "completed", "crawl_id": crawl_id}
    except Exception as e:
        return {"url": url, "status": "failed", "error": str(e), "crawl_id": crawl_id}

@celery_app.task
def scrape_images_task(url, crawl_id):
    print(f"Scraping images from: {url}")
    try:
        asyncio.run(scrape_images(url))  
        return {"url": url, "status": "completed", "crawl_id": crawl_id}
    except Exception as e:
        return {"url": url, "status": "failed", "error": str(e), "crawl_id": crawl_id}


@celery_app.task
def scrape_html_task(url, crawl_id, output_folder="/path/to/output"):
    print(f"Scraping HTML content from: {url}")
    try:
        scrape_html(url, output_folder)  
        return {"url": url, "status": "completed", "crawl_id": crawl_id}
    except Exception as e:
        return {"url": url, "status": "failed", "error": str(e), "crawl_id": crawl_id}


@celery_app.task(name="process_batch")
def process_batch(batch_key, bucket_name):
    print("Processing the batch data")
    batch_data = redis_client.get(batch_key)
    if not batch_data:
        print(f"Batch key {batch_key} not found in Redis.")
        return {"error": "Batch not found in Redis"}

    batch = json.loads(batch_data)
    results = []
    batch_output = []

    for item in batch:
        url = item.get("url")
        schema_type = item.get("schema", {}).get("type", "").strip().lower()
        crawl_id = item.get("crawl_id")

        if not url or not schema_type:
            results.append({"url": url, "error": "Missing URL or schema type"})
            continue

        try:
            if schema_type == "table":
                result = scrape_tables_task(url, crawl_id)
            elif schema_type == "pdf":
                result = scrape_pdfs_task(url, crawl_id)
            elif schema_type == "image":
                result = scrape_images_task(url, crawl_id)
            elif schema_type == "html":
                result = scrape_html_task(url, crawl_id)
            else:
                result = {"url": url, "error": "Invalid schema type"}

            batch_output.append(result)

        except Exception as e:
            redis_client.set(f"failed_{crawl_id}", json.dumps({
                "url": url,
                "error": str(e),
                "retry_count": 0,
                "retry": True
            }))
            results.append({"url": url, "error": str(e)})

    # After scraping all URLs in the batch, trigger parsing for each URL's file.
    for item in batch:
        url = item.get("url")
        crawl_id = item.get("crawl_id")
        domain = urlparse(url).netloc.replace('.', '_')
        file_key = f"{domain}/tables_{crawl_id}.html"
        start_parse(bucket_name, file_key)  # Trigger parse task for each file_key

    # After batch is complete, indicate it in Redis
    redis_client.set(f"batch_{batch_key}_completed", "true")
    # Handle Next Batch Scraping (Wait for 30 minutes before processing next batch)
    next_batch_key = redis_client.get(f"next_batch_{batch_key}")
    if next_batch_key:
        print(f"Waiting 30 minutes for next batch {next_batch_key}")
        process_batch.apply_async((next_batch_key, bucket_name))#, countdown=30 * 60

    return {"batch_processed": batch_key, "results": results}


def start_parse(bucket_name: str, file_key: str):
    task = parse_task.apply_async(args=[bucket_name, file_key], queue="parsing_queue")
    return {"message": "Parse task started", "task_id": task.id}
