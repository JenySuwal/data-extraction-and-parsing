from celery import Celery
import asyncio
import os
import json
import redis
from html_scraping import scrape_html
from pdf_scraping import scrape_pdfs
from image_scraping import scrape_images
from table_scraping import fetch_tables_html
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

celery_app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/2"
)

@celery_app.task(soft_time_limit=300, time_limit=600)
def scrape_tables_task(url, crawl_id):
    print(f"Scraping table data from: {url}")  
    try:
        fetch_tables_html(url)  
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
def scrape_html_task(url, crawl_id):
    print(f"Scraping HTML content from: {url}")
    try:
        scrape_html(url)
        return {"url": url, "status": "completed", "crawl_id": crawl_id}
    except Exception as e:
        return {"url": url, "status": "failed", "error": str(e), "crawl_id": crawl_id}

@celery_app.task
def process_batch(batch_key, crawl_id, next_batch_key=None):
    print(f"Processing batch with crawl_id: {crawl_id} and batch_key: {batch_key}")

    batch_data = redis_client.get(batch_key)
    if not batch_data:
        print(f"Error: Batch key {batch_key} not found in Redis.")
        return {"error": "Batch not found in Redis"}

    try:
        batch = json.loads(batch_data)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in Redis for batch {batch_key}.")
        return {"error": "Invalid JSON format in Redis"}

    results = []
    output_folder = f"output/{crawl_id}"
    os.makedirs(output_folder, exist_ok=True)

    for item in batch:
        url = item.get("url")
        # schema_type = item.get("schema", {}).get("type")
        schema_type = item.get("schema", {}).get("type", "").strip().lower()
        print(f"Schema type received: '{schema_type}' for URL: {url}")

        if not url or not schema_type:
            results.append({"url": url, "error": "Missing URL or schema type"})
            continue

        print(f"Submitting {schema_type} scraping task for {url}")
        try:
            if schema_type == "table":
                print(f"Calling scrape_tables_task for {url}")
                try:
                    result = scrape_tables_task(url, crawl_id)  
                except Exception as e:
                    print(f"Error calling scrape_tables_task for {url}: {str(e)}")
            
            elif schema_type == "pdf":
                result = scrape_pdfs_task.apply_async(args=[url, crawl_id])

            elif schema_type == "image":
                result = scrape_images_task.apply_async(args=[url, crawl_id])

            elif schema_type == "html":
                result = scrape_html_task.apply_async(args=[url, crawl_id])
                
            else:
                result = {"url": url, "error": "Invalid schema type"}

            if isinstance(result, object) and hasattr(result, "id"):
                results.append({"url": url, "crawl_id": crawl_id, "task_id": result.id})
            else:
                results.append({"url": url, "error": "Task submission failed"})
        except Exception as e:
            print(f"Error submitting task for {url}: {str(e)}")
            results.append({"url": url, "error": str(e)})

    
    if next_batch_key:
        print(f"Scheduling next batch {next_batch_key} in 30 minutes")
        process_batch.apply_async(args=[next_batch_key, crawl_id], countdown=30 * 60)

    return {"batch_processed": batch_key, "crawl_id": crawl_id, "results": results}
