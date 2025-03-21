from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
import uuid

app = FastAPI()

class Schema(BaseModel):
    type: str
    columns: list[str] = []

class ScrapeRequestItem(BaseModel):
    url: HttpUrl  
    scraping_schema: Schema  

class ScrapeRequest(BaseModel):
    requests: list[ScrapeRequestItem]  

scrape_tasks = {}

@app.post("/process_data/")
async def process_data(request: ScrapeRequest):
    crawl_details = []  

    for item in request.requests:
        crawl_id = str(uuid.uuid4())

        scrape_tasks[crawl_id] = {
            "url": item.url,
            "schema": item.scraping_schema.dict(),
            "status": "processing",
            "data": None  
        }

        scraped_data = {"message": f"Scraping initiated for {item.url} with schema {item.scraping_schema.type}"}

        scrape_tasks[crawl_id]["status"] = "completed"
        scrape_tasks[crawl_id]["data"] = scraped_data

        crawl_details.append({
            "crawl_id": crawl_id,
            "url": item.url,
            "schema_type": item.scraping_schema.type,
            "status": "processing"
        })

    return {
        "crawl_details": crawl_details,
        # "status": "processing",
        # "message": "Scraping started for multiple URLs with different schemas"
    }

@app.get("/crawl_status/{crawl_id}")
async def crawl_status(crawl_id: str):
    if crawl_id not in scrape_tasks:
        raise HTTPException(status_code=404, detail="Crawl ID not found")
    
    return scrape_tasks[crawl_id]
