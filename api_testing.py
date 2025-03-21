from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uuid

app = FastAPI()

# Define data_schema model
class Schema(BaseModel):
    type: str
    columns: list = []

class RequestData(BaseModel):
    urls: list[str]  # Accept multiple URLs
    data_schema: Schema

@app.post("/process-data/")
async def process_data(request_data: RequestData):
    urls = request_data.urls
    schema = request_data.data_schema

    # Check if data_schema type is valid
    valid_data_schemas = ["table", "image", "html","pdf"]
    if schema.type not in valid_data_schemas:
        raise HTTPException(status_code=400, detail="Invalid data_schema type")

    results = []

    for url in urls:
        crawl_id = str(uuid.uuid4())  # Unique Crawl ID per URL

        # Simulate processing based on schema type
        if schema.type == "table":
            print(f"Processing table data for {url} with Crawl ID {crawl_id}")
        elif schema.type == "image":
            print(f"Processing image data for {url} with Crawl ID {crawl_id}")
        elif schema.type == "html":
            print(f"Processing HTML content for {url} with Crawl ID {crawl_id}")

        results.append({
            "url": url,
            "crawl_id": crawl_id,
            "status": "success"
        })

    return {
        "schema_selected": {
            "type": schema.type,
            "columns": schema.columns
        },
        "requests": results
    }
