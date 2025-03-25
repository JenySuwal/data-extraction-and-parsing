from celery_tasks_for_scraping import scrape_tables_task

if __name__ == "__main__":
    url = "https://example.com/table"
    crawl_id = "test-crawl-id-123"

    result = scrape_tables_task.delay(url, crawl_id)
    print(f"Task ID: {result.id}")
