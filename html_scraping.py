import os
from playwright.async_api import async_playwright

async def scrape_html(url: str, output_folder: str):
    print(f"Navigating to: {url}")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()
        await page.goto(url, timeout=60000)

        # Save HTML content
        html_path = os.path.join(output_folder, "index.html")
        with open(html_path, 'w', encoding='utf-8') as file:
            file.write(await page.content())

        await browser.close()
