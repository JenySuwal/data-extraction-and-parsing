
import os
from playwright.async_api import async_playwright

async def scrape_pdfs(url: str, output_folder: str):
    print(f"Navigating to: {url}")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()
        await page.goto(url, timeout=60000)

        # Extract and download PDFs
        pdf_folder = os.path.join(output_folder, "pdfs")
        os.makedirs(pdf_folder, exist_ok=True)
        pdf_links = await page.query_selector_all('a[href$=".pdf"]')
        for i, pdf_link in enumerate(pdf_links):
            pdf_url = await pdf_link.get_attribute('href')
            pdf_url = url.rstrip("/") + pdf_url if pdf_url.startswith("/") else pdf_url
            response = await page.request.get(pdf_url)
            if response and response.status == 200:
                with open(os.path.join(pdf_folder, f"file_{i+1}.pdf"), 'wb') as pdf_file:
                    pdf_file.write(await response.body())

        await browser.close()
