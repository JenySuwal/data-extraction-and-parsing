# scrape_images.py

import os
from playwright.async_api import async_playwright

async def scrape_images(url: str, output_folder: str):
    print(f"Navigating to: {url}")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()
        await page.goto(url, timeout=60000)

        # Extract and download images
        images = await page.query_selector_all("img")
        image_urls = []
        image_folder = os.path.join(output_folder, "images")
        os.makedirs(image_folder, exist_ok=True)
        
        for image in images:
            src = await image.get_attribute("src")
            alt = await image.get_attribute("alt")
            if src:
                image_name = alt if alt else f"image_{len(image_urls)+1}"
                image_name = ''.join(c if c.isalnum() else '_' for c in image_name)
                if src.startswith("//"):
                    src = "https:" + src
                elif src.startswith("/"):
                    src = url.rstrip("/") + src
                image_urls.append((src, image_name))
        
        for img_url, img_name in image_urls:
            try:
                response = await page.request.get(img_url)
                if response and response.status == 200:
                    image_path = os.path.join(image_folder, f"{img_name}.jpg")
                    with open(image_path, 'wb') as img_file:
                        img_file.write(await response.body())
                else:
                    print(f"Failed to download image {img_url} (status: {response.status})")
            except Exception as e:
                print(f"Error downloading image {img_url}: {e}")

        await browser.close()
