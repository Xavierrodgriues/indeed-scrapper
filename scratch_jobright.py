import asyncio
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup

async def main():
    async with AsyncWebCrawler(verbose=True) as crawler:
        result = await crawler.arun(url="https://jobright.ai/jobs/search?value=DevOps+Engineer", bypass_cache=True)
        print(f"Success: {result.success}")
        if result.success:
            html = result.html
            soup = BeautifulSoup(html, "html.parser")
            cards = soup.select('a[class*="index_job-card__"]')
            print(f"Found {len(cards)} job cards.")
            
            for i, card in enumerate(cards[:2]):
                title_el = card.select_one("h2")
                company_el = card.select_one("div") # According to agent, inside card header
                href = card.get("href")
                print(f"[{i}] {title_el.text if title_el else 'No Title'} - {href}")

asyncio.run(main())
