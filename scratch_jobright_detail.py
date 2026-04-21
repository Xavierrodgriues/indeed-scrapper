import asyncio
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup

async def main():
    async with AsyncWebCrawler(verbose=True) as crawler:
        url = "https://jobright.ai/jobs/info/69e7d3837820c036924d86d9"
        result = await crawler.arun(url=url, bypass_cache=True)
        if result.success:
            html = result.html
            soup = BeautifulSoup(html, "html.parser")
            
            # The title is usually an h1 or h2
            title = soup.find("h1")
            print("H1 Title:", title.get_text(strip=True) if title else None)
            
            # The description
            desc_div = soup.select_one('div[class*="index_jobDetailContent__"]')
            print("Desc length:", len(desc_div.get_text()) if desc_div else 0)
            
            # Company and Location?
            # Easiest way might be the <script type="application/ld+json"> which contains Structured Data!
            ld_json = soup.find("script", type="application/ld+json")
            if ld_json:
                print("Found Schema.org LD-JSON!")
                print("Sample:", ld_json.string[:200])

asyncio.run(main())
