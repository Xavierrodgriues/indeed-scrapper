import asyncio
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup

async def main():
    async with AsyncWebCrawler(verbose=True) as crawler:
        result = await crawler.arun(url="https://jobright.ai/jobs/search?value=DevOps+Engineer", bypass_cache=True)
        if result.success:
            html = result.html
            soup = BeautifulSoup(html, "html.parser")
            cards = soup.select('a[class*="index_job-card__"]')
            
            for i, card in enumerate(cards[:2]):
                title = card.select_one("h2")
                company_divs = card.find_all("div", class_=lambda x: x and "index_company-name__" in x)
                loc_divs = card.find_all("div", class_=lambda x: x and "index_location__" in x)
                
                # Try generic extraction if classes are minified or different
                divs = card.find_all("div")
                company = divs[1].get_text(strip=True) if len(divs) > 1 else 'Unknown'
                loc = divs[2].get_text(strip=True) if len(divs) > 2 else 'Unknown'
                
                href = card.get("href")
                
                print(f"--- Card {i} ---")
                print(f"Title: {title.get_text(strip=True) if title else None}")
                print(f"Company divs length: {len(company_divs)}")
                print(f"Href: {href}")
                print(f"Generic Div 1 (Company?): {company}")
                print(f"Generic Div 2 (Location?): {loc}")

asyncio.run(main())
