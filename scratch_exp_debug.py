import asyncio
from src.jobright import JobrightScraper

async def main():
    # Jobright Scraper will test fetching the LSEG Job ID
    j = JobrightScraper("DevOps Engineer", "United States")
    
    # Let's mock a fast single URL fetch
    from crawl4ai import AsyncWebCrawler
    from bs4 import BeautifulSoup
    import re
    import json
    from datetime import UTC, datetime
    from src.scraper import _extract_role_experience

    async with AsyncWebCrawler(verbose=True) as crawler:
        url = "https://jobright.ai/jobs/info/69e7b668f8fa2f3ec32a9315"
        detail_res = await crawler.arun(url=url, bypass_cache=True)
        if getattr(detail_res, "success", False):
            detail_soup = BeautifulSoup(getattr(detail_res, "html", ""), "html.parser")
            
            # Simulated fix logic exactly as in jobright.py
            clean_desc = "fake desc"
            full_dom_text = detail_soup.get_text(" ", strip=True)
            badge_match = re.search(r'(\d+)\+?\s+years\s+exp', full_dom_text, re.I)
            
            if badge_match:
                experience = f"{badge_match.group(1)} years"
            else:
                experience = _extract_role_experience(clean_desc, "DevOps Engineer")
            
            print(f"✅ EXTRACED EXPERIENCE: {experience}")

asyncio.run(main())
