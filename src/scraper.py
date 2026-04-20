"""Core Indeed scraping logic built on top of Crawl4AI."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig, JsonCssExtractionStrategy
from dotenv import load_dotenv

from .utils import encode_indeed_url

logger = logging.getLogger(__name__)


class IndeedScraper:
    """Asynchronous Indeed scraper with proxy-aware Crawl4AI crawling."""

    def __init__(
        self,
        keyword: str,
        location: str,
        max_pages: int = 3,
        results_per_page: int = 10,
        timeout_ms: int = 60_000,
        domain: str = "www.indeed.com",
    ) -> None:
        self.keyword = keyword.strip()
        self.location = location.strip()
        self.max_pages = max(1, max_pages)
        self.results_per_page = max(1, results_per_page)
        self.timeout_ms = max(10_000, timeout_ms)
        self.domain = domain
        self.session_id = f"indeed-{self.keyword}-{self.location}".replace(" ", "-").lower()

    def _build_proxy_url(self) -> str | None:
        """Construct a ScrapeOps proxy URL if an API key is configured."""

        load_dotenv()
        api_key = os.getenv("SCRAPEOPS_API_KEY")
        if not api_key:
            logger.info("SCRAPEOPS_API_KEY is not set; running without a proxy.")
            return None

        return f"http://scrapeops:{api_key}@residential-proxy.scrapeops.io:8181"

    def _build_browser_config(self) -> BrowserConfig:
        """Create a managed, stealthy browser configuration."""

        load_dotenv()
        user_data_dir = os.getenv("INDEED_USER_DATA_DIR")
        headless = os.getenv("INDEED_HEADLESS", "true").strip().lower() not in {"0", "false", "no"}
        return BrowserConfig(
            headless=headless,
            use_managed_browser=True,
            enable_stealth=True,
            use_persistent_context=bool(user_data_dir),
            user_data_dir=str(Path(user_data_dir).expanduser()) if user_data_dir else None,
            verbose=False,
        )

    def _build_extraction_strategy(self) -> JsonCssExtractionStrategy:
        """Create the CSS extraction schema for Indeed job cards."""

        schema = {
            "name": "IndeedJobListings",
            "baseSelector": ".job_seen_beacon",
            "fields": [
                {
                    "name": "job_id",
                    "selector": "a.jcs-JobTitle",
                    "type": "attribute",
                    "attribute": "data-jk",
                },
                {"name": "title", "selector": "a.jcs-JobTitle span[title]", "type": "text"},
                {"name": "company", "selector": "[data-testid='company-name']", "type": "text"},
                {"name": "location", "selector": "[data-testid='text-location']", "type": "text"},
                {"name": "salary_raw", "selector": "#salaryInfoAndJobType", "type": "text"},
                {"name": "posted_at", "selector": "span.date", "type": "text"},
                {"name": "description", "selector": ".job-snippet", "type": "text"},
                {
                    "name": "job_url",
                    "selector": "a.jcs-JobTitle",
                    "type": "attribute",
                    "attribute": "href",
                },
            ],
        }
        return JsonCssExtractionStrategy(schema)

    def _build_run_config(self) -> CrawlerRunConfig:
        """Create the per-request crawler configuration."""

        proxy_url = self._build_proxy_url()
        return CrawlerRunConfig(
            wait_for="css:.job_seen_beacon",
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=self._build_extraction_strategy(),
            page_timeout=self.timeout_ms,
            session_id=self.session_id,
            scan_full_page=False,
            proxy_config=proxy_url,
            verbose=False,
        )

    @staticmethod
    def _normalize_extracted_content(extracted_content: Any) -> list[dict[str, Any]]:
        """Normalize Crawl4AI extraction output into a list of dictionaries."""

        if extracted_content is None:
            return []

        if isinstance(extracted_content, list):
            return [item for item in extracted_content if isinstance(item, dict)]

        if isinstance(extracted_content, dict):
            if isinstance(extracted_content.get("items"), list):
                return [item for item in extracted_content["items"] if isinstance(item, dict)]
            return [extracted_content]

        if isinstance(extracted_content, str):
            try:
                parsed = json.loads(extracted_content)
            except json.JSONDecodeError as exc:
                logger.error("Failed to decode extracted content: %s", exc)
                return []

            if isinstance(parsed, list):
                return [item for item in parsed if isinstance(item, dict)]
            if isinstance(parsed, dict):
                if isinstance(parsed.get("items"), list):
                    return [item for item in parsed["items"] if isinstance(item, dict)]
                return [parsed]

        logger.warning("Unsupported extracted content type: %s", type(extracted_content).__name__)
        return []

    @staticmethod
    def _looks_valid(items: list[dict[str, Any]]) -> bool:
        """Check whether extracted records contain the core fields we need."""

        if not items:
            return False

        first = items[0]
        required = ("job_id", "title", "company", "location")
        return all(str(first.get(field, "")).strip() for field in required)

    @staticmethod
    def _extract_posted_at(card: Any) -> str | None:
        """Pull a relative timestamp from a card if one is visible."""

        candidates = [
            "span.date",
            "[data-testid='myJobsStateDate']",
            "[data-testid='job-posting-age']",
            "[aria-label*='ago']",
        ]
        for selector in candidates:
            element = card.select_one(selector)
            if element:
                text = element.get_text(" ", strip=True)
                if text:
                    return text

        card_text = card.get_text(" ", strip=True)
        for token in ("just posted", "today", "yesterday", "ago"):
            if token in card_text.lower():
                return card_text
        return None

    def _parse_cards_from_html(self, html: str, source_url: str, scraped_at: datetime) -> list[dict[str, Any]]:
        """Fallback parser that reads the raw HTML when structured extraction is incomplete."""

        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select("div.job_seen_beacon")
        parsed_items: list[dict[str, Any]] = []

        for card in cards:
            link = card.select_one("a.jcs-JobTitle")
            title_el = card.select_one("a.jcs-JobTitle span[title], span[title]")
            company_el = card.select_one("[data-testid='company-name'], .companyName")
            location_el = card.select_one("[data-testid='text-location'], [data-testid='inlineHeader-companyLocation'], .companyLocation")
            salary_el = card.select_one("#salaryInfoAndJobType, .salary-snippet")
            description_el = card.select_one(".job-snippet")

            job_id = ""
            if link is not None:
                job_id = str(link.get("data-jk") or "").strip()
                if not job_id:
                    href = str(link.get("href") or "")
                    if "jk=" in href:
                        job_id = href.split("jk=", 1)[1].split("&", 1)[0]

            title = ""
            if title_el is not None:
                title = str(title_el.get("title") or title_el.get_text(" ", strip=True)).strip()
            elif link is not None:
                title = link.get_text(" ", strip=True)

            company = company_el.get_text(" ", strip=True) if company_el is not None else ""
            location = location_el.get_text(" ", strip=True) if location_el is not None else ""
            salary_raw = salary_el.get_text(" ", strip=True) if salary_el is not None else None
            description = description_el.get_text(" ", strip=True) if description_el is not None else card.get_text(" ", strip=True)
            posted_at = self._extract_posted_at(card) or scraped_at.isoformat()

            if not any([job_id, title, company, location]):
                continue

            parsed_items.append(
                {
                    "job_id": job_id,
                    "title": title,
                    "company": company,
                    "location": location,
                    "salary_raw": salary_raw,
                    "posted_at": posted_at,
                    "description": description,
                    "job_url": link.get("href") if link is not None else None,
                    "source_url": source_url,
                    "scraped_at": scraped_at.isoformat(),
                }
            )

        logger.info("Fallback HTML parser extracted %d job cards.", len(parsed_items))
        return parsed_items

    async def scrape(self) -> list[dict[str, Any]]:
        """Scrape Indeed listings across paginated result pages."""

        browser_config = self._build_browser_config()
        run_config = self._build_run_config()
        all_items: list[dict[str, Any]] = []

        logger.info(
            "Starting Indeed scrape for keyword=%r location=%r max_pages=%d",
            self.keyword,
            self.location,
            self.max_pages,
        )

        async with AsyncWebCrawler(config=browser_config) as crawler:
            for page_number in range(self.max_pages):
                start = page_number * self.results_per_page
                url = encode_indeed_url(self.keyword, self.location, start, self.domain)
                logger.info("Scraping page %d at %s", page_number + 1, url)

                try:
                    result = await crawler.arun(url=url, config=run_config)
                except Exception as exc:
                    logger.exception("Crawl failed for %s: %s", url, exc)
                    continue

                if not getattr(result, "success", False):
                    logger.warning(
                        "Crawl returned unsuccessfully for %s: %s",
                        url,
                        getattr(result, "error_message", "unknown error"),
                    )
                    continue

                page_items = self._normalize_extracted_content(getattr(result, "extracted_content", None))
                fallback_items = self._parse_cards_from_html(getattr(result, "html", ""), url, datetime.now(UTC))

                if not self._looks_valid(page_items):
                    logger.info("Structured extraction was incomplete; falling back to HTML parsing.")
                    page_items = fallback_items
                elif fallback_items:
                    fallback_by_job_id = {
                        str(item.get("job_id", "")).strip(): item
                        for item in fallback_items
                        if str(item.get("job_id", "")).strip()
                    }
                    for item in page_items:
                        job_id = str(item.get("job_id", "")).strip()
                        fallback = fallback_by_job_id.get(job_id)
                        if not fallback:
                            continue
                        for key in ("description", "salary_raw", "posted_at", "job_url"):
                            current_value = item.get(key)
                            if current_value in (None, "", [], {}):
                                item[key] = fallback.get(key)
                logger.info("Extracted %d job cards from page %d", len(page_items), page_number + 1)
                all_items.extend(page_items)

                if not page_items:
                    logger.info("No more job cards detected; stopping pagination early.")
                    break

            logger.info("Starting to fetch full Job Descriptions for %d items", len(all_items))
            for i, item in enumerate(all_items, 1):
                job_id = item.get("job_id")
                if not job_id:
                    continue
                url = f"https://{self.domain}/viewjob?jk={job_id}"
                logger.info("Fetching JD %d/%d for %s", i, len(all_items), job_id)
                try:
                    await asyncio.sleep(1.5)  # Add a small delay to avoid rate limits
                    result = await crawler.arun(
                        url=url,
                        config=CrawlerRunConfig(
                            cache_mode=CacheMode.BYPASS,
                            page_timeout=self.timeout_ms,
                            session_id=self.session_id,
                            scan_full_page=False,
                        )
                    )
                    
                    if getattr(result, "success", False):
                        html = getattr(result, "html", "")
                        soup = BeautifulSoup(html, "html.parser")
                        jd_el = soup.select_one("#jobDescriptionText")
                        if jd_el:
                            item["job_description"] = jd_el.get_text("\n", strip=True)
                        else:
                            logger.warning("Could not find #jobDescriptionText for %s", job_id)
                    else:
                        logger.warning("Failed to fetch JD page for %s", job_id)
                except Exception as exc:
                    logger.warning("Exception fetching JD for %s: %s", job_id, exc)

        logger.info("Scrape complete. Total extracted items: %d", len(all_items))
        return all_items
