"""Command-line entry point for the autonomous Indeed scraping pipeline."""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pydantic import ValidationError

from src.models import JobListing
from src.scraper import IndeedScraper
from src.utils import (
    atomic_csv_write,
    build_listing,
    csv_fieldnames,
    daily_output_path,
    ensure_data_directories,
    filter_new_listings,
    load_manifest,
    manifest_path,
    register_jobs,
    save_manifest,
    serialize_listing_payload,
)


def configure_logging() -> None:
    """Configure application logging once at startup."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description="Run the Indeed scraping pipeline.")
    parser.add_argument("--keyword", required=True, help="Search keyword, for example 'Software Engineer'.")
    parser.add_argument("--location", required=True, help="Job location, for example 'Remote'.")
    parser.add_argument("--max-pages", type=int, default=3, help="Maximum number of result pages to scrape.")
    parser.add_argument("--domain", default="www.indeed.com", help="Indeed domain to scrape (e.g. www.indeed.com).")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Base data directory for jobs and manifests.",
    )
    return parser.parse_args()


def _build_validated_listings(
    raw_items: list[dict[str, Any]],
    source_url: str,
    scraped_at: datetime,
) -> list[JobListing]:
    """Validate and normalize raw scraped items."""

    valid_listings = []
    for index, raw_item in enumerate(raw_items, start=1):
        try:
            listing = build_listing(raw_item, source_url=source_url, scraped_at=scraped_at)
        except (ValidationError, ValueError, TypeError) as exc:
            logging.getLogger(__name__).warning("Skipping invalid item %d: %s", index, exc)
            continue
        valid_listings.append(listing)
    return valid_listings


async def run_pipeline(keyword: str, location: str, max_pages: int, data_dir: Path, domain: str) -> tuple[int, int, int]:
    """Run scrape -> validate -> dedupe -> persist."""

    logger = logging.getLogger(__name__)
    jobs_dir, _ = ensure_data_directories(data_dir)
    manifest_file = manifest_path(data_dir)
    manifest = load_manifest(manifest_file)
    scraper = IndeedScraper(keyword=keyword, location=location, max_pages=max_pages, domain=domain)

    scraped_at = datetime.now(UTC)
    raw_items = await scraper.scrape()
    total_found = len(raw_items)
    source_url = f"https://{domain}/"

    valid_listings = _build_validated_listings(raw_items, source_url=source_url, scraped_at=scraped_at)
    new_listings, skipped = filter_new_listings(manifest, valid_listings)

    output_file = daily_output_path(jobs_dir, scraped_at, keyword, location)
    if new_listings:
        serialized = [serialize_listing_payload(listing) for listing in new_listings]
        atomic_csv_write(output_file, serialized, csv_fieldnames(serialized))
        manifest = register_jobs(manifest, new_listings, output_file)
        logger.info("Saved %d new listings to %s", len(new_listings), output_file)
    else:
        logger.info("No new listings found for %s in %s.", keyword, location)

    save_manifest(manifest_file, manifest)

    invalid_count = len(raw_items) - len(valid_listings)
    return total_found, len(new_listings), skipped + invalid_count


def main() -> None:
    """Entry point for CLI execution."""

    configure_logging()
    load_dotenv()
    args = parse_args()

    total_found, new_count, skipped_count = asyncio.run(
        run_pipeline(
            keyword=args.keyword,
            location=args.location,
            max_pages=args.max_pages,
            data_dir=args.data_dir,
            domain=args.domain,
        )
    )
    print(f"Total found: {total_found} | New: {new_count} | Skipped: {skipped_count}")


if __name__ == "__main__":
    main()
