"""Indeed scraping pipeline package."""

from .models import JobListing
from .scraper import IndeedScraper

__all__ = ["JobListing", "IndeedScraper"]
