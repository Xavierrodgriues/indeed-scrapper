"""Async MongoDB helpers for persisting scraped job listings."""

from __future__ import annotations

import logging
import os
from typing import Any

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Fields to persist in MongoDB (only what we need in the DB)
_DB_FIELDS = {"job_id", "title", "job_description", "experience", "scraped_at"}


def _get_collection():
    """Return the Motor async collection, connecting lazily."""
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
    except ImportError:
        raise RuntimeError("motor is not installed. Run: pip install motor")

    load_dotenv()
    uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB", "jobs_data")
    collection_name = os.getenv("MONGODB_COLLECTION", "indeed_job_details")

    if not uri:
        raise RuntimeError("MONGODB_URI is not set in your .env file.")

    client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=10_000)
    return client[db_name][collection_name]


async def upsert_jobs(serialized_listings: list[dict[str, Any]]) -> tuple[int, int]:
    """
    Upsert a batch of job listings into MongoDB.

    Only the fields defined in _DB_FIELDS are stored.
    Uses job_id as the unique key — existing records are updated, new ones inserted.

    Returns:
        (inserted_count, modified_count)
    """
    if not serialized_listings:
        return 0, 0

    collection = _get_collection()

    inserted = 0
    modified = 0

    for row in serialized_listings:
        job_id = row.get("job_id")
        if not job_id:
            continue

        doc = {k: row.get(k) for k in _DB_FIELDS}
        doc["job_id"] = job_id  # ensure it's always present

        result = await collection.update_one(
            {"job_id": job_id},
            {"$set": doc},
            upsert=True,
        )

        if result.upserted_id:
            inserted += 1
        elif result.modified_count:
            modified += 1

    return inserted, modified
