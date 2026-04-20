"""Utility helpers for manifest handling, file I/O, and normalization."""

from __future__ import annotations

import csv
import json
import logging
import re
import tempfile
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from .models import JobListing

logger = logging.getLogger(__name__)


def ensure_data_directories(base_dir: Path) -> tuple[Path, Path]:
    """Create the jobs and manifests directories if they do not exist."""

    jobs_dir = base_dir / "jobs"
    manifests_dir = base_dir / "manifests"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir.mkdir(parents=True, exist_ok=True)
    return jobs_dir, manifests_dir


def manifest_path(base_dir: Path) -> Path:
    """Return the canonical manifest path."""

    return base_dir / "manifests" / "manifest.json"


def load_manifest(path: Path) -> dict[str, dict[str, Any]]:
    """Load the manifest file, returning an empty manifest when absent or invalid."""

    if not path.exists():
        logger.info("Manifest not found at %s; starting with an empty manifest.", path)
        return {"job_ids": {}}

    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to load manifest %s: %s", path, exc)
        return {"job_ids": {}}

    job_ids = payload.get("job_ids")
    if not isinstance(job_ids, dict):
        logger.warning("Manifest %s is missing the job_ids mapping; resetting.", path)
        return {"job_ids": {}}

    return {"job_ids": job_ids}


def save_manifest(path: Path, manifest: dict[str, dict[str, Any]]) -> None:
    """Persist the manifest atomically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_json_write(path, manifest)


def job_id_exists(manifest: dict[str, dict[str, Any]], job_id: str) -> bool:
    """Check whether a job id has already been recorded."""

    return job_id in manifest.get("job_ids", {})


def register_jobs(
    manifest: dict[str, dict[str, Any]],
    listings: list[JobListing],
    output_file: Path,
) -> dict[str, dict[str, Any]]:
    """Update the manifest with newly persisted job records."""

    job_ids = manifest.setdefault("job_ids", {})
    timestamp = datetime.now(UTC).isoformat()

    for listing in listings:
        job_ids[listing.job_id] = {
            "checksum": listing.checksum,
            "file": str(output_file),
            "updated_at": timestamp,
        }

    return manifest


def atomic_json_write(path: Path, payload: Any) -> None:
    """Write JSON atomically using a temp file in the same directory."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(path.parent),
            prefix=f".{path.stem}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            try:
                import os

                os.fsync(handle.fileno())
            except OSError as exc:
                logger.debug("fsync skipped for %s: %s", temp_path, exc)
        temp_path.replace(path)
    finally:
        if temp_path is not None and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                logger.debug("Temporary file %s could not be removed after write.", temp_path)


def atomic_csv_write(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    """Write CSV atomically using a temp file in the same directory."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=str(path.parent),
            prefix=f".{path.stem}.",
            suffix=".csv.tmp",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            try:
                import os

                os.fsync(handle.fileno())
            except OSError as exc:
                logger.debug("fsync skipped for %s: %s", temp_path, exc)
        temp_path.replace(path)
    finally:
        if temp_path is not None and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                logger.debug("Temporary file %s could not be removed after write.", temp_path)


def daily_output_path(
    jobs_dir: Path,
    scrape_date: datetime,
    keyword: str,
    location: str,
) -> Path:
    """Build a partitioned daily output path."""

    date_dir = jobs_dir / scrape_date.date().isoformat()
    date_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{slugify(keyword)}_{slugify(location)}.csv"
    return date_dir / file_name


def slugify(value: str) -> str:
    """Convert a string to a stable filesystem slug."""

    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or "unknown"


def build_checksum(description: str | None) -> str:
    """Compute a stable SHA-256 checksum from the description text."""

    digest = sha256((description or "").encode("utf-8")).hexdigest()
    return digest


def parse_posted_at(value: Any, reference: datetime | None = None) -> datetime:
    """Normalize Indeed relative timestamps into an aware datetime."""

    reference = reference or datetime.now(UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)

    if value is None:
        return reference

    text = str(value).strip().lower()
    if not text:
        return reference

    iso_candidate = text.replace("z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_candidate)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        pass

    if "just posted" in text or text == "today":
        return reference
    if "yesterday" in text:
        return reference - timedelta(days=1)

    patterns = [
        (r"(\d+)\+?\s+days?\s+ago", timedelta(days=1)),
        (r"(\d+)\+?\s+hours?\s+ago", timedelta(hours=1)),
        (r"(\d+)\+?\s+minutes?\s+ago", timedelta(minutes=1)),
    ]
    for pattern, unit in patterns:
        match = re.search(pattern, text)
        if match:
            amount = int(match.group(1))
            return reference - (unit * amount)

    logger.warning("Unrecognized posted_at value %r; using reference time.", value)
    return reference


def serialize_listing_payload(listing: JobListing) -> dict[str, Any]:
    """Convert a validated listing to a compact CSV record."""

    row = listing.model_dump(mode="json")
    metadata = row.pop("metadata", {})
    metadata_dict = metadata if isinstance(metadata, dict) else {}
    row["description"] = metadata_dict.pop("description", None)
    row["job_url"] = metadata_dict.pop("job_url", None)
    row["source_url"] = metadata_dict.pop("source_url", None)
    row["scraped_at"] = metadata_dict.pop("scraped_at", None)
    row["metadata_json"] = json.dumps(metadata_dict, ensure_ascii=False, separators=(",", ":"))
    return row


def csv_fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    """Build a small, stable CSV schema."""

    preferred = [
        "job_id",
        "title",
        "company",
        "location",
        "salary_raw",
        "posted_at",
        "checksum",
        "job_url",
        "description",
        "job_description",
        "source_url",
        "scraped_at",
        "metadata_json",
    ]
    fieldnames = [name for name in preferred if any(name in row for row in rows)]
    seen = set(fieldnames)
    for row in rows:
        for name in row.keys():
            if name not in seen:
                fieldnames.append(name)
                seen.add(name)
    return fieldnames


def build_listing(
    raw_item: dict[str, Any],
    source_url: str,
    scraped_at: datetime,
) -> JobListing:
    """Build a validated JobListing from the raw scraper payload."""

    metadata: dict[str, Any] = {}
    for key, value in raw_item.items():
        if key not in {"job_id", "title", "company", "location", "salary_raw", "posted_at"}:
            metadata[key] = value

    description = metadata.get("description")
    if isinstance(description, str):
        description_text = description
    elif description is None:
        description_text = ""
    else:
        description_text = str(description)

    metadata["source_url"] = source_url
    metadata["scraped_at"] = scraped_at.isoformat()

    return JobListing(
        job_id=str(raw_item.get("job_id", "")).strip(),
        title=str(raw_item.get("title", "")).strip(),
        company=str(raw_item.get("company", "")).strip(),
        location=str(raw_item.get("location", "")).strip(),
        salary_raw=_normalize_optional_string(raw_item.get("salary_raw")),
        posted_at=parse_posted_at(raw_item.get("posted_at"), scraped_at),
        job_description=raw_item.get("job_description"),
        metadata=metadata,
        checksum=build_checksum(description_text),
    )


def _normalize_optional_string(value: Any) -> str | None:
    """Return a clean optional string value."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def filter_new_listings(
    manifest: dict[str, dict[str, Any]],
    listings: list[JobListing],
) -> tuple[list[JobListing], int]:
    """Split listings into new and skipped based on the manifest."""

    new_listings: list[JobListing] = []
    skipped = 0
    for listing in listings:
        if job_id_exists(manifest, listing.job_id):
            skipped += 1
            continue
        new_listings.append(listing)
    return new_listings, skipped


def encode_indeed_url(keyword: str, location: str, start: int, domain: str = "www.indeed.com") -> str:
    """Build the Indeed search URL for a specific page offset."""

    return (
        f"https://{domain}/jobs?"
        f"q={quote_plus(keyword)}&l={quote_plus(location)}&start={start}&sort=date"
    )

