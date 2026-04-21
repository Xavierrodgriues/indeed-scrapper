"""Pydantic models for Indeed job listings."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class JobListing(BaseModel):
    """Validated job listing record for the data lake."""

    model_config = ConfigDict(extra="ignore")

    job_id: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    company: str = Field(..., min_length=1)
    location: str = Field(..., min_length=1)
    salary_raw: str | None = None
    posted_at: datetime
    job_description: str | None = None
    experience: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    checksum: str = Field(..., min_length=64, max_length=64)

    @field_validator("job_id", "title", "company", "location", "checksum")
    @classmethod
    def strip_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("value cannot be empty")
        return value
