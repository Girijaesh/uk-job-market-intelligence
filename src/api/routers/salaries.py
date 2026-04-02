"""
FastAPI router — /salaries endpoints.

Provides salary analysis by job title, location, and over time.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.database.connection import get_db
from src.database.queries import (
    get_salary_by_location,
    get_salary_by_title,
    get_salary_trend,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class SalaryByTitle(BaseModel):
    title: str
    avg_salary_min: Optional[float] = None
    avg_salary_max: Optional[float] = None
    job_count: int


class SalaryByLocation(BaseModel):
    location: str
    avg_salary_min: Optional[float] = None
    avg_salary_max: Optional[float] = None
    job_count: int


class SalaryTrendItem(BaseModel):
    week: Optional[str] = None
    avg_salary_min: Optional[float] = None
    avg_salary_max: Optional[float] = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/by-title",
    response_model=list[SalaryByTitle],
    summary="Average salary by job title",
)
def salary_by_title(
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    """
    Return average min/max salary grouped by normalised job title.

    Titles are lowercased for grouping so "Data Scientist" and
    "data scientist" are treated as the same role.
    """
    return get_salary_by_title(db, limit=limit)


@router.get(
    "/by-location",
    response_model=list[SalaryByLocation],
    summary="Average salary by UK location",
)
def salary_by_location(
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    """Return average salary grouped by UK city/region."""
    return get_salary_by_location(db)


@router.get(
    "/trend",
    response_model=list[SalaryTrendItem],
    summary="Average salary trend over the past 12 weeks",
)
def salary_trend(
    weeks: int = Query(default=12, ge=1, le=52),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    """Return weekly average salary over the past *weeks* weeks."""
    return get_salary_trend(db, weeks=weeks)
