"""
FastAPI router — /skills endpoints.

Provides trending skill data, category breakdowns, and salary correlation.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

from src.database.connection import get_db
from src.database.queries import (
    get_skills_by_category,
    get_skills_salary_correlation,
    get_top_skills,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class SkillItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    skill: str
    category: str
    count: int
    avg_salary_min: Optional[float] = None
    avg_salary_max: Optional[float] = None


class CategoryItem(BaseModel):
    category: str
    count: int


class SalaryCorrelationItem(BaseModel):
    skill: str
    category: str
    avg_salary_max: Optional[float] = None
    count: int


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/trending",
    response_model=list[SkillItem],
    summary="Top 20 trending skills",
)
def get_trending_skills(
    weeks: int = Query(default=4, ge=1, le=52, description="Look-back window in weeks"),
    category: Optional[str] = Query(default=None, description="Filter by skill category"),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    """
    Return the top 20 skills by total mention count over the last *weeks* weeks.

    Optionally filter by *category* (e.g. `language`, `ml_framework`, `mlops`).
    """
    results = get_top_skills(db, weeks=weeks, limit=20, category=category)
    if not results:
        return []
    return results


@router.get(
    "/by-category",
    response_model=list[CategoryItem],
    summary="Skill counts grouped by category",
)
def skills_by_category(
    weeks: int = Query(default=4, ge=1, le=52),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    """Return total skill mention counts grouped by technology category."""
    return get_skills_by_category(db, weeks=weeks)


@router.get(
    "/salary-correlation",
    response_model=list[SalaryCorrelationItem],
    summary="Skills ranked by average salary of jobs that mention them",
)
def salary_correlation(
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    """
    Return up to 30 skills ranked by the average maximum salary of jobs that
    mention each skill.
    """
    return get_skills_salary_correlation(db, limit=30)
