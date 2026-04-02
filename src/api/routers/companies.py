"""
FastAPI router — /companies endpoints.

Provides hiring activity and per-company skill demand data.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.database.connection import get_db
from src.database.queries import get_company_skills, get_top_hiring_companies

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class CompanyHiring(BaseModel):
    company: str
    job_count: int
    avg_salary_min: Optional[float] = None
    avg_salary_max: Optional[float] = None


class CompanySkillItem(BaseModel):
    skill: str
    category: str
    count: int


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/hiring",
    response_model=list[CompanyHiring],
    summary="Top companies by jobs posted this month",
)
def top_hiring_companies(
    limit: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    """
    Return the top *limit* companies ordered by number of jobs posted in the
    current calendar month.
    """
    return get_top_hiring_companies(db, limit=limit)


@router.get(
    "/skills",
    response_model=list[CompanySkillItem],
    summary="Most requested skills for a given company",
)
def company_skills(
    company: str = Query(..., description="Exact or partial company name"),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    """
    Return the top 10 skills most frequently requested by *company*.

    Company name matching is case-insensitive.
    """
    if not company.strip():
        raise HTTPException(status_code=400, detail="company query parameter is required.")
    results = get_company_skills(db, company_name=company.strip(), limit=10)
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"No skill data found for company: {company!r}",
        )
    return results
