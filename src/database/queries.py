"""
Common database query helpers used across the application.

All functions accept an active SQLAlchemy Session and return plain Python
objects (dicts or dataclasses) so callers are not tied to ORM internals.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from src.database.models import Job, JobSkill, SkillTrend

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Job queries
# ---------------------------------------------------------------------------


def get_total_job_count(db: Session) -> int:
    """Return the total number of job rows in the database."""
    return db.query(func.count(Job.id)).scalar() or 0


def get_latest_scrape_date(db: Session) -> datetime | None:
    """Return the most recent scraped_at timestamp across all jobs."""
    return db.query(func.max(Job.scraped_at)).scalar()


def get_new_jobs_this_week(db: Session) -> int:
    """Return how many jobs were scraped in the last 7 days."""
    cutoff = datetime.utcnow() - timedelta(days=7)
    return (
        db.query(func.count(Job.id)).filter(Job.scraped_at >= cutoff).scalar() or 0
    )


def get_jobs_without_skills(db: Session) -> list[Job]:
    """
    Return all jobs that have no entries in job_skills yet.

    Used by the NLP pipeline to find unprocessed jobs.
    """
    processed_ids = db.query(JobSkill.job_id).distinct()
    return db.query(Job).filter(Job.id.notin_(processed_ids)).all()


def get_latest_jobs(db: Session, limit: int = 100) -> list[dict[str, Any]]:
    """Return the most recent *limit* jobs with their top-3 skills."""
    jobs = (
        db.query(Job)
        .order_by(Job.scraped_at.desc())
        .limit(limit)
        .all()
    )
    results = []
    for job in jobs:
        skills = [js.skill for js in job.skills[:3]]
        results.append(
            {
                "id": job.id,
                "title": job.title,
                "company": job.company,
                "location": job.location,
                "salary_min": job.salary_min,
                "salary_max": job.salary_max,
                "top_skills": skills,
                "posted_date": job.posted_date,
                "url": job.url,
            }
        )
    return results


# ---------------------------------------------------------------------------
# Skill trend queries
# ---------------------------------------------------------------------------


def get_top_skills(
    db: Session,
    weeks: int = 4,
    limit: int = 20,
    category: str | None = None,
) -> list[dict[str, Any]]:
    """
    Return the top skills by total count over the last *weeks* weeks.

    Optionally filtered by *category*.
    """
    cutoff: date = date.today() - timedelta(weeks=weeks)
    query = db.query(
        SkillTrend.skill,
        SkillTrend.category,
        func.sum(SkillTrend.count).label("total_count"),
        func.avg(SkillTrend.avg_salary_min).label("avg_salary_min"),
        func.avg(SkillTrend.avg_salary_max).label("avg_salary_max"),
    ).filter(SkillTrend.week_start >= cutoff)

    if category:
        query = query.filter(SkillTrend.category == category)

    rows = (
        query.group_by(SkillTrend.skill, SkillTrend.category)
        .order_by(func.sum(SkillTrend.count).desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "skill": r.skill,
            "category": r.category,
            "count": int(r.total_count or 0),
            "avg_salary_min": r.avg_salary_min,
            "avg_salary_max": r.avg_salary_max,
        }
        for r in rows
    ]


def get_skills_by_category(db: Session, weeks: int = 4) -> list[dict[str, Any]]:
    """Return skill counts grouped by category."""
    cutoff: date = date.today() - timedelta(weeks=weeks)
    rows = (
        db.query(
            SkillTrend.category,
            func.sum(SkillTrend.count).label("total_count"),
        )
        .filter(SkillTrend.week_start >= cutoff)
        .group_by(SkillTrend.category)
        .order_by(func.sum(SkillTrend.count).desc())
        .all()
    )
    return [{"category": r.category, "count": int(r.total_count or 0)} for r in rows]


def get_skills_salary_correlation(
    db: Session, limit: int = 30
) -> list[dict[str, Any]]:
    """Return skills ranked by their average max salary."""
    rows = (
        db.query(
            SkillTrend.skill,
            SkillTrend.category,
            func.avg(SkillTrend.avg_salary_max).label("avg_max"),
            func.sum(SkillTrend.count).label("total_count"),
        )
        .filter(SkillTrend.avg_salary_max.isnot(None))
        .group_by(SkillTrend.skill, SkillTrend.category)
        .order_by(func.avg(SkillTrend.avg_salary_max).desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "skill": r.skill,
            "category": r.category,
            "avg_salary_max": r.avg_max,
            "count": int(r.total_count or 0),
        }
        for r in rows
    ]


def get_skill_trend_over_time(
    db: Session, skill: str, weeks: int = 12
) -> list[dict[str, Any]]:
    """Return weekly count for a specific skill over the last *weeks* weeks."""
    cutoff: date = date.today() - timedelta(weeks=weeks)
    rows = (
        db.query(SkillTrend.week_start, SkillTrend.count)
        .filter(SkillTrend.skill == skill, SkillTrend.week_start >= cutoff)
        .order_by(SkillTrend.week_start)
        .all()
    )
    return [{"week_start": str(r.week_start), "count": r.count} for r in rows]


# ---------------------------------------------------------------------------
# Salary queries
# ---------------------------------------------------------------------------


def get_salary_by_title(db: Session, limit: int = 20) -> list[dict[str, Any]]:
    """Return average min/max salary grouped by normalised job title."""
    rows = (
        db.query(
            func.lower(Job.title).label("title"),
            func.avg(Job.salary_min).label("avg_min"),
            func.avg(Job.salary_max).label("avg_max"),
            func.count(Job.id).label("job_count"),
        )
        .filter(Job.salary_max.isnot(None))
        .group_by(func.lower(Job.title))
        .order_by(func.avg(Job.salary_max).desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "title": r.title,
            "avg_salary_min": r.avg_min,
            "avg_salary_max": r.avg_max,
            "job_count": int(r.job_count),
        }
        for r in rows
    ]


def get_salary_by_location(db: Session) -> list[dict[str, Any]]:
    """Return average salary grouped by UK location."""
    rows = (
        db.query(
            Job.location,
            func.avg(Job.salary_min).label("avg_min"),
            func.avg(Job.salary_max).label("avg_max"),
            func.count(Job.id).label("job_count"),
        )
        .filter(Job.salary_max.isnot(None), Job.location.isnot(None))
        .group_by(Job.location)
        .order_by(func.avg(Job.salary_max).desc())
        .limit(30)
        .all()
    )
    return [
        {
            "location": r.location,
            "avg_salary_min": r.avg_min,
            "avg_salary_max": r.avg_max,
            "job_count": int(r.job_count),
        }
        for r in rows
    ]


def get_salary_trend(db: Session, weeks: int = 12) -> list[dict[str, Any]]:
    """
    Return average salary per week over the last *weeks* weeks.

    Uses Python-side weekly bucketing (ISO Monday) so the query is portable
    across PostgreSQL and SQLite (used in tests).
    """
    cutoff: date = date.today() - timedelta(weeks=weeks)
    rows = (
        db.query(Job.scraped_at, Job.salary_min, Job.salary_max)
        .filter(Job.scraped_at >= cutoff, Job.salary_max.isnot(None))
        .all()
    )

    # Bucket by ISO week start (Monday)
    buckets: dict[date, dict[str, list[float]]] = {}
    for scraped_at, sal_min, sal_max in rows:
        week = _week_start_from_dt(scraped_at)
        if week not in buckets:
            buckets[week] = {"mins": [], "maxs": []}
        if sal_min is not None:
            buckets[week]["mins"].append(sal_min)
        if sal_max is not None:
            buckets[week]["maxs"].append(sal_max)

    result = []
    for week in sorted(buckets):
        data = buckets[week]
        result.append(
            {
                "week": str(week),
                "avg_salary_min": sum(data["mins"]) / len(data["mins"]) if data["mins"] else None,
                "avg_salary_max": sum(data["maxs"]) / len(data["maxs"]) if data["maxs"] else None,
            }
        )
    return result


def _week_start_from_dt(dt: datetime | date | None) -> date:
    """Return the ISO Monday for the week containing *dt*."""
    if dt is None:
        return date.today() - timedelta(days=date.today().weekday())
    if isinstance(dt, datetime):
        dt = dt.date()
    return dt - timedelta(days=dt.weekday())


# ---------------------------------------------------------------------------
# Company queries
# ---------------------------------------------------------------------------


def get_top_hiring_companies(
    db: Session, limit: int = 20
) -> list[dict[str, Any]]:
    """Return companies with the most jobs posted this calendar month."""
    start_of_month = date.today().replace(day=1)
    rows = (
        db.query(
            Job.company,
            func.count(Job.id).label("job_count"),
            func.avg(Job.salary_min).label("avg_min"),
            func.avg(Job.salary_max).label("avg_max"),
        )
        .filter(
            Job.scraped_at >= start_of_month,
            Job.company.isnot(None),
        )
        .group_by(Job.company)
        .order_by(func.count(Job.id).desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "company": r.company,
            "job_count": int(r.job_count),
            "avg_salary_min": r.avg_min,
            "avg_salary_max": r.avg_max,
        }
        for r in rows
    ]


def get_company_skills(
    db: Session, company_name: str, limit: int = 10
) -> list[dict[str, Any]]:
    """Return the most commonly requested skills for a given company."""
    rows = (
        db.query(
            JobSkill.skill,
            JobSkill.category,
            func.count(JobSkill.id).label("count"),
        )
        .join(Job, Job.id == JobSkill.job_id)
        .filter(func.lower(Job.company) == company_name.lower())
        .group_by(JobSkill.skill, JobSkill.category)
        .order_by(func.count(JobSkill.id).desc())
        .limit(limit)
        .all()
    )
    return [
        {"skill": r.skill, "category": r.category, "count": int(r.count)}
        for r in rows
    ]
