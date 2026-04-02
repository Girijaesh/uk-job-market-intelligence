"""
NLP skill extraction pipeline.

For each unprocessed job description:
    1. Lowercase the text.
    2. Search for every skill in SKILLS_DICT using \\b word-boundary regexes
       (avoids false positives like "r" matching inside "docker").
    3. Persist matches to job_skills.
    4. Recompute skill_trends (weekly aggregate counts + salary data).
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

from src.database.connection import get_db_context
from src.database.models import Job, JobSkill, SkillTrend
from src.database.queries import get_jobs_without_skills
from src.nlp.skills_list import SKILLS_DICT, SKILL_TO_CATEGORY

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pre-compile skill regexes once at module load
# ---------------------------------------------------------------------------

# For each skill, compile a case-insensitive pattern with word boundaries.
# Skills containing special regex characters (e.g. "c++") are escaped first.
_SKILL_PATTERNS: dict[str, re.Pattern] = {}

for _skill in SKILL_TO_CATEGORY:
    _escaped = re.escape(_skill)
    # Use lookahead/lookbehind instead of \b for multi-word / special-char skills
    _SKILL_PATTERNS[_skill] = re.compile(
        rf"(?<!\w){_escaped}(?!\w)", re.IGNORECASE
    )


def _extract_skills_from_text(text: str) -> list[tuple[str, str]]:
    """
    Return a list of (skill, category) tuples found in *text*.

    Each skill appears at most once per call (deduplication).
    """
    if not text:
        return []

    lowered = text.lower()
    found: list[tuple[str, str]] = []
    seen: set[str] = set()

    for skill, pattern in _SKILL_PATTERNS.items():
        if skill not in seen and pattern.search(lowered):
            found.append((skill, SKILL_TO_CATEGORY[skill]))
            seen.add(skill)

    return found


def _week_start(dt: date | datetime | None) -> date:
    """Return the Monday of the ISO week containing *dt* (defaults to today)."""
    if dt is None:
        dt = date.today()
    if isinstance(dt, datetime):
        dt = dt.date()
    # ISO weekday: Monday=1 ... Sunday=7
    return dt - timedelta(days=dt.weekday())


class SkillExtractor:
    """
    Processes unprocessed jobs and populates job_skills + skill_trends tables.
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Main entry points
    # ------------------------------------------------------------------

    def process_new_jobs(self) -> dict[str, int]:
        """
        Extract skills for all jobs that have no job_skills rows yet.

        Returns:
            dict with keys "jobs_processed" and "skills_inserted".
        """
        with get_db_context() as db:
            unprocessed: list[Job] = get_jobs_without_skills(db)
            self.logger.info("Found %d unprocessed jobs.", len(unprocessed))

            jobs_processed = 0
            skills_inserted = 0

            for job in unprocessed:
                pairs = _extract_skills_from_text(job.description or "")
                for skill, category in pairs:
                    db.add(
                        JobSkill(job_id=job.id, skill=skill, category=category)
                    )
                    skills_inserted += 1
                jobs_processed += 1

            self.logger.info(
                "Processed %d jobs, inserted %d skill rows.",
                jobs_processed,
                skills_inserted,
            )

        return {"jobs_processed": jobs_processed, "skills_inserted": skills_inserted}

    def update_skill_trends(self) -> int:
        """
        Recompute the skill_trends table from scratch for all weeks.

        Groups job_skills by (skill, ISO week), counts occurrences, and
        calculates average salary for jobs that mention each skill.

        Returns:
            Number of skill_trends rows upserted.
        """
        self.logger.info("Recalculating skill_trends...")

        with get_db_context() as db:
            # Pull all job_skill rows with associated salary data
            rows = (
                db.query(
                    JobSkill.skill,
                    JobSkill.category,
                    Job.scraped_at,
                    Job.salary_min,
                    Job.salary_max,
                )
                .join(Job, Job.id == JobSkill.job_id)
                .all()
            )

            # Aggregate: (skill, week_start) -> {count, salary_mins, salary_maxs}
            agg: dict[tuple[str, date], dict[str, Any]] = defaultdict(
                lambda: {"category": "", "count": 0, "salary_mins": [], "salary_maxs": []}
            )

            for row in rows:
                key = (row.skill, _week_start(row.scraped_at))
                bucket = agg[key]
                bucket["category"] = row.category
                bucket["count"] += 1
                if row.salary_min is not None:
                    bucket["salary_mins"].append(row.salary_min)
                if row.salary_max is not None:
                    bucket["salary_maxs"].append(row.salary_max)

            # Upsert into skill_trends
            upserted = 0
            for (skill, week), data in agg.items():
                avg_min = (
                    sum(data["salary_mins"]) / len(data["salary_mins"])
                    if data["salary_mins"]
                    else None
                )
                avg_max = (
                    sum(data["salary_maxs"]) / len(data["salary_maxs"])
                    if data["salary_maxs"]
                    else None
                )

                existing = (
                    db.query(SkillTrend)
                    .filter(
                        SkillTrend.skill == skill,
                        SkillTrend.week_start == week,
                    )
                    .first()
                )
                if existing:
                    existing.count = data["count"]
                    existing.avg_salary_min = avg_min
                    existing.avg_salary_max = avg_max
                else:
                    db.add(
                        SkillTrend(
                            skill=skill,
                            category=data["category"],
                            count=data["count"],
                            week_start=week,
                            avg_salary_min=avg_min,
                            avg_salary_max=avg_max,
                        )
                    )
                upserted += 1

        self.logger.info("skill_trends upserted: %d rows.", upserted)
        return upserted

    def run(self) -> dict[str, int]:
        """Run process_new_jobs then update_skill_trends in sequence."""
        result = self.process_new_jobs()
        trends_upserted = self.update_skill_trends()
        result["trends_upserted"] = trends_upserted
        return result
