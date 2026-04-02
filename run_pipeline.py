"""
UK Job Market Intelligence Engine - Main Pipeline Script.

Execution order:
    1. ReedScraper   - scrape jobs from Reed.co.uk
    2. AdzunaScraper - scrape jobs from Adzuna
    3. SkillExtractor - extract skills from all unprocessed job descriptions
    4. SkillExtractor.update_skill_trends - rebuild weekly aggregate table

A failure in one stage is logged and skipped; later stages still run.

Usage:
    python run_pipeline.py

Environment variables required (see .env.example):
    DATABASE_URL, REED_API_KEY, ADZUNA_APP_ID, ADZUNA_API_KEY
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging - write to both stdout and a rotating file
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", mode="a", encoding="utf-8"),
    ],
)
# Ensure stdout handler uses UTF-8 on Windows (avoids cp1252 errors)
for handler in logging.getLogger().handlers:
    if isinstance(handler, logging.StreamHandler) and hasattr(handler.stream, "reconfigure"):
        try:
            handler.stream.reconfigure(encoding="utf-8")
        except Exception:
            pass

logger = logging.getLogger("pipeline")


def _banner(msg: str) -> None:
    sep = "-" * 60
    logger.info(sep)
    logger.info(msg)
    logger.info(sep)


# ---------------------------------------------------------------------------
# Stage helpers
# ---------------------------------------------------------------------------


def stage_scrape_reed() -> int:
    """Run the Reed scraper and return the number of new jobs inserted."""
    from src.scraper.reed_scraper import KEYWORDS, LOCATIONS, ReedScraper

    logger.info("Stage 1/4 - Reed scraper starting...")
    scraper = ReedScraper()
    total = scraper.run(keywords=KEYWORDS, locations=LOCATIONS, num_pages=5)
    logger.info("Reed scraper complete - new jobs inserted: %d", total)
    return total


def stage_scrape_adzuna() -> int:
    """Run the Adzuna scraper and return the number of new jobs inserted."""
    from src.scraper.adzuna_scraper import KEYWORDS, LOCATIONS, AdzunaScraper

    logger.info("Stage 2/4 - Adzuna scraper starting...")
    scraper = AdzunaScraper()
    total = scraper.run(keywords=KEYWORDS, locations=LOCATIONS, num_pages=4)
    logger.info("Adzuna scraper complete - new jobs inserted: %d", total)
    return total


def stage_extract_skills() -> dict[str, int]:
    """Extract skills from all unprocessed jobs."""
    from src.nlp.skill_extractor import SkillExtractor

    logger.info("Stage 3/4 - NLP skill extraction starting...")
    extractor = SkillExtractor()
    result = extractor.process_new_jobs()
    logger.info(
        "Skill extraction complete - jobs_processed=%d skills_inserted=%d",
        result["jobs_processed"],
        result["skills_inserted"],
    )
    return result


def stage_update_trends() -> int:
    """Rebuild the skill_trends aggregation table."""
    from src.nlp.skill_extractor import SkillExtractor

    logger.info("Stage 4/4 - Updating skill trends...")
    extractor = SkillExtractor()
    upserted = extractor.update_skill_trends()
    logger.info("Skill trends updated - rows upserted: %d", upserted)
    return upserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    pipeline_start = datetime.utcnow()
    _banner(f"UK Job Market Intelligence Pipeline - {pipeline_start.isoformat()}")

    # Initialise DB schema before anything else
    from src.database.connection import init_db

    logger.info("Initialising database schema...")
    init_db()

    results: dict[str, int | str] = {
        "reed_new_jobs": 0,
        "adzuna_new_jobs": 0,
        "jobs_processed": 0,
        "skills_inserted": 0,
        "trends_upserted": 0,
    }
    errors: list[str] = []

    # --- Stage 1: Reed ---
    try:
        results["reed_new_jobs"] = stage_scrape_reed()
    except Exception as exc:
        msg = f"Reed scraper FAILED: {exc}"
        logger.error(msg, exc_info=True)
        errors.append(msg)

    # --- Stage 2: Adzuna ---
    try:
        results["adzuna_new_jobs"] = stage_scrape_adzuna()
    except Exception as exc:
        msg = f"Adzuna scraper FAILED: {exc}"
        logger.error(msg, exc_info=True)
        errors.append(msg)

    # --- Stage 3: NLP extraction ---
    try:
        nlp_result = stage_extract_skills()
        results["jobs_processed"] = nlp_result["jobs_processed"]
        results["skills_inserted"] = nlp_result["skills_inserted"]
    except Exception as exc:
        msg = f"Skill extraction FAILED: {exc}"
        logger.error(msg, exc_info=True)
        errors.append(msg)

    # --- Stage 4: Trends ---
    try:
        results["trends_upserted"] = stage_update_trends()
    except Exception as exc:
        msg = f"Trend update FAILED: {exc}"
        logger.error(msg, exc_info=True)
        errors.append(msg)

    # --- Summary ---
    pipeline_end = datetime.utcnow()
    elapsed = (pipeline_end - pipeline_start).total_seconds()

    _banner("Pipeline Summary")
    logger.info("  Start time        : %s", pipeline_start.isoformat())
    logger.info("  End time          : %s", pipeline_end.isoformat())
    logger.info("  Elapsed           : %.1f seconds", elapsed)
    logger.info("  Reed new jobs     : %s", results["reed_new_jobs"])
    logger.info("  Adzuna new jobs   : %s", results["adzuna_new_jobs"])
    logger.info("  Jobs processed    : %s", results["jobs_processed"])
    logger.info("  Skills inserted   : %s", results["skills_inserted"])
    logger.info("  Trends upserted   : %s", results["trends_upserted"])

    if errors:
        logger.warning("  Errors (%d):", len(errors))
        for err in errors:
            logger.warning("    - %s", err)
        sys.exit(1)
    else:
        logger.info("  Status            : SUCCESS")


if __name__ == "__main__":
    main()
