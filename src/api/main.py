"""
UK Job Market Intelligence API — FastAPI application entry point.

Startup:
    - Initialises the database schema (creates tables if missing).
    - Registers all API routers.
    - Adds CORS middleware so the Streamlit dashboard can reach the API.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routers import companies, salaries, skills
from src.database.connection import init_db
from src.database.queries import (
    get_latest_scrape_date,
    get_total_job_count,
)
from src.database.connection import get_db_context
from sqlalchemy import func
from src.database.models import JobSkill

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create database tables on startup."""
    logger.info("API starting up — initialising database schema…")
    init_db()
    logger.info("API ready.")
    yield


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    lifespan=lifespan,
    title="UK Job Market Intelligence API",
    version="1.0.0",
    description=(
        "Real-time insights into UK technology job market trends: "
        "skill demand, salary analysis, and hiring company data scraped from "
        "Reed.co.uk and Adzuna."
    ),
    docs_url="/docs",
    redoc_url="/redoc",
)

# ---------------------------------------------------------------------------
# CORS  — allow all origins so the Streamlit dashboard can connect
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(skills.router, prefix="/skills", tags=["Skills"])
app.include_router(salaries.router, prefix="/salaries", tags=["Salaries"])
app.include_router(companies.router, prefix="/companies", tags=["Companies"])


# ---------------------------------------------------------------------------
# Utility endpoints
# ---------------------------------------------------------------------------


@app.get("/health", tags=["Meta"])
def health_check() -> dict:
    """Returns API status and current UTC timestamp."""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/stats", tags=["Meta"])
def stats() -> dict:
    """Returns aggregate counts: total jobs, unique skills, latest scrape."""
    with get_db_context() as db:
        total_jobs = get_total_job_count(db)
        latest_scrape = get_latest_scrape_date(db)
        unique_skills = (
            db.query(func.count(func.distinct(JobSkill.skill))).scalar() or 0
        )

    return {
        "total_jobs": total_jobs,
        "unique_skills": unique_skills,
        "latest_scrape": latest_scrape.isoformat() if latest_scrape else None,
    }
