"""
Abstract base class for all job-market scrapers.

Provides:
    - Abstract interface: fetch_jobs()
    - Shared DB persistence with duplicate-skipping: save_to_db()
    - Retry logic with exponential backoff
    - Rate limiting between API calls
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import Any

import requests
from requests import Response, Session as HTTPSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.database.connection import get_db_context
from src.database.models import Job

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_RETRIES = 3
_BACKOFF_FACTOR = 2          # seconds × 2^attempt
_RATE_LIMIT_SLEEP = 1.0      # seconds between API calls
_REQUEST_TIMEOUT = 30        # seconds


def _build_http_session() -> HTTPSession:
    """
    Build a requests Session with automatic retry on transient HTTP errors.

    Retries on:  429, 500, 502, 503, 504
    Back-off:    {backoff_factor} * (2 ** (retry - 1))
    """
    session = HTTPSession()
    retry = Retry(
        total=_MAX_RETRIES,
        backoff_factor=_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class BaseScraper(ABC):
    """
    Abstract base for Reed and Adzuna scrapers.

    Sub-classes must implement :meth:`fetch_jobs`.
    """

    source: str = "unknown"  # override in sub-class: "reed" | "adzuna"

    def __init__(self) -> None:
        self._http: HTTPSession = _build_http_session()
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_jobs(
        self,
        keyword: str,
        location: str,
        num_pages: int,
    ) -> list[dict[str, Any]]:
        """
        Fetch job listings from the external API.

        Args:
            keyword:   Search term, e.g. "machine learning engineer"
            location:  Location string, e.g. "London"
            num_pages: Number of result pages to fetch

        Returns:
            A list of job dicts ready to pass to :meth:`save_to_db`.
            Each dict must contain at minimum the key ``job_id``.
        """

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _get(self, url: str, **kwargs: Any) -> Response:
        """
        Perform a GET request with rate limiting.

        The built-in retry adapter handles transient failures. A 1-second
        sleep is applied *before* the request to respect API rate limits.
        """
        time.sleep(_RATE_LIMIT_SLEEP)
        self.logger.debug("GET %s  params=%s", url, kwargs.get("params"))
        response = self._http.get(url, timeout=_REQUEST_TIMEOUT, **kwargs)
        response.raise_for_status()
        return response

    def save_to_db(self, jobs: list[dict[str, Any]]) -> int:
        """
        Persist a list of job dicts to the database.

        Jobs whose ``job_id`` already exists in the database are silently
        skipped to avoid duplicates.

        Args:
            jobs: List of job dicts produced by :meth:`fetch_jobs`.

        Returns:
            The number of *new* rows inserted.
        """
        if not jobs:
            self.logger.info("No jobs to save.")
            return 0

        inserted = 0
        skipped = 0

        with get_db_context() as db:
            # Fetch existing job_ids in one query for efficiency
            existing_ids: set[str] = {
                row[0]
                for row in db.query(Job.job_id).all()
            }

            for job_data in jobs:
                job_id = str(job_data.get("job_id", ""))
                if not job_id:
                    self.logger.warning("Job dict missing job_id, skipping: %s", job_data)
                    continue

                if job_id in existing_ids:
                    skipped += 1
                    continue

                job = Job(
                    job_id=job_id,
                    title=job_data.get("title", ""),
                    company=job_data.get("company"),
                    location=job_data.get("location"),
                    salary_min=job_data.get("salary_min"),
                    salary_max=job_data.get("salary_max"),
                    salary_currency=job_data.get("salary_currency", "GBP"),
                    description=job_data.get("description"),
                    url=job_data.get("url"),
                    source=self.source,
                    posted_date=job_data.get("posted_date"),
                )
                db.add(job)
                existing_ids.add(job_id)  # guard against duplicates in the same batch
                inserted += 1

        self.logger.info(
            "save_to_db: inserted=%d skipped=%d source=%s",
            inserted,
            skipped,
            self.source,
        )
        return inserted

    def run(
        self,
        keywords: list[str],
        locations: list[str],
        num_pages: int,
    ) -> int:
        """
        Convenience method: fetch jobs for each keyword × location combination
        and persist them to the database.

        Returns:
            Total number of new rows inserted.
        """
        total_inserted = 0
        for location in locations:
            for keyword in keywords:
                self.logger.info(
                    "Scraping keyword=%r location=%r pages=%d",
                    keyword,
                    location,
                    num_pages,
                )
                try:
                    jobs = self.fetch_jobs(keyword, location, num_pages)
                    inserted = self.save_to_db(jobs)
                    total_inserted += inserted
                    self.logger.info(
                        "  -> fetched=%d new=%d", len(jobs), inserted
                    )
                except Exception as exc:
                    self.logger.error(
                        "Error scraping keyword=%r location=%r: %s",
                        keyword,
                        location,
                        exc,
                        exc_info=True,
                    )
        return total_inserted
