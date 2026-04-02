"""
Reed.co.uk job scraper.

Uses the Reed Jobs API (v1) with HTTP Basic Authentication.
Docs: https://www.reed.co.uk/developers/jobseeker
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from dotenv import load_dotenv

from src.scraper.base_scraper import BaseScraper

load_dotenv()

logger = logging.getLogger(__name__)

_REED_API_KEY: str = os.getenv("REED_API_KEY", "")
_BASE_URL = "https://www.reed.co.uk/api/1.0/search"
_RESULTS_PER_PAGE = 100

KEYWORDS = [
    "machine learning engineer",
    "data scientist",
    "mlops engineer",
    "data engineer",
    "python developer",
    "AI engineer",
]

LOCATIONS = ["London", "United Kingdom"]


class ReedScraper(BaseScraper):
    """
    Fetches job listings from the Reed.co.uk API.

    Authentication: HTTP Basic auth (API key as username, empty password).
    """

    source = "reed"

    def __init__(self, api_key: str = _REED_API_KEY) -> None:
        super().__init__()
        if not api_key:
            raise ValueError(
                "REED_API_KEY is not set. Add it to your .env file."
            )
        self._api_key = api_key

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_job(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Map a raw Reed API result dict to our internal schema."""
        posted_date: datetime | None = None
        date_str = raw.get("date")
        if date_str:
            for fmt in ("%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    posted_date = datetime.strptime(date_str, fmt)
                    break
                except (ValueError, TypeError):
                    continue

        return {
            "job_id": f"reed_{raw.get('jobId')}",
            "title": raw.get("jobTitle", ""),
            "company": raw.get("employerName"),
            "location": raw.get("locationName"),
            "salary_min": raw.get("minimumSalary"),
            "salary_max": raw.get("maximumSalary"),
            "salary_currency": "GBP",
            "description": raw.get("jobDescription", ""),
            "url": raw.get("jobUrl"),
            "source": "reed",
            "posted_date": posted_date,
        }

    # ------------------------------------------------------------------
    # BaseScraper interface
    # ------------------------------------------------------------------

    def fetch_jobs(
        self,
        keyword: str,
        location: str,
        num_pages: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Fetch up to *num_pages* pages of Reed job results.

        Args:
            keyword:   Search term
            location:  Location string (e.g. "London")
            num_pages: Pages to fetch (100 results per page)

        Returns:
            List of normalised job dicts.
        """
        all_jobs: list[dict[str, Any]] = []

        for page in range(num_pages):
            params = {
                "keywords": keyword,
                "locationName": location,
                "resultsToTake": _RESULTS_PER_PAGE,
                "resultsToSkip": page * _RESULTS_PER_PAGE,
            }
            self.logger.debug(
                "Reed page=%d keyword=%r location=%r", page + 1, keyword, location
            )
            try:
                response = self._get(
                    _BASE_URL,
                    auth=(self._api_key, ""),
                    params=params,
                )
                data = response.json()
                results: list[dict] = data.get("results", [])
                if not results:
                    self.logger.debug(
                        "No more results at page %d for keyword=%r", page + 1, keyword
                    )
                    break

                parsed = [self._parse_job(r) for r in results]
                all_jobs.extend(parsed)
                self.logger.info(
                    "Reed page %d/%d: %d results (keyword=%r location=%r)",
                    page + 1,
                    num_pages,
                    len(results),
                    keyword,
                    location,
                )

                # Stop early if we got fewer results than requested
                if len(results) < _RESULTS_PER_PAGE:
                    break

            except Exception as exc:
                self.logger.error(
                    "Reed API error on page %d keyword=%r: %s",
                    page + 1,
                    keyword,
                    exc,
                    exc_info=True,
                )
                break

        return all_jobs
