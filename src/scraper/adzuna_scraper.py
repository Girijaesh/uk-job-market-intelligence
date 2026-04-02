"""
Adzuna job scraper.

Uses the Adzuna Jobs API v1 for Great Britain.
Docs: https://developer.adzuna.com/
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

_ADZUNA_APP_ID: str = os.getenv("ADZUNA_APP_ID", "")
_ADZUNA_API_KEY: str = os.getenv("ADZUNA_API_KEY", "")
_BASE_URL = "https://api.adzuna.com/v1/api/jobs/gb/search/"
_RESULTS_PER_PAGE = 50

KEYWORDS = [
    "machine learning",
    "data scientist",
    "mlops",
    "data engineer",
    "python machine learning",
    "AI engineer",
]

LOCATIONS = ["london", "united kingdom"]


class AdzunaScraper(BaseScraper):
    """
    Fetches job listings from the Adzuna API for Great Britain.

    Authentication: app_id and app_key as query parameters.
    """

    source = "adzuna"

    def __init__(
        self,
        app_id: str = _ADZUNA_APP_ID,
        api_key: str = _ADZUNA_API_KEY,
    ) -> None:
        super().__init__()
        if not app_id or not api_key:
            raise ValueError(
                "ADZUNA_APP_ID and ADZUNA_API_KEY must both be set in your .env file."
            )
        self._app_id = app_id
        self._api_key = api_key

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_job(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Map a raw Adzuna API result dict to our internal schema."""
        posted_date: datetime | None = None
        date_str = raw.get("created")
        if date_str:
            for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    posted_date = datetime.strptime(date_str, fmt)
                    break
                except (ValueError, TypeError):
                    continue

        company = raw.get("company", {})
        company_name = company.get("display_name") if isinstance(company, dict) else None

        location = raw.get("location", {})
        location_name = None
        if isinstance(location, dict):
            display_parts = location.get("display_name")
            area = location.get("area", [])
            location_name = display_parts or (", ".join(area) if area else None)

        return {
            "job_id": f"adzuna_{raw.get('id')}",
            "title": raw.get("title", ""),
            "company": company_name,
            "location": location_name,
            "salary_min": raw.get("salary_min"),
            "salary_max": raw.get("salary_max"),
            "salary_currency": "GBP",
            "description": raw.get("description", ""),
            "url": raw.get("redirect_url"),
            "source": "adzuna",
            "posted_date": posted_date,
        }

    # ------------------------------------------------------------------
    # BaseScraper interface
    # ------------------------------------------------------------------

    def fetch_jobs(
        self,
        keyword: str,
        location: str,
        num_pages: int = 4,
    ) -> list[dict[str, Any]]:
        """
        Fetch up to *num_pages* pages of Adzuna job results.

        Args:
            keyword:   Search term
            location:  Location string (e.g. "london")
            num_pages: Number of pages to fetch (50 results per page)

        Returns:
            List of normalised job dicts.
        """
        all_jobs: list[dict[str, Any]] = []

        for page in range(1, num_pages + 1):
            url = f"{_BASE_URL}{page}"
            params: dict[str, Any] = {
                "app_id": self._app_id,
                "app_key": self._api_key,
                "results_per_page": _RESULTS_PER_PAGE,
                "what": keyword,
                "where": location,
                "content-type": "application/json",
            }
            self.logger.debug(
                "Adzuna page=%d keyword=%r location=%r", page, keyword, location
            )
            try:
                response = self._get(url, params=params)
                data = response.json()
                results: list[dict] = data.get("results", [])
                if not results:
                    self.logger.debug(
                        "No more results at page %d for keyword=%r", page, keyword
                    )
                    break

                parsed = [self._parse_job(r) for r in results]
                all_jobs.extend(parsed)
                self.logger.info(
                    "Adzuna page %d/%d: %d results (keyword=%r location=%r)",
                    page,
                    num_pages,
                    len(results),
                    keyword,
                    location,
                )

                if len(results) < _RESULTS_PER_PAGE:
                    break

            except Exception as exc:
                self.logger.error(
                    "Adzuna API error on page %d keyword=%r: %s",
                    page,
                    keyword,
                    exc,
                    exc_info=True,
                )
                break

        return all_jobs
