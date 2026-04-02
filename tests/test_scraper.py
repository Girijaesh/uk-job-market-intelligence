"""
Tests for the job scrapers.

All HTTP calls are mocked so tests never hit real APIs.
"""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.scraper.adzuna_scraper import AdzunaScraper
from src.scraper.reed_scraper import ReedScraper


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_reed_api_response(n: int = 3) -> dict:
    """Build a fake Reed API /search response with *n* jobs."""
    return {
        "results": [
            {
                "jobId": 1000 + i,
                "jobTitle": f"ML Engineer {i}",
                "employerName": f"Acme Corp {i}",
                "locationName": "London",
                "minimumSalary": 50000.0,
                "maximumSalary": 80000.0,
                "jobDescription": "Python pytorch mlflow docker kubernetes",
                "jobUrl": f"https://www.reed.co.uk/jobs/{1000 + i}",
                "date": "01/04/2026",
            }
            for i in range(n)
        ]
    }


def _make_adzuna_api_response(n: int = 3) -> dict:
    """Build a fake Adzuna API response with *n* jobs."""
    return {
        "results": [
            {
                "id": f"az_{2000 + i}",
                "title": f"Data Scientist {i}",
                "company": {"display_name": f"DataCo {i}"},
                "location": {"display_name": "Manchester", "area": ["Manchester"]},
                "salary_min": 45000.0,
                "salary_max": 70000.0,
                "description": "python scikit-learn aws s3 sql",
                "redirect_url": f"https://www.adzuna.co.uk/jobs/{2000 + i}",
                "created": "2026-04-01T08:00:00Z",
            }
            for i in range(n)
        ]
    }


# ---------------------------------------------------------------------------
# Reed scraper tests
# ---------------------------------------------------------------------------


class TestReedScraper:
    def test_fetch_jobs_returns_list_of_dicts(self):
        """fetch_jobs should return a non-empty list of dicts."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_reed_api_response(3)
        mock_resp.raise_for_status.return_value = None

        with patch.object(ReedScraper, "_get", return_value=mock_resp):
            scraper = ReedScraper(api_key="fake-key")
            jobs = scraper.fetch_jobs("machine learning", "London", num_pages=1)

        assert isinstance(jobs, list)
        assert len(jobs) == 3

    def test_fetch_jobs_required_fields(self):
        """Every returned job dict must contain the required schema fields."""
        required = {"job_id", "title", "company", "location", "salary_min", "salary_max", "url"}
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_reed_api_response(2)
        mock_resp.raise_for_status.return_value = None

        with patch.object(ReedScraper, "_get", return_value=mock_resp):
            scraper = ReedScraper(api_key="fake-key")
            jobs = scraper.fetch_jobs("python", "London", num_pages=1)

        for job in jobs:
            for field in required:
                assert field in job, f"Missing field: {field}"

    def test_fetch_jobs_source_is_reed(self):
        """All jobs returned by ReedScraper must have source='reed'."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_reed_api_response(2)
        mock_resp.raise_for_status.return_value = None

        with patch.object(ReedScraper, "_get", return_value=mock_resp):
            scraper = ReedScraper(api_key="fake-key")
            jobs = scraper.fetch_jobs("python", "London", num_pages=1)

        assert all(j["source"] == "reed" for j in jobs)

    def test_duplicate_job_ids_not_inserted_twice(self, tmp_path):
        """
        Calling save_to_db twice with the same jobs should insert only once.
        Uses an in-memory SQLite database to avoid needing Postgres.
        """
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        from src.database.models import Base, Job

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        jobs = [
            {
                "job_id": "reed_9999",
                "title": "Test Engineer",
                "company": "TestCo",
                "location": "London",
                "salary_min": 50000.0,
                "salary_max": 80000.0,
                "salary_currency": "GBP",
                "description": "python docker",
                "url": "https://example.com/job/9999",
                "source": "reed",
                "posted_date": datetime(2026, 4, 1),
            }
        ]

        scraper = ReedScraper.__new__(ReedScraper)
        scraper.logger = MagicMock()

        # Patch get_db_context to use our in-memory SQLite session
        from contextlib import contextmanager

        @contextmanager
        def fake_db_context():
            db = Session()
            try:
                yield db
                db.commit()
            finally:
                db.close()

        with patch("src.scraper.base_scraper.get_db_context", fake_db_context):
            inserted_first = scraper.save_to_db(jobs)
            inserted_second = scraper.save_to_db(jobs)  # same jobs again

        assert inserted_first == 1
        assert inserted_second == 0  # duplicate — should be skipped

    def test_empty_results_returns_early(self):
        """If the API returns no results, fetch_jobs should stop early."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"results": []}
        mock_resp.raise_for_status.return_value = None

        with patch.object(ReedScraper, "_get", return_value=mock_resp):
            scraper = ReedScraper(api_key="fake-key")
            jobs = scraper.fetch_jobs("obscure role", "London", num_pages=5)

        assert jobs == []


# ---------------------------------------------------------------------------
# Adzuna scraper tests
# ---------------------------------------------------------------------------


class TestAdzunaScraper:
    def test_fetch_jobs_returns_list_of_dicts(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_adzuna_api_response(3)
        mock_resp.raise_for_status.return_value = None

        with patch.object(AdzunaScraper, "_get", return_value=mock_resp):
            scraper = AdzunaScraper(app_id="fake-id", api_key="fake-key")
            jobs = scraper.fetch_jobs("data scientist", "london", num_pages=1)

        assert isinstance(jobs, list)
        assert len(jobs) == 3

    def test_fetch_jobs_source_is_adzuna(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _make_adzuna_api_response(2)
        mock_resp.raise_for_status.return_value = None

        with patch.object(AdzunaScraper, "_get", return_value=mock_resp):
            scraper = AdzunaScraper(app_id="fake-id", api_key="fake-key")
            jobs = scraper.fetch_jobs("data scientist", "london", num_pages=1)

        assert all(j["source"] == "adzuna" for j in jobs)

    def test_missing_credentials_raises(self):
        with pytest.raises(ValueError, match="ADZUNA_APP_ID"):
            AdzunaScraper(app_id="", api_key="")
