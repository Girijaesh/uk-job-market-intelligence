"""
Tests for the FastAPI application.

Uses FastAPI's TestClient with an in-memory SQLite database so no real
Postgres instance is required during CI.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.api.main import app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def client():
    """
    Provide a TestClient backed by an in-memory SQLite database.

    Patches:
        - init_db: replaced with SQLite-backed init
        - get_db / get_db_context: replaced with SQLite sessions
    """
    from contextlib import contextmanager

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool

    from src.database.models import Base

    # StaticPool ensures all sessions share the same in-memory SQLite connection
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    TestSession = sessionmaker(bind=engine)

    def override_get_db():
        db = TestSession()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    @contextmanager
    def override_db_context():
        db = TestSession()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    # Override FastAPI dependency
    from src.database import connection as conn_module
    app.dependency_overrides[conn_module.get_db] = override_get_db

    with (
        patch("src.api.main.init_db", return_value=None),
        patch("src.database.connection.get_db_context", override_db_context),
        patch("src.api.main.get_db_context", override_db_context),
    ):
        with TestClient(app) as c:
            yield c

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------


class TestHealth:
    def test_health_returns_200(self, client: TestClient):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_ok_status(self, client: TestClient):
        data = client.get("/health").json()
        assert data["status"] == "ok"

    def test_health_contains_timestamp(self, client: TestClient):
        data = client.get("/health").json()
        assert "timestamp" in data
        assert isinstance(data["timestamp"], str)


# ---------------------------------------------------------------------------
# /stats
# ---------------------------------------------------------------------------


class TestStats:
    def test_stats_returns_200(self, client: TestClient):
        response = client.get("/stats")
        assert response.status_code == 200

    def test_stats_has_required_keys(self, client: TestClient):
        data = client.get("/stats").json()
        assert "total_jobs" in data
        assert "unique_skills" in data
        assert "latest_scrape" in data

    def test_stats_counts_are_integers(self, client: TestClient):
        data = client.get("/stats").json()
        assert isinstance(data["total_jobs"], int)
        assert isinstance(data["unique_skills"], int)


# ---------------------------------------------------------------------------
# /skills/trending
# ---------------------------------------------------------------------------


class TestSkillsTrending:
    def test_returns_200(self, client: TestClient):
        response = client.get("/skills/trending")
        assert response.status_code == 200

    def test_returns_list(self, client: TestClient):
        data = client.get("/skills/trending").json()
        assert isinstance(data, list)

    def test_weeks_param_accepted(self, client: TestClient):
        response = client.get("/skills/trending?weeks=8")
        assert response.status_code == 200

    def test_category_param_accepted(self, client: TestClient):
        response = client.get("/skills/trending?category=language")
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# /skills/by-category
# ---------------------------------------------------------------------------


class TestSkillsByCategory:
    def test_returns_200(self, client: TestClient):
        assert client.get("/skills/by-category").status_code == 200

    def test_returns_list(self, client: TestClient):
        assert isinstance(client.get("/skills/by-category").json(), list)


# ---------------------------------------------------------------------------
# /skills/salary-correlation
# ---------------------------------------------------------------------------


class TestSalaryCorrelation:
    def test_returns_200(self, client: TestClient):
        assert client.get("/skills/salary-correlation").status_code == 200


# ---------------------------------------------------------------------------
# /salaries/by-title
# ---------------------------------------------------------------------------


class TestSalaryByTitle:
    def test_returns_200(self, client: TestClient):
        assert client.get("/salaries/by-title").status_code == 200

    def test_returns_list(self, client: TestClient):
        assert isinstance(client.get("/salaries/by-title").json(), list)


# ---------------------------------------------------------------------------
# /salaries/by-location
# ---------------------------------------------------------------------------


class TestSalaryByLocation:
    def test_returns_200(self, client: TestClient):
        assert client.get("/salaries/by-location").status_code == 200


# ---------------------------------------------------------------------------
# /salaries/trend
# ---------------------------------------------------------------------------


class TestSalaryTrend:
    def test_returns_200(self, client: TestClient):
        assert client.get("/salaries/trend").status_code == 200


# ---------------------------------------------------------------------------
# /companies/hiring
# ---------------------------------------------------------------------------


class TestCompaniesHiring:
    def test_returns_200(self, client: TestClient):
        assert client.get("/companies/hiring").status_code == 200

    def test_returns_list(self, client: TestClient):
        assert isinstance(client.get("/companies/hiring").json(), list)


# ---------------------------------------------------------------------------
# /companies/skills
# ---------------------------------------------------------------------------


class TestCompanySkills:
    def test_missing_company_param_returns_422(self, client: TestClient):
        """company is a required query param — missing it should 422."""
        response = client.get("/companies/skills")
        assert response.status_code == 422

    def test_unknown_company_returns_404(self, client: TestClient):
        response = client.get("/companies/skills?company=nonexistent_company_xyz")
        assert response.status_code == 404
