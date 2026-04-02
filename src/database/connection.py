"""
Database connection management for the UK Job Market Intelligence Engine.

Provides:
    - A singleton SQLAlchemy engine + session factory
    - get_db(): FastAPI dependency that yields a database session
    - init_db(): Creates all tables if they do not already exist
"""

import logging
import os
from contextlib import contextmanager
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.database.models import Base

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Engine singleton
# ---------------------------------------------------------------------------

_DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    "postgresql://jobuser:password@localhost:5432/jobmarket",
)

_engine: Engine | None = None
_SessionLocal: sessionmaker | None = None


def _get_engine() -> Engine:
    """Return (or lazily create) the singleton SQLAlchemy engine."""
    global _engine
    if _engine is None:
        logger.info("Creating SQLAlchemy engine for %s", _DATABASE_URL.split("@")[-1])
        _engine = create_engine(
            _DATABASE_URL,
            pool_pre_ping=True,          # detect stale connections
            pool_size=5,
            max_overflow=10,
            echo=False,
        )
    return _engine


def _get_session_factory() -> sessionmaker:
    """Return (or lazily create) the session factory."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=_get_engine(),
        )
    return _SessionLocal


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def init_db() -> None:
    """
    Create all tables defined in models.py if they do not yet exist.

    Safe to call multiple times - SQLAlchemy uses CREATE TABLE IF NOT EXISTS
    semantics via `checkfirst=True`.
    """
    engine = _get_engine()
    logger.info("Initialising database schema...")
    Base.metadata.create_all(bind=engine, checkfirst=True)
    logger.info("Database schema ready.")


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency that yields a transactional database session.

    Usage::

        @app.get("/items")
        def read_items(db: Session = Depends(get_db)):
            ...
    """
    SessionLocal = _get_session_factory()
    db: Session = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@contextmanager
def get_db_context() -> Generator[Session, None, None]:
    """
    Context-manager variant for use outside FastAPI (e.g. scripts, tests).

    Usage::

        with get_db_context() as db:
            jobs = db.query(Job).all()
    """
    SessionLocal = _get_session_factory()
    db: Session = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
