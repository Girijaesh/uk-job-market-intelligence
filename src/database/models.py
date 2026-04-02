"""
SQLAlchemy ORM models for the UK Job Market Intelligence Engine.

Tables:
    - jobs: Raw job postings from Reed and Adzuna
    - job_skills: Skills extracted from each job description
    - skill_trends: Aggregated weekly skill demand and salary correlation data
"""

from datetime import datetime, date

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Text,
    DateTime,
    Date,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


class Job(Base):
    """Represents a single job posting scraped from an external source."""

    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(255), unique=True, nullable=False, index=True)
    title = Column(String(500), nullable=False)
    company = Column(String(500), nullable=True)
    location = Column(String(255), nullable=True)
    salary_min = Column(Float, nullable=True)
    salary_max = Column(Float, nullable=True)
    salary_currency = Column(String(10), nullable=False, default="GBP")
    description = Column(Text, nullable=True)
    url = Column(String(2000), nullable=True)
    source = Column(String(50), nullable=False)  # "reed" or "adzuna"
    posted_date = Column(DateTime, nullable=True)
    scraped_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Relationship: one job -> many skill entries
    skills = relationship(
        "JobSkill", back_populates="job", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Job id={self.id} title={self.title!r} source={self.source!r}>"


class JobSkill(Base):
    """Records a specific skill extracted from a job's description."""

    __tablename__ = "job_skills"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(
        Integer,
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    skill = Column(String(255), nullable=False)
    category = Column(String(100), nullable=False)  # e.g. language, framework, mlops

    # Prevent duplicate skill rows for the same job
    __table_args__ = (UniqueConstraint("job_id", "skill", name="uq_job_skill"),)

    job = relationship("Job", back_populates="skills")

    def __repr__(self) -> str:
        return f"<JobSkill job_id={self.job_id} skill={self.skill!r} category={self.category!r}>"


class SkillTrend(Base):
    """Weekly aggregated statistics for each skill."""

    __tablename__ = "skill_trends"

    id = Column(Integer, primary_key=True, autoincrement=True)
    skill = Column(String(255), nullable=False)
    category = Column(String(100), nullable=False)
    count = Column(Integer, nullable=False, default=0)
    week_start = Column(Date, nullable=False)
    avg_salary_min = Column(Float, nullable=True)
    avg_salary_max = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("skill", "week_start", name="uq_skill_week"),
    )

    def __repr__(self) -> str:
        return (
            f"<SkillTrend skill={self.skill!r} week={self.week_start} count={self.count}>"
        )
