# UK Job Market Intelligence Engine

[![Run Tests](https://github.com/Girijaesh/uk-job-market-intelligence/actions/workflows/tests.yml/badge.svg)](https://github.com/Girijaesh/uk-job-market-intelligence/actions/workflows/tests.yml)
[![Daily Scraper](https://github.com/Girijaesh/uk-job-market-intelligence/actions/workflows/daily_scrape.yml/badge.svg)](https://github.com/Girijaesh/uk-job-market-intelligence/actions/workflows/daily_scrape.yml)
![Python](https://img.shields.io/badge/python-3.11-blue?logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?logo=fastapi)
![Streamlit](https://img.shields.io/badge/Streamlit-1.33-FF4B4B?logo=streamlit)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-compose-2496ED?logo=docker)

---

## What This Project Does

As a Masters student in Robotics and AI at Queen Mary University of London, I built this project to answer a question I kept asking myself: *what skills do UK employers actually want from ML and data engineers, and what do they pay for them?*

The **UK Job Market Intelligence Engine** scrapes thousands of live job postings every day from Reed.co.uk and Adzuna, extracts technology skills from job descriptions using NLP, stores everything in a PostgreSQL database, and surfaces insights through a REST API and an interactive Streamlit dashboard. The result is a live, self-updating picture of the UK tech job market — which skills are trending, which command the highest salaries, and who is hiring.

This is a production-quality project with a real database, containerised services, automated daily scraping via GitHub Actions, a full test suite, and a working dashboard.

---

## Screenshots

### Top Skills This Week
Real-time horizontal bar chart showing the 20 most in-demand technology skills across 3,125 UK job postings. Each bar is colour-coded by category — **Python dominates with 200+ mentions**, followed by **AWS** (cloud), **Azure** (cloud), **LLM** (NLP/AI), and **Databricks** (data engineering). The sidebar lets you filter by date range, skill category, and location (All UK / London only).

![Top Skills This Week](https://github.com/Girijaesh/uk-job-market-intelligence/blob/main/Screenshot%202026-04-02%20100051.png?raw=true)

---

### Top 15 Hiring Companies This Month
Horizontal bar chart ranked by number of active job postings. **IT Career Switch** leads with 350+ postings, followed by specialist recruiters **Harnham** (260) and **ITOL Recruit** (160). Tech employers include **Accenture** (88 roles), **Faculty AI** (30 roles), and **Tenth Revolution Group** (77 roles). The blue intensity gradient maps directly to posting volume.

![Top 15 Hiring Companies]()

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         GitHub Actions                          │
│   daily_scrape.yml (08:00 UTC)   tests.yml (every push/PR)     │
└────────────────────────┬────────────────────────────────────────┘
                         │ runs
                         ▼
               ┌─────────────────┐
               │ run_pipeline.py │
               └──────┬──────────┘
          ┌───────────┤
          ▼           ▼
   ReedScraper   AdzunaScraper
   (Reed API)    (Adzuna API)
          │           │
          └─────┬─────┘
                ▼
         PostgreSQL 15
          (jobs table)
                │
                ▼
        SkillExtractor
        (NLP regex scan)
                │
                ▼
    job_skills + skill_trends
                │
       ┌────────┴────────┐
       ▼                 ▼
  FastAPI (8000)   Streamlit (8501)
  /skills          → fetches from API
  /salaries        → renders charts
  /companies
```

All services run in Docker containers connected via `jobmarket-network`.

---

## Running Locally

**Prerequisites:** Docker Desktop, Python 3.11, Git

```bash
# 1. Clone the repository
git clone https://github.com/Girijaesh/uk-job-market-intelligence.git
cd uk-job-market-intelligence

# 2. Copy and fill in your API keys
cp .env.example .env
# Edit .env — add your REED_API_KEY, ADZUNA_APP_ID, ADZUNA_API_KEY

# 3. Start all Docker services
docker-compose up -d

# 4. Install Python dependencies locally (for the pipeline script)
pip install -r requirements.txt
python -m spacy download en_core_web_sm

# 5. Run the full pipeline (scrape → NLP → trends)
python run_pipeline.py

# 6. Open the dashboard
open http://localhost:8501

# 7. Browse the API docs
open http://localhost:8000/docs

# 8. Browse the database (pgAdmin)
open http://localhost:5050  # login: admin@admin.com / admin
```

---

## API Endpoints

All endpoints return JSON. Interactive docs at `/docs`.

### Meta
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | API health check + timestamp |
| GET | `/stats` | Total jobs, unique skills, last scrape date |

### Skills
| Method | Path | Query Params | Description |
|--------|------|-------------|-------------|
| GET | `/skills/trending` | `weeks`, `category` | Top 20 skills by mention count |
| GET | `/skills/by-category` | `weeks` | Skill counts grouped by category |
| GET | `/skills/salary-correlation` | — | Skills ranked by average max salary |

**Example:**
```json
GET /skills/trending?weeks=4&category=ml_framework

[
  { "skill": "pytorch", "category": "ml_framework", "count": 342, "avg_salary_min": 55000, "avg_salary_max": 85000 },
  { "skill": "tensorflow", "category": "ml_framework", "count": 289, "avg_salary_min": 52000, "avg_salary_max": 82000 }
]
```

### Salaries
| Method | Path | Description |
|--------|------|-------------|
| GET | `/salaries/by-title` | Average salary by normalised job title |
| GET | `/salaries/by-location` | Average salary by UK city |
| GET | `/salaries/trend` | Weekly average salary over last 12 weeks |

### Companies
| Method | Path | Query Params | Description |
|--------|------|-------------|-------------|
| GET | `/companies/hiring` | `limit` | Top companies by jobs posted this month |
| GET | `/companies/skills` | `company` (required) | Top skills requested by a specific company |

---

## Running Tests

```bash
pytest tests/ -v
```

Tests use an in-memory SQLite database — no running Postgres needed.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| API | FastAPI + Uvicorn |
| Dashboard | Streamlit + Plotly |
| Database | PostgreSQL 15 + SQLAlchemy ORM |
| NLP | Regex skill extraction (spaCy-ready) |
| Scraping | requests + retry/backoff |
| Containerisation | Docker + docker-compose |
| CI/CD | GitHub Actions |
| Testing | pytest + FastAPI TestClient |

---

## What I Learned

Building this project taught me far more than any single module on my Masters course. A few honest reflections:

**Data is messier than you expect.** Reed and Adzuna return salary data inconsistently — some postings have both min and max, some have neither. Building a schema that handles nulls gracefully, and writing queries that don't collapse when half your salary columns are empty, forced me to think carefully about data quality from day one.

**Regex NLP has real limits.** Matching skills with `\b` word boundaries catches the obvious cases but misses things like "ML Ops" vs "MLOps" or "Sci-kit learn" vs "scikit-learn". A production system would benefit from fuzzy matching or a proper NLP model — but for a first version, the regex approach gave me 80% of the value with 10% of the complexity.

**Docker-compose is genuinely useful.** I'd used Docker before for packaging, but setting up a proper multi-service environment — Postgres, pgAdmin, FastAPI, Streamlit, all on the same network — made me understand why it's the default for local development on data projects.

**CI/CD pays off immediately.** Having GitHub Actions run tests on every push caught three bugs in the first week that I'd have otherwise only found when the scraper ran overnight and silently failed.

---

## Future Improvements

- [ ] Add full-text search endpoint (`/jobs/search?q=`) with PostgreSQL `tsvector`
- [ ] Fuzzy skill matching to catch spelling variants
- [ ] Company de-duplication (e.g. "Google" vs "Google DeepMind" vs "Google UK")
- [ ] Salary normalisation (convert daily/hourly rates to annual equivalents)
- [ ] Add LinkedIn scraper (via unofficial API or Playwright)
- [ ] Sentence-transformer embeddings for semantic job similarity
- [ ] Email/Slack alert when a new top skill breaks into the top 5
- [ ] Deployment to a cloud VM with a public URL

---

## License

MIT — see [LICENSE](LICENSE) for details.

---

*Built as a portfolio project by a Masters student in Robotics & AI, Queen Mary University of London, 2026.*
