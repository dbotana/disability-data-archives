# Federal Disability Data Monitor

Detects, archives, and reports changes to federal disability-related datasets, pages, and APIs. **No server required** — GitHub Actions runs the daily check; data lives in this repo as CSV files; your computer only needs to be on when you open the dashboard.

> **DISCLAIMER:** This is a non-partisan public interest research tool. It records factual changes to publicly available federal government data. It does not advocate for or against any political position, administration, or policy. It records *what* changed, not *why* or whether any change is "good" or "bad."

---

## Contents

1. [What This Does](#what-this-does)
2. [How It Works (No Server Required)](#how-it-works-no-server-required)
3. [Setup: GitHub Actions (Recommended)](#setup-github-actions-recommended)
4. [Viewing the Dashboard](#viewing-the-dashboard)
5. [Running a Check Manually](#running-a-check-manually)
6. [Architecture](#architecture)
7. [Configuration](#configuration)
8. [Adding New URLs](#adding-new-urls)
9. [Change Severity Guide](#change-severity-guide)
10. [Dashboard Guide](#dashboard-guide)
11. [Alert Setup](#alert-setup)
12. [Wayback Machine Integration](#wayback-machine-integration)
13. [Historical Toolkit (1975–2000)](#historical-toolkit-19752000)

---

## What This Does

Monitors ~50+ federal URLs across CDC, Census Bureau, SSA, NIH, ACL, DOL (BLS, JAN, OFCCP), Department of Education, DOJ (ADA.gov), HHS, EEOC, CMS, Regulations.gov, and data.gov for:

- **Page removals** (404, 410, short maintenance pages)
- **Content changes** (SHA-256 hash comparison + text diffing)
- **Dataset row count drops** (>10% triggers CRITICAL alert)
- **Schema changes** (new/removed columns in JSON APIs)
- **Redirects** — intra-.gov (HIGH) vs. external (CRITICAL)
- **Disability keyword removals** — tracks 40+ terms (ADA, SSDI, Section 504, etc.)
- **Semantic shifts** — cosine similarity between before/after text embeddings

When changes are detected, the system:
- Archives raw HTML/JSON to disk (`archive/{agency}/{YYYY-MM-DD}/{target_id}/`)
- Stores structured change records in SQLite
- Generates unified text diffs (stored, displayed in dashboard)
- Submits CRITICAL changes to the Wayback Machine Save API
- Dispatches alerts (email, Slack, RSS feed)
- Updates the web dashboard at `http://localhost:8000`

---

## How It Works (No Server Required)

```
GitHub Actions (daily 8am UTC)
    │
    ├─► run.py --github
    │       ├─ crawls ~50+ federal URLs
    │       ├─ diffs against last snapshot in data/snapshots.csv
    │       ├─ classifies changes (CRITICAL / HIGH / MEDIUM / LOW)
    │       ├─ submits CRITICAL URLs to Wayback Machine
    │       └─ commits data/changes.csv + data/snapshots.csv back to repo
    │
    └─► data/ (CSV files committed to GitHub)
            ├─ changes.csv     — append-only change log
            ├─ snapshots.csv   — latest snapshot per target
            ├─ feed.xml        — RSS feed
            └─ run_summary.md  — last run summary (shown in Actions UI)

Your computer (on demand only)
    └─► docker compose up dashboard
            └─ FastAPI reads data/*.csv → http://localhost:8000
```

Your computer never needs to be on for monitoring to run. You only start Docker when you want to browse the dashboard.

---

## Setup: GitHub Actions (Recommended)

**One-time setup (~5 minutes):**

```bash
# 1. Fork or clone this repo to your GitHub account
#    (The Actions workflow is already included in .github/workflows/monitor.yml)

# 2. Push to GitHub
git remote add origin https://github.com/YOUR_USERNAME/disability-data-archives.git
git push -u origin main
```

**Enable Actions** (if not already enabled):
- Go to your repo on GitHub → **Actions** tab → click **"I understand my workflows, go ahead and enable them"**

**Set secrets** (optional but recommended):
- Go to **Settings → Secrets and variables → Actions → New repository secret**

| Secret | Purpose |
|--------|---------|
| `EMAIL_USER` | Gmail address for email alerts |
| `EMAIL_PASSWORD` | Gmail App Password (not your login password) |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL |
| `WAYBACK_ACCESS_KEY` | Internet Archive S3-like access key |
| `WAYBACK_SECRET_KEY` | Internet Archive S3-like secret key |

If you skip secrets, the monitor still runs and commits data — it just won't send email/Slack alerts.

**Trigger a test run:**
- Go to **Actions → Federal Disability Data Monitor → Run workflow → Run workflow**
- After ~5–10 minutes, check the **Actions** tab for results and the `data/` folder for committed CSV files.

**Automatic schedule:** GitHub runs the workflow daily at 8am UTC (no action needed from you).

---

## Viewing the Dashboard

**With Docker (recommended):**

```bash
# Requirements: Docker Desktop

# 1. Clone the repo (or git pull to get latest data)
git clone https://github.com/YOUR_USERNAME/disability-data-archives.git
cd disability-data-archives

# 2. Copy secrets file
cp .env.example .env
# Edit .env to set DASHBOARD_PASSWORD (optional — defaults to "changeme")

# 3. Start dashboard
docker compose up dashboard

# 4. Open browser
open http://localhost:8000
# Login: admin / changeme  (or whatever you set in .env)

# 5. Stop when done
docker compose down
```

**Without Docker (Python):**

```bash
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install torch==2.3.0+cpu --index-url https://download.pytorch.org/whl/cpu
pip install -r requirements.txt
python main.py --dashboard-only
# Open http://localhost:8000
```

---

## Running a Check Manually

**GitHub Actions (no computer needed):**
- Go to **Actions → Federal Disability Data Monitor → Run workflow**
- Choose frequency: `all` / `high` / `medium` / `low`

**Docker (local, commits to GitHub):**

```bash
# Requires GITHUB_TOKEN in .env with repo write access
docker compose run --rm monitor

# Check only high-frequency targets
MONITOR_FREQUENCY=high docker compose run --rm monitor

# Dry run — fetch and diff but do NOT commit to GitHub
docker compose run --rm monitor python run.py --dry-run
```

**Python (local):**

```bash
# Commit results to GitHub
python run.py --github

# Write to local data/ only (no GitHub commit)
python run.py

# Dry run
python run.py --dry-run
```

---

## Architecture

```
.github/workflows/monitor.yml   (GitHub Actions — daily trigger)
    │
    └─► run.py ─────────────────────────────────────────────────────┐
            │                                                        │
            ├─► crawler.py          async httpx fetcher             │
            │       └─ SHA-256 hashing, BeautifulSoup text extract  │
            ├─► diff_engine.py      severity classifier             │
            ├─► semantic_analyzer.py  MiniLM embeddings             │
            ├─► wayback.py          Wayback Machine Save API        │
            └─► github_storage.py   read/write data/*.csv ──────────┘
                    └─► GitHub Contents/Git Data API (atomic commit)

main.py --dashboard-only
    └─► dashboard.py  FastAPI + Jinja2  (reads data/*.csv, no DB needed)
```

**Key technical choices:**
- Fully async (`asyncio` + `httpx`)
- No local database — `data/changes.csv` and `data/snapshots.csv` are the store
- GitHub's Git Data API used for atomic multi-file commits (no race conditions)
- NLP model (~80MB) cached in GitHub Actions via `actions/cache`
- Dashboard reads CSV files directly — no SQLite required

---

## Configuration

All configuration lives in `config.yaml`. Environment variable references (`${VAR_NAME}`) are expanded from `.env` or the shell environment.

### Key sections

#### `system`
```yaml
system:
  log_level: "INFO"                  # DEBUG|INFO|WARNING|ERROR
  log_file: "logs/monitor.log"       # Log file path
  timezone: "America/New_York"

  storage_backend: "github"          # "github" (commit CSV) | "local" (write CSV only)
  github_repo: "${GITHUB_REPOSITORY}" # e.g. "jsmith/disability-data-archives"
  github_branch: "${GITHUB_REF_NAME:-main}"
  data_changes_path: "data/changes.csv"
  data_snapshots_path: "data/snapshots.csv"
```

#### `crawling`
```yaml
crawling:
  min_delay_seconds: 2.0             # Minimum delay between requests to same domain
  max_concurrent_requests: 5         # Global concurrency limit
  request_timeout_seconds: 30
  max_retries: 3                     # Tenacity retry attempts on network errors
```

#### `scheduling`
```yaml
scheduling:
  high_frequency_interval_hours: 6   # SSA, Regulations.gov, SODA APIs
  medium_frequency_interval_hours: 24 # HTML pages, dataset indexes
  low_frequency_interval_days: 7     # Deep diffs, Wayback comparisons
  daily_digest_hour: 8               # Hour to send daily email digest (24h clock)
```

#### `nlp`
```yaml
nlp:
  embedding_model: "all-MiniLM-L6-v2"        # ~80MB — always enabled
  zero_shot_model: "facebook/bart-large-mnli" # ~1.6GB — disabled by default
  zero_shot_enabled: false                    # Set true only with 4GB+ free RAM
  disability_keywords: [...]                  # 40+ terms tracked for removal
```

#### `dashboard`
```yaml
dashboard:
  host: "0.0.0.0"
  port: 8000
  auth_username: "${DASHBOARD_USER}"
  auth_password: "${DASHBOARD_PASSWORD}"
```

---

## Adding New URLs

Add a new entry to the `targets` list in `config.yaml`:

```yaml
targets:
  - id: "my-new-target"              # Unique slug (used in file paths + DB)
    url: "https://www.agency.gov/page"
    agency: "AgencyName"
    name: "Human-readable name"
    frequency: "medium"              # high (6h) | medium (24h) | low (weekly)
    type: "html"                     # html | json | csv | xml
    tags: ["disability", "data"]     # Arbitrary tags for filtering
    row_count_check: false           # true to monitor record counts
```

New targets are picked up automatically on the next run (no restart needed — `run.py` reads `config.yaml` fresh each time).

**Row count monitoring** (`row_count_check: true`) is useful for:
- JSON API endpoints that return arrays
- HTML pages with data tables (`<tr>` rows counted)
- CSV datasets (newlines counted)

---

## Change Severity Guide

| Severity | Triggers | Examples |
|----------|----------|---------|
| **CRITICAL** | HTTP 404/410/451 | Page removed entirely |
| | Redirect to non-.gov domain | ssa.gov → thirdparty.com |
| | Row count drop >10% | Dataset shrinks by 10%+ |
| | Short body (<1KB) with removal phrases | "page not found", "no longer available" |
| | Previously-404 page returns 200 | Court-ordered restoration |
| **HIGH** | >30% text changed | Major content rewrite |
| | Redirect to different .gov URL | Page moved within government |
| | Non-200/non-4xx HTTP status change | 200 → 503 |
| **MEDIUM** | 5–30% text changed | Moderate update |
| | ETag/Last-Modified changed with minor content | Background metadata update |
| | Row count increased | New data added |
| **LOW** | <5% text changed | Minor update |
| | Only HTTP metadata changed | Headers updated, content same |

---

## Dashboard Guide

Access at `http://localhost:8000` (HTTP Basic Auth required).

| Route | Description |
|-------|-------------|
| `/` | Summary stats: change counts by severity (last 24h), recent changes (last 7 days) |
| `/changes` | Paginated change log — filter by severity, agency, lookback period |
| `/url/{target_id}` | All changes for one URL, with latest snapshot metadata |
| `/diff/{change_id}` | Syntax-highlighted unified diff for one change event |
| `/trends` | Bar chart of changes per day by severity; bar chart by agency |
| `/export` | Download full change log as CSV |
| `/health` | Health check endpoint (no auth) — returns `{"status": "ok"}` |
| `/api/trends` | JSON data for trend charts (consumed by trends page) |

---

## Alert Setup

### Email

```yaml
alerts:
  email:
    enabled: true
    smtp_host: "smtp.gmail.com"
    smtp_port: 587
    smtp_user: "${EMAIL_USER}"
    smtp_password: "${EMAIL_PASSWORD}"
    from_address: "monitor@yourorg.org"
    to_addresses:
      - "team@yourorg.org"
    min_severity: "HIGH"   # Only send HIGH and CRITICAL
```

For Gmail, use an [App Password](https://myaccount.google.com/apppasswords) (not your main password).

### Slack

```yaml
alerts:
  slack:
    enabled: true
    webhook_url: "${SLACK_WEBHOOK_URL}"
    channel: "#disability-data-alerts"
    min_severity: "HIGH"
```

Create a webhook at your Slack workspace's App configuration page.

### RSS Feed

RSS is enabled by default, written to `data/feed.xml`. Subscribe with any RSS reader (Feedly, NetNewsWire, etc.) or host the file via a web server.

```yaml
alerts:
  rss:
    enabled: true
    output_path: "data/feed.xml"
    max_items: 200
```

### Daily Digest

A Markdown and HTML digest is saved daily to `data/digests/{YYYY-MM-DD}.*` regardless of email configuration. If email is configured, the digest is also emailed.

---

## Wayback Machine Integration

When a **CRITICAL** change is detected:
1. The current URL is automatically submitted to the Wayback Machine Save API.
2. The resulting archive URL is stored in the `wayback_submissions` table.
3. A daily background job verifies submissions via the CDX API.

```yaml
wayback:
  enabled: true
  submit_on_critical: true
  access_key: "${WAYBACK_ACCESS_KEY}"    # Optional: higher rate limits
  secret_key: "${WAYBACK_SECRET_KEY}"
```

Wayback Machine S3-API keys are optional. Without them, submissions are limited to ~10/min. Register at [archive.org](https://archive.org/account/signup).

The system also checks whether a URL was present in the **End of Term 2024 Web Archive** (October 2024 – January 20, 2025) to help identify pages removed after the administration transition.

---

## Historical Toolkit (1975–2000)

The original research toolkit for historical disability rights data is preserved in this repository alongside the monitoring system.

### Files
- [disability_data_retrieval.py](disability_data_retrieval.py) — Data retrieval from GovInfo, National Archives, Census Bureau
- [advanced_analysis.py](advanced_analysis.py) — Statistical analysis and text mining
- [visualizations.py](visualizations.py) — Chart and dashboard generation

### Usage
```bash
# Step 1: Retrieve historical data
python disability_data_retrieval.py

# Step 2: Analyze
python advanced_analysis.py

# Step 3: Visualize
python visualizations.py
```

Outputs are written to `disability_data/` and `visualizations/`. See the inline documentation in each file for customization options.

---

## Monitored Agencies

| Agency | What's Monitored |
|--------|-----------------|
| **SSA** | Press releases, open data portal, SSDI/SSI annual reports, disability benefits pages |
| **CDC / NCBDDD** | Disability and health data, BRFSS, NHIS, NCBDDD homepage, autism surveillance, SODA API |
| **Census Bureau** | Disability tables, ACS variable definitions, SIPP datasets, disability.census.gov |
| **NIH / NICHD** | NICHD disability research, NIH RePORTER grant portfolio |
| **ACL / NIDILRR** | ACL data and research, NCI datasets, program directory, NIDILRR homepage |
| **DOL / BLS / JAN** | ODEP statistics, BLS disability employment, Job Accommodation Network, OFCCP Section 503 |
| **Department of Education** | IDEA data center, OCR data, child count tables, OSEP homepage |
| **DOJ / ADA.gov** | ADA.gov homepage, resources, and topics index |
| **HHS** | Disability topic page, Section 504 civil rights |
| **Regulations.gov** | Open disability rulemakings, ADA rulemakings, Section 504 rulemakings |
| **EEOC** | Disability discrimination pages, ADA enforcement guidance |
| **CMS** | Medicaid LTSS, open data disability datasets |
| **White House** | Accessibility page |
| **data.gov** | Disability dataset catalog |

---

## File Structure

```
disability-data-archives/
│   # Monitoring system
├── config.yaml                  # All configuration (50+ target URLs)
├── run.py                       # Single-shot monitor run (used by Actions + Docker)
├── main.py                      # Dashboard-only server + legacy daemon entry point
├── github_storage.py            # Read/write data/*.csv via GitHub API
├── crawler.py                   # Async httpx fetcher + SHA-256 hashing
├── diff_engine.py               # Change detection + severity classification
├── semantic_analyzer.py         # sentence-transformers MiniLM + keyword tracking
├── reporter.py                  # Email/Slack/RSS + daily digest
├── wayback.py                   # Wayback Machine Save + CDX API
├── dashboard.py                 # FastAPI web dashboard (reads CSV)
├── templates/                   # Jinja2 HTML templates
│   ├── base.html
│   ├── index.html
│   ├── changes.html
│   ├── url_detail.html
│   ├── diff_view.html
│   └── trends.html
│
│   # GitHub Actions
├── .github/workflows/monitor.yml  # Daily + manual dispatch workflow
│
│   # Docker
├── Dockerfile
├── docker-compose.yml           # Two services: monitor (run-once) + dashboard
├── .env.example                 # Template for secrets
│
│   # Tests
├── pytest.ini
├── tests/
│   ├── conftest.py
│   ├── test_diff_engine.py      # ~20 tests
│   ├── test_storage.py          # ~10 tests
│   ├── test_semantic_analyzer.py # ~14 tests
│   └── test_crawler.py          # ~12 tests
│
│   # Historical toolkit (preserved)
├── disability_data_retrieval.py
├── advanced_analysis.py
├── visualizations.py
│
│   # Data (committed to GitHub by monitor; read by dashboard)
├── data/
│   ├── changes.csv              # Append-only change log
│   ├── snapshots.csv            # Latest snapshot per target
│   ├── feed.xml                 # RSS feed
│   ├── run_summary.md           # Last run summary
│   └── digests/                 # Daily digest markdown files
│
│   # Local-only runtime directories (git-ignored)
├── logs/
└── models/                      # NLP model cache (~80MB)
```

---

## Running Tests

```bash
# Install test dependencies
pip install torch==2.3.0+cpu --index-url https://download.pytorch.org/whl/cpu
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run with coverage report
pytest tests/ --cov=. --cov-report=term-missing
```

Expected output: 50+ tests passing across 4 modules.

---

## License & Ethics

- Accesses only **publicly available** government data.
- Does **not** circumvent authentication, CAPTCHAs, or rate limits.
- Does **not** store or expose personally identifiable information (PII).
- Archived content used strictly for non-commercial public interest research.
- Politically neutral detection logic: records facts, not interpretations.

This project is provided for academic, journalistic, and public interest research purposes under the principle of government transparency.
