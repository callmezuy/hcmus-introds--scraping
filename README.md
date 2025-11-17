# arXiv Paper Scraper — Lab 01

Project: a resilient, resumable pipeline that collects arXiv metadata, downloads available LaTeX sources (versioned), and retrieves reference lists via Semantic Scholar.

**This README explains how to set up the environment (pip or conda), obtain the Kaggle snapshot, configure scraping rate and parallelism, and run the scraper.**

**Contents**
- Prerequisites
- Installation
- Kaggle snapshot (recommended)
- Configuration (quick reference)
- Usage examples (single paper, batch, test)
- Output layout and resumability
- Troubleshooting & tips

---

## Prerequisites

- Python: 3.8 — 3.11 recommended.
- Git (optional) if you cloned the repo.
- A Kaggle account and API token if you want to use the Kaggle dataset snapshot locally (recommended for Stage 1 performance).

## Installation

Using virtualenv / pip

```bash
# From the repo root
python -m venv .venv
# Activate (Git Bash / Linux / macOS):
source .venv/Scripts/activate
# On Windows PowerShell:
.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r src/requirements.txt
```

---

## Kaggle snapshot

For large runs Stage 1 (metadata discovery) should prefer a local snapshot of the arXiv metadata. This avoids heavy use of the arXiv API and dramatically speeds initial lookups.

Using the Kaggle CLI:

```bash
# Install Kaggle CLI if you haven't already
pip install kaggle

# Place your Kaggle API token at ~/.kaggle/kaggle.json

# Download and unzip the Cornell arXiv dataset into `dataset/`
mkdir -p dataset
kaggle datasets download -p dataset --unzip Cornell-University/arxiv
```

After downloading, set the path to the snapshot in the project config: edit `src/config.py` and set:

```py
# src/config.py
KAGGLE_DATASET_PATH = "dataset/arxiv-metadata-oai-snapshot.json"
```

---

## Configuration

Most runtime tuning is performed in `src/config.py`. Important variables:

- `KAGGLE_DATASET_PATH` — path to local arXiv snapshot JSON (string). If None or invalid, the code falls back to arXiv API calls.
- `CSV_PATH` — path to the student assignment CSV used to look up assigned arXiv ID ranges (default: `"../TCTA-DS1.csv"`).
- `ARXIV_API_DELAY` — seconds to wait between arXiv API calls (float). Increase to be conservative with the arXiv service.
- `SEMANTIC_SCHOLAR_DELAY` — minimum seconds between Semantic Scholar API calls (float). Default is 1.0 to respect rate limits.
- `SEMANTIC_SCHOLAR_WORKERS` — number of concurrent workers for Semantic Scholar stage (int).
- `MAX_WORKERS` — number of concurrent download/extract workers for Stage 2 (int).
- `MAX_RETRIES` and `RETRY_DELAY` — controls retry/backoff on transient errors.

Notes:
- Ensure the CSV file `TCTA-DS1.csv` appears in the repository root or update `CSV_PATH` in `src/config.py` to point to its actual location. Expected CSV header: `MSSV,start_month,start_id,end_month,end_id`.

Example configuration snippet (edit `src/config.py`):

```py
ARXIV_API_DELAY = 3.0
SEMANTIC_SCHOLAR_DELAY = 1.0
RETRY_DELAY = 2.0
MAX_RETRIES = 3
MAX_WORKERS = 16
SEMANTIC_SCHOLAR_WORKERS = 4
```

Notes:
- Reducing `MAX_WORKERS` lowers parallel downloads and disk activity.

---

## Usage

Run the scraper from the `src/` directory. Basic usage:

```bash
cd src
python scraper.py <STUDENT_ID> [MAX_PAPERS]
```

- `<STUDENT_ID>`: identifier used to place output under `data/<STUDENT_ID>/`.
- `MAX_PAPERS` (optional): limit the number of papers processed (useful for tests).

Single-paper run (fetch metadata if missing, then download and references):

```bash
python scraper.py <STUDENT_ID> -p <ARXIV_ID>
# Example: python scraper.py 23127040 -p 2103.00001
```

Advanced examples:

- Test-run 10 papers (quick):
  `python scraper.py 23127040 10`
- Full run (no limit):
  `python scraper.py 23127040`

---

## Outputs and layout

Root data directory (default): `data/` created under repository root. The scraper writes per-student folders:

- `data/<STUDENT_ID>/` — per-student outputs
  - `<paper-id>/metadata.json` — atomic JSON with metadata (includes `revised_dates` when available)
  - `<paper-id>/references.json` — atomic JSON with references; only arXiv-keyed references are included
  - `tex/<paper-id>v<version>/` — versioned source folders created for every downloaded source (contains `.tex` and `.bib` files copied from archives)

- `cache/` (project cache)
  - `<STUDENT_ID>_metadata.json` —
  - `<STUDENT_ID>_references.json` —
  - `<STUDENT_ID>_downloaded.json` — central index used to avoid re-downloading already-processed versions
  - `performance_report.json` — aggregated run/per-paper timings and disk/memory stats

Resumability and atomic writes:
- `metadata.json` and `references.json` are written atomically (tmp file -> fsync -> replace) so interrupted runs will not produce partial JSON.
- The scraper writes per-paper manifest and central cache entries only after successful processing of a version; this ensures subsequent runs will skip already-processed versions.

