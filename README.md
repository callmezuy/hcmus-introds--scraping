# arXiv Paper Scraper - Lab 01

## Overview

This project implements a comprehensive arXiv paper scraper that collects full-text LaTeX sources, metadata, and reference information for assigned papers. The scraper follows an optimized pipeline design that efficiently retrieves data using multiple sources (arXiv API, Semantic Scholar API, and Kaggle dataset).

## Requirements

- Python 3.8 or higher
- Internet connection for API access
- Kaggle arXiv metadata dataset

## Installation

### 1. Clone or extract the project

Extract the submitted ZIP file to your desired location.

### 2. Set up Python environment

It is recommended to use a virtual environment:

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows (Command Prompt):
.venv\Scripts\activate

# On Windows (Git Bash/MINGW):
source .venv/Scripts/activate

# On Linux/Mac:
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

This will install:
- `arxiv`: Python wrapper for arXiv API
- `requests`: HTTP library for Semantic Scholar API
- `psutil`: Process and system utilities for performance monitoring
- `kaggle`: Kaggle API client for downloading datasets

### 4. Download Kaggle dataset (Required for Stage 1.3)

The Kaggle arXiv metadata dataset is **required** for Stage 1.3 to fetch submission dates for cited papers.

#### Option 1: Using Kaggle CLI (Recommended)

1. **Setup Kaggle API credentials:**
   - Go to https://www.kaggle.com/settings/account
   - Scroll to "API" section
   - Click "Create New Token" → downloads `kaggle.json`
   - Move the file to:
     - Windows: `C:\Users\<username>\.kaggle\kaggle.json`
     - Linux/Mac: `~/.kaggle/kaggle.json`
   - Set permissions (Linux/Mac): `chmod 600 ~/.kaggle/kaggle.json`

2. **Download the dataset:**
   ```bash
   # From project root directory
   kaggle datasets download -d Cornell-University/arxiv --unzip
   ```

   This downloads `arxiv-metadata-oai-snapshot.json` (~4.7GB) to the project root.

#### Option 2: Manual Download

1. Go to: https://www.kaggle.com/datasets/Cornell-University/arxiv
2. Click "Download" (requires Kaggle account)
3. Extract `arxiv-metadata-oai-snapshot.json`
4. Place it in the project root directory

**Note:** Without this file, Stage 1.3 will be skipped and `references.json` will be missing the required `submission_date` field, causing the submission to be incomplete.

## Project Structure

```
.
├── src/
│   ├── config.py                    # Configuration and settings
│   ├── logger.py                    # Logging utilities
│   ├── performance.py               # Performance monitoring
│   ├── arxiv_client.py              # arXiv API client
│   ├── semantic_scholar_client.py   # Semantic Scholar API client
│   ├── kaggle_handler.py            # Kaggle dataset handler
│   ├── file_processor.py            # File extraction and processing
│   └── scraper.py                   # Main scraper orchestrator
├── requirements.txt                 # Python dependencies
├── README.md                        # This file
├── Report.docx                      # Detailed implementation report
├── TCTA-DS1.csv                     # Student assignment file
└── arxiv-metadata-oai-snapshot.json # Kaggle dataset (download separately)
```

## Usage

### Basic Usage

Run the scraper with your student ID:

```bash
cd src
python scraper.py <STUDENT_ID> [MAX_PAPERS]
```

Examples:
```bash
# Process all assigned papers
python scraper.py 23127XXX

# Test mode: Process only 3 papers
python scraper.py 23127XXX 3
```

### What the Scraper Does

The scraper executes an **optimized parallel pipeline** to maximize efficiency:

#### Parallel Execution Model

The scraper runs multiple stages **simultaneously** using threading:

```
┌─────────────────────────────────────────────────────────────┐
│                    PARALLEL STAGES                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Stage 1.1 (Metadata) ──────────────────────────► Cache    │
│                                                             │
│  Stage 2 (Download) ─────────────────────────────► Data    │
│                                                             │
│  Stage 1.2 (Citations) ──┬──────────────────────► Cache    │
│                          │                                  │
│                          └──► Stage 1.3 (Cited) ──► Cache  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Stage Details:**

**1. Stage 1.1 - Metadata Collection** (Parallel Thread 1)
   - Fetches metadata for assigned papers using arXiv API
   - Retrieves: title, authors, abstract, submission dates, revised dates, versions
   - Saves to cache: `<STUDENT_ID>_metadata.json`
   - Rate limit: 3 seconds between API calls

**2. Stage 2 - Download and Processing** (Parallel Thread 2)
   - Runs **independently** and **simultaneously** with other stages
   - Downloads `.tar.gz` source files for all paper versions
   - Extracts and validates TeX source files
   - Identifies and copies `.tex` and `.bib` files
   - Removes figure files to reduce storage
   - Organizes files into version-specific directories
   - Creates `metadata.json` and `references.json` for each paper
   - Uses multi-threading (8 workers by default) for parallel downloads

**3. Stage 1.2 - Citation Retrieval** (Parallel Thread 3)
   - Gets citation lists using Semantic Scholar API
   - Collects references for each paper
   - Extracts arXiv IDs and SemanticScholar IDs from references
   - Saves to cache: `<STUDENT_ID>_citations.json`
   - Rate limit: 1 second between API calls

**4. Stage 1.3 - Cited Paper Metadata** (Sequential after Stage 1.2)
   - **Automatically starts** after Stage 1.2 completes in the same thread
   - Runs **in parallel** with Stage 2 (if still running)
   - Efficiently loads metadata for cited papers from Kaggle dataset
   - Scans local file (no API calls) - much faster than individual requests
   - Saves to cache: `<STUDENT_ID>_cited_dates.json`

**5. Final Stage - Update References**
   - Runs after all parallel stages complete
   - Updates `references.json` files with full citation metadata
   - Combines data from all previous stages

#### Performance Benefits

- **3x faster**: Stages 1.1, 1.2, and 2 run simultaneously
- **Resumable**: All stages cache results - interrupted runs can resume
- **Network optimized**: Stage 2 uses 8 parallel workers for downloads
- **I/O optimized**: Stage 1.3 runs while Stage 2 is still downloading

### Output Structure

The scraper creates the following directory structure in `data/<STUDENT_ID>/`:

```
data/
└── <STUDENT_ID>/
    ├── <yymm-id>/                    # One folder per paper (e.g., "2402-10011")
    │   ├── tex/                      # LaTeX source directory
    │   │   ├── <yymm-id>v<version>/  # Version subdirectory (e.g., "2402-10011v1")
    │   │   │   ├── *.tex             # TeX files (original structure preserved)
    │   │   │   ├── *.bib             # BibTeX files (original structure preserved)
    │   │   │   └── <subfolders>/     # Subdirectories with TeX/bib files
    │   │   │       ├── *.tex
    │   │   │       └── *.bib
    │   │   └── <yymm-id>v<version>/  # Additional versions...
    │   ├── metadata.json             # Paper metadata
    │   └── references.json           # Cited paper metadata
    └── performance_report.json       # Auto-generated statistics (for monitoring only)
```

### Cache Files

Cache files are stored in `cache/` directory:
- `<STUDENT_ID>_metadata.json`: Cached paper metadata
- `<STUDENT_ID>_citations.json`: Cached citation data
- `<STUDENT_ID>_cited_dates.json`: Cached cited paper metadata

**Resumability**: If the scraper is interrupted, it will resume from cache on the next run.

### Configuration

Edit `src/config.py` to customize:

- `ARXIV_API_DELAY`: Delay between arXiv API calls (default: 3.0 seconds)
- `SEMANTIC_SCHOLAR_DELAY`: Delay between Semantic Scholar API calls (default: 1.0 seconds)
- `MAX_RETRIES`: Number of retry attempts for failed requests (default: 3)
- `MAX_WORKERS`: Number of parallel workers for downloads (default: 8)
- `KAGGLE_METADATA_PATH`: Path to Kaggle dataset file (default: `../arxiv-metadata-oai-snapshot.json`)

## Performance Monitoring

The scraper automatically tracks:

- **Time metrics**: Total time, per-stage time, average time per paper
- **Memory metrics**: Initial, peak, and average memory usage
- **Success rates**: Papers, references, downloads
- **Storage metrics**: Size before/after figure removal
- **Reference statistics**: Total references, average per paper

All metrics are logged to console, `scraper.log`, and `performance_report.json`.

## Logging

Logs are written to:
- **Console**: INFO level messages
- **scraper.log**: Detailed DEBUG level messages with timestamps

## Troubleshooting

### "Student ID not found"
Make sure your student ID exists in `TCTA-DS1.csv` in the project root.

### "Kaggle metadata file not found"
Download the Kaggle dataset as described in Installation step 4. **This file is required** for the scraper to generate complete `references.json` files with submission dates. Without it, the output will be incomplete and fail requirements.

### "Rate limit exceeded"
The scraper includes built-in rate limiting, but if you encounter 429 errors:
- Increase delays in `config.py`
- Wait a few minutes and restart

### "Failed to download paper"
Some papers may not have source files available. The scraper will log these and continue.

### Memory issues
If you encounter memory problems:
- Reduce `MAX_WORKERS` in `config.py`
- Process papers in smaller batches

## Notes

- The scraper respects API rate limits to avoid being blocked
- Progress is saved to cache files for resumability
- Figure files are automatically removed to save space
- All dates are stored in ISO format
- The scraper is designed to handle failures gracefully and continue processing
