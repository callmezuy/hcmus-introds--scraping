# arXiv Paper Scraper - Lab 01

## Overview

This project implements a comprehensive arXiv paper scraper that collects full-text LaTeX sources, metadata, and reference information for assigned papers. The scraper follows an optimized pipeline design that efficiently retrieves data using multiple sources (arXiv API, and Semantic Scholar API).

## Requirements

- Python 3.8 or higher
- Internet connection for API access

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

## Project Structure

```
.
├── src/
│   ├── config.py                    # Configuration and settings
│   ├── logger.py                    # Logging utilities
│   ├── performance.py               # Performance monitoring
│   ├── arxiv_client.py              # arXiv API client
│   ├── semantic_scholar_client.py   # Semantic Scholar API client
│   ├── file_processor.py            # File extraction and processing
│   └── scraper.py                   # Main scraper orchestrator
├── requirements.txt                 # Python dependencies
├── README.md                        # This file
├── Report.docx                      # Detailed implementation report
├── TCTA-DS1.csv                     # Student assignment file
```

## Usage

### Basic Usage

Run the scraper with your student ID:

```bash
cd src
python scraper.py <STUDENT_ID> [MAX_PAPERS]
```

Test mode: Process only X papers
```bash
python scraper.py <STUDENT_ID> [X]
```

Single-paper run:
```bash
python src/scraper.py <STUDENT_ID> -p <PAPER_ID>
```

Control copying of large `.bib` files (default: skip > 5 MB):

```bash
python src/scraper.py <STUDENT_ID> --no-skip-large-bib
python src/scraper.py <STUDENT_ID> --bib-threshold-mb 10.0
```

### Configuration

Edit `src/config.py` to customize:

- `ARXIV_API_DELAY`: Delay between arXiv API calls (default: 3.0 seconds)
- `SEMANTIC_SCHOLAR_DELAY`: Delay between Semantic Scholar API calls (default: 1.0 seconds)
- `MAX_RETRIES`: Number of retry attempts for failed requests (default: 3)
- `MAX_WORKERS`: Number of parallel workers for downloads (default: 8)


## Troubleshooting

### "Student ID not found"
Make sure your student ID exists in `TCTA-DS1.csv` in the project root.

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
