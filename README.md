# arXiv Paper Scraper - Lab 01

## Overview

This project implements a comprehensive arXiv paper scraper that collects full-text LaTeX sources, metadata, and reference information for assigned papers. The scraper follows an optimized pipeline design that efficiently retrieves data using multiple sources (arXiv API, Semantic Scholar API, and Kaggle dataset).

## Requirements

- Python 3.8 or higher
- Internet connection for API access
- Kaggle arXiv metadata dataset (optional but recommended for Stage 1.3)

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

### 4. Download Kaggle dataset (Optional but recommended)

For optimal performance in Stage 1.3, download the Kaggle arXiv metadata:

1. Go to: https://www.kaggle.com/datasets/Cornell-University/arxiv
2. Download `arxiv-metadata-oai-snapshot.json`
3. Place it in the project root directory

**Note:** If this file is not present, the scraper will still work but Stage 1.3 will be skipped or use fallback methods.

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

The scraper executes the following pipeline:

1. **Stage 1.1 - Metadata Collection**: Fetches metadata for assigned papers using arXiv API
   - Retrieves title, authors, abstract, submission dates, revised dates, versions
   - Saves to cache file for resumability

2. **Stage 1.2 - Citation Retrieval**: Gets citation lists using Semantic Scholar API
   - Collects references for each paper
   - Extracts arXiv IDs from references
   - Saves to cache file

3. **Stage 1.3 - Cited Paper Metadata**: Retrieves metadata for cited papers from Kaggle dataset
   - Efficiently loads metadata for thousands of cited papers
   - Much faster than individual API calls
   - Saves to cache file

4. **Stage 2 - Download and Processing**: Downloads source files and processes them
   - Downloads `.tar.gz` source files
   - Extracts and identifies `.tex` and `.bib` files
   - Removes figure files to reduce size
   - Organizes files into structured directories
   - Creates `metadata.json` and `references.json` for each paper

### Output Structure

The scraper creates the following directory structure in `data/<STUDENT_ID>/`:

```
data/
└── <STUDENT_ID>/
    ├── <YYYYMM-ID>/           # One folder per paper
    │   ├── tex/               # LaTeX source files
    │   │   ├── main.tex
    │   │   └── ...
    │   ├── references.bib     # Merged bibliography file
    │   ├── metadata.json      # Paper metadata
    │   └── references.json    # Cited paper metadata
    └── performance_report.json  # Performance statistics
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
- `SEMANTIC_SCHOLAR_DELAY`: Delay between Semantic Scholar API calls (default: 1.1 seconds)
- `MAX_RETRIES`: Number of retry attempts for failed requests (default: 3)
- `MAX_WORKERS`: Number of parallel workers for downloads (default: 4)
- `KAGGLE_METADATA_PATH`: Path to Kaggle dataset file

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
Download the Kaggle dataset as described in Installation step 4. The scraper can still run without it but performance will be degraded.

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

## Support

For questions or issues, contact the course instructor as specified in the lab description.
