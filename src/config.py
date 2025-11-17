"""
Configuration file for arXiv scraper
"""
import csv
import os

SEMANTIC_SCHOLAR_API_KEY = "9JcwvT4mJ39GR0cF7ntcB34Qg2pCJSS614DhOP2y"
KAGGLE_DATASET_PATH = "../dataset/arxiv-metadata-oai-snapshot.json"

# API Rate Limits
ARXIV_API_DELAY = 3.0
SEMANTIC_SCHOLAR_DELAY = 1.0
MAX_RETRIES = 3
RETRY_DELAY = 2.0

# Paths
DATA_DIR = os.path.join("..", "data")
CACHE_DIR = os.path.join("..", "cache")

# Logging
LOG_FILE = "scraper.log"
LOG_LEVEL = "INFO"

# Threading
MAX_WORKERS = 16
SEMANTIC_SCHOLAR_WORKERS = 4

def get_assigned_range(student_id, csv_path="../TCTA-DS1.csv"):
    """
    Get the assigned arXiv paper range for a student ID
    
    Args:
        student_id: Student ID (e.g., "23127XXX")
        csv_path: Path to the CSV file with assignments
        
    Returns:
        dict: Contains start_month, start_id, end_month, end_id
    """
    # Try different encodings
    for encoding in ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']:
        try:
            with open(csv_path, 'r', encoding=encoding) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['MSSV'] == str(student_id):
                        return {
                            'start_month': row['start_month'],
                            'start_id': int(row['start_id']),
                            'end_month': row['end_month'],
                            'end_id': int(row['end_id'])
                        }
            break
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Student ID {student_id} not found in {csv_path}")

def format_arxiv_id(month, paper_id):
    """
    Format arXiv ID from month and paper ID
    
    Args:
        month: Month in format YYYY-MM (e.g., "2024-02")
        paper_id: Paper ID number
        
    Returns:
        str: Formatted arXiv ID (e.g., "2402.10011")
    """
    # Extract year and month, use only last 2 digits of year
    parts = month.split('-')
    year = parts[0][-2:]  # Last 2 digits of year
    month_num = parts[1]
    return f"{year}{month_num}.{paper_id:05d}"

def parse_arxiv_id(arxiv_id):
    """
    Parse arXiv ID to extract month and paper ID
    
    Args:
        arxiv_id: arXiv ID (e.g., "2402.10011")
        
    Returns:
        tuple: (month, paper_id) where month is "YYYY-MM" format
    """
    parts = arxiv_id.split('.')
    if len(parts) != 2:
        raise ValueError(f"Invalid arXiv ID format: {arxiv_id}")
    
    year_month = parts[0]
    paper_id = int(parts[1])
    
    # Convert YYMM to YYYY-MM (assuming 20xx for years)
    if len(year_month) == 4:
        month = f"20{year_month[:2]}-{year_month[2:]}"
    else:
        # Fallback for older format
        month = f"{year_month[:4]}-{year_month[4:]}"
    
    return month, paper_id

def format_folder_name(arxiv_id):
    """
    Format folder name from arXiv ID
    
    Args:
        arxiv_id: arXiv ID (e.g., "2402.10011")
        
    Returns:
        str: Folder name (e.g., "2402-10011")
    """
    return arxiv_id.replace('.', '-')
