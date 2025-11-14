"""
Kaggle dataset handler for arXiv metadata
"""
import json
import os
from datetime import datetime
from logger import setup_logger

logger = setup_logger(__name__)

class KaggleMetadataHandler:
    """Handler for Kaggle arXiv metadata dataset"""
    
    def __init__(self, metadata_path):
        """
        Initialize handler
        
        Args:
            metadata_path: Path to arxiv-metadata-oai-snapshot.json file
        """
        self.metadata_path = metadata_path
        self.metadata_cache = {}
    
    def check_file_exists(self):
        """Check if Kaggle metadata file exists"""
        exists = os.path.exists(self.metadata_path)
        if not exists:
            logger.warning(f"Kaggle metadata file not found at {self.metadata_path}")
            logger.info("Please download from: https://www.kaggle.com/datasets/Cornell-University/arxiv")
        return exists
    
    def load_metadata_for_ids(self, arxiv_ids):
        """
        Load metadata for specific arXiv IDs from Kaggle dataset
        
        Args:
            arxiv_ids: List of arXiv IDs to fetch
            
        Returns:
            dict: Dictionary mapping arXiv ID to metadata
        """
        if not self.check_file_exists():
            logger.error("Cannot load metadata: Kaggle file not found")
            return {}
        
        arxiv_ids_set = set(arxiv_ids)
        found_metadata = {}
        
        logger.info(f"Loading metadata for {len(arxiv_ids)} papers from Kaggle dataset...")
        logger.info("This may take several minutes for large datasets...")
        
        try:
            import time
            start_time = time.time()
            lines_read = 0
            remaining_ids = arxiv_ids_set.copy()
            
            # Use larger buffer for faster I/O
            with open(self.metadata_path, 'r', encoding='utf-8', buffering=8*1024*1024) as f:
                count = 0
                for line in f:
                    lines_read += 1
                    
                    # Progress logging every 100k lines
                    if lines_read % 100000 == 0:
                        elapsed = time.time() - start_time
                        rate = lines_read / elapsed if elapsed > 0 else 0
                        logger.info(f"Scanned {lines_read:,} lines ({rate:.0f} lines/sec), found {count}/{len(arxiv_ids)} papers...")
                    
                    # Quick check: skip line if it doesn't contain probable IDs
                    # Most lines start with {"id":"YYMM.NNNNN" - check for pattern
                    if line[0:10].find('"id"') == -1:
                        continue
                    
                    try:
                        record = json.loads(line)
                        paper_id = record.get('id', '')
                        
                        # Check if this ID is in our remaining list
                        if paper_id in remaining_ids:
                            # Convert GMT date to ISO format
                            submission_date_gmt = record.get('versions', [{}])[0].get('created', '') if record.get('versions') else ''
                            submission_date_iso = ''
                            if submission_date_gmt:
                                try:
                                    # Parse GMT format: "Thu, 18 May 2023 17:35:35 GMT"
                                    dt = datetime.strptime(submission_date_gmt, '%a, %d %b %Y %H:%M:%S %Z')
                                    submission_date_iso = dt.isoformat() + '+00:00'
                                except ValueError:
                                    submission_date_iso = submission_date_gmt  # Keep original if parse fails
                            
                            # Normalize authors_parsed into list of strings (e.g., "First Last")
                            raw_authors = record.get('authors_parsed', [])
                            authors_list = []
                            if raw_authors and isinstance(raw_authors, (list, tuple)):
                                for a in raw_authors:
                                    if isinstance(a, (list, tuple)):
                                        last = a[0].strip() if len(a) > 0 and a[0] else ''
                                        first = a[1].strip() if len(a) > 1 and a[1] else ''
                                        if first and last:
                                            authors_list.append(f"{first} {last}")
                                        elif first:
                                            authors_list.append(first)
                                        elif last:
                                            authors_list.append(last)
                                    elif isinstance(a, str):
                                        authors_list.append(a.strip())
                            else:
                                # Fallback: try 'authors' field which may be a single string
                                authors_raw = record.get('authors', '')
                                if isinstance(authors_raw, str) and authors_raw:
                                    # Split common separators
                                    parts = [p.strip() for p in authors_raw.replace(';', ',').split(',') if p.strip()]
                                    authors_list.extend(parts)

                            found_metadata[paper_id] = {
                                'title': record.get('title', ''),
                                'authors': authors_list,
                                'submission_date': submission_date_iso
                            }
                            
                            remaining_ids.remove(paper_id)
                            count += 1
                            if count % 50 == 0:
                                logger.info(f"✓ Found {count}/{len(arxiv_ids)} papers ({count*100//len(arxiv_ids)}%)")
                            
                            # Early exit if we found all papers
                            if not remaining_ids:
                                logger.info(f"Found all {count} papers, stopping early")
                                break
                    
                    except (json.JSONDecodeError, KeyError):
                        continue
            
            elapsed = time.time() - start_time
            logger.info(f"Successfully loaded metadata for {len(found_metadata)}/{len(arxiv_ids)} papers in {elapsed:.1f}s")
            logger.info(f"Scanned {lines_read:,} total lines")
            return found_metadata
        
        except FileNotFoundError:
            logger.error(f"Kaggle metadata file not found: {self.metadata_path}")
            return {}
        except Exception as e:
            logger.error(f"Error loading Kaggle metadata: {e}")
            return {}
    
