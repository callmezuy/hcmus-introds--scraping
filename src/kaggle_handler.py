"""
Kaggle dataset handler for arXiv metadata
"""
import json
import os
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
        
        try:
            with open(self.metadata_path, 'r', encoding='utf-8') as f:
                count = 0
                for line in f:
                    try:
                        record = json.loads(line)
                        paper_id = record.get('id', '')
                        
                        # Check if this ID is in our list
                        if paper_id in arxiv_ids_set:
                            found_metadata[paper_id] = {
                                'arxiv_id': paper_id,
                                'title': record.get('title', ''),
                                'authors': record.get('authors_parsed', []),
                                'authors_string': record.get('authors', ''),
                                'submission_date': record.get('versions', [{}])[0].get('created', '') if record.get('versions') else '',
                                'revised_dates': [v.get('created', '') for v in record.get('versions', [])],
                                'categories': record.get('categories', ''),
                                'abstract': record.get('abstract', ''),
                                'doi': record.get('doi', ''),
                                'journal_ref': record.get('journal-ref', '')
                            }
                            
                            count += 1
                            if count % 100 == 0:
                                logger.info(f"Found {count}/{len(arxiv_ids)} papers...")
                            
                            # Early exit if we found all papers
                            if count >= len(arxiv_ids_set):
                                break
                    
                    except json.JSONDecodeError:
                        continue
            
            logger.info(f"Successfully loaded metadata for {len(found_metadata)}/{len(arxiv_ids)} papers")
            return found_metadata
        
        except FileNotFoundError:
            logger.error(f"Kaggle metadata file not found: {self.metadata_path}")
            return {}
        except Exception as e:
            logger.error(f"Error loading Kaggle metadata: {e}")
            return {}
    
    def extract_submission_dates(self, arxiv_ids):
        """
        Extract only submission dates for cited papers
        
        Args:
            arxiv_ids: List of arXiv IDs
            
        Returns:
            dict: Dictionary mapping arXiv ID to submission date
        """
        if not self.check_file_exists():
            logger.error("Cannot extract dates: Kaggle file not found")
            return {}
        
        arxiv_ids_set = set(arxiv_ids)
        submission_dates = {}
        
        logger.info(f"Extracting submission dates for {len(arxiv_ids)} papers from Kaggle dataset...")
        
        try:
            with open(self.metadata_path, 'r', encoding='utf-8') as f:
                count = 0
                for line in f:
                    try:
                        record = json.loads(line)
                        paper_id = record.get('id', '')
                        
                        if paper_id in arxiv_ids_set:
                            versions = record.get('versions', [])
                            if versions:
                                submission_dates[paper_id] = versions[0].get('created', '')
                            
                            count += 1
                            if count % 1000 == 0:
                                logger.info(f"Processed {count}/{len(arxiv_ids)} papers...")
                            
                            if count >= len(arxiv_ids_set):
                                break
                    
                    except json.JSONDecodeError:
                        continue
            
            logger.info(f"Successfully extracted dates for {len(submission_dates)}/{len(arxiv_ids)} papers")
            return submission_dates
        
        except Exception as e:
            logger.error(f"Error extracting submission dates: {e}")
            return {}
