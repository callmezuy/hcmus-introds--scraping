"""
arXiv API client for fetching paper metadata
"""
import arxiv
import os
import tarfile
import time
from config import ARXIV_API_DELAY, MAX_RETRIES, RETRY_DELAY
from logger import setup_logger

logger = setup_logger(__name__)

class ArxivClient:
    """Client for interacting with arXiv API"""
    
    def __init__(self):
        self.client = arxiv.Client()
        self.last_request_time = 0
    
    def _rate_limit(self):
        """Apply rate limiting between API calls"""
        elapsed = time.time() - self.last_request_time
        if elapsed < ARXIV_API_DELAY:
            time.sleep(ARXIV_API_DELAY - elapsed)
        self.last_request_time = time.time()
    
    def get_batch_metadata(self, arxiv_ids, batch_size=100):
        """
        Get metadata for multiple papers at once
        
        Args:
            arxiv_ids: List of arXiv IDs
            batch_size: Number of papers per API call (max 100)
            
        Returns:
            dict: Dictionary mapping arxiv_id to metadata
        """
        all_metadata = {}
        
        # Process in batches
        try:
            for i in range(0, len(arxiv_ids), batch_size):
                batch = arxiv_ids[i:i+batch_size]
                logger.info(f"Fetching batch {i//batch_size + 1}: {len(batch)} papers")
            
                for attempt in range(MAX_RETRIES):
                    try:
                        self._rate_limit()
                        
                        search = arxiv.Search(id_list=batch, max_results=batch_size)
                        
                        for paper in self.client.results(search):
                            arxiv_id = paper.entry_id.split('/')[-1].split('v')[0]  # Extract ID
                            
                            metadata = {
                                'title': paper.title,
                                'authors': [author.name for author in paper.authors],
                                'submission_date': paper.published.isoformat(),
                                'revised_dates': [],
                                'journal_ref': paper.journal_ref
                            }
                            
                            # Get version information
                            if hasattr(paper, '_raw') and 'arxiv:version' in paper._raw:
                                versions = paper._raw.get('arxiv:version', [])
                                if not isinstance(versions, list):
                                    versions = [versions]
                                
                                for version in versions:
                                    created = version.get('created', '')
                                    metadata['revised_dates'].append(created)
                            
                            # Fallback if version info not available
                            if not metadata['revised_dates']:
                                metadata['revised_dates'].append(paper.published.isoformat())
                                if paper.updated != paper.published:
                                    metadata['revised_dates'].append(paper.updated.isoformat())
                            
                            all_metadata[arxiv_id] = metadata
                        
                        logger.info(f"Successfully fetched {len(all_metadata)} papers")
                        break
                        
                    except KeyboardInterrupt:
                        logger.warning("Interrupted by user (Ctrl+C)")
                        raise
                    except Exception as e:
                        logger.warning(f"Batch attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                        if attempt < MAX_RETRIES - 1:
                            # Exponential backoff: wait longer each retry (especially for 429)
                            wait_time = RETRY_DELAY * (2 ** attempt)
                            logger.info(f"Waiting {wait_time}s before retry...")
                            try:
                                time.sleep(wait_time)
                            except KeyboardInterrupt:
                                logger.warning("Interrupted during retry wait")
                                raise
                        else:
                            logger.error(f"Failed to fetch batch after {MAX_RETRIES} attempts")
        except KeyboardInterrupt:
            logger.warning("Batch fetching interrupted")
            raise
        
        return all_metadata
    
    def get_paper_metadata(self, arxiv_id):
        """
        Get metadata for a paper including all versions
        
        Args:
            arxiv_id: arXiv ID (e.g., "2310.12345")
            
        Returns:
            dict: Paper metadata including title, authors, dates, versions
        """
        for attempt in range(MAX_RETRIES):
            try:
                self._rate_limit()
                
                search = arxiv.Search(id_list=[arxiv_id])
                paper = next(self.client.results(search))
                
                # Extract metadata
                metadata = {
                    'title': paper.title,
                    'authors': [author.name for author in paper.authors],
                    'submission_date': paper.published.isoformat(),
                    'revised_dates': [],
                    'journal_ref': paper.journal_ref
                }
                
                # Get version information
                if hasattr(paper, '_raw') and 'arxiv:version' in paper._raw:
                    versions = paper._raw.get('arxiv:version', [])
                    if not isinstance(versions, list):
                        versions = [versions]
                    
                    for version in versions:
                        created = version.get('created', '')
                        metadata['revised_dates'].append(created)
                
                # Fallback if version info not available
                if not metadata['revised_dates']:
                    metadata['revised_dates'].append(paper.published.isoformat())
                    if paper.updated != paper.published:
                        metadata['revised_dates'].append(paper.updated.isoformat())
                
                logger.info(f"Successfully fetched metadata for {arxiv_id}")
                return metadata
                
            except StopIteration:
                logger.error(f"Paper {arxiv_id} not found in arXiv")
                return None
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES} failed for {arxiv_id}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error(f"Failed to fetch metadata for {arxiv_id} after {MAX_RETRIES} attempts")
                    return None
    
    def download_source_version(self, arxiv_id_with_version, save_dir):
        """
        Download source files for a specific version of a paper
        
        Args:
            arxiv_id_with_version: arXiv ID with version (e.g., "2310.12345v1")
            save_dir: Directory to save the source file
            
        Returns:
            str: Path to downloaded file, or None if failed
        """
        for attempt in range(MAX_RETRIES):
            try:
                self._rate_limit()
                
                search = arxiv.Search(id_list=[arxiv_id_with_version])
                paper = next(self.client.results(search))
                
                filename = f"{arxiv_id_with_version.replace('.', '-')}.tar.gz"
                downloaded_path = paper.download_source(dirpath=save_dir, filename=filename)
                
                # Validate downloaded file
                if downloaded_path and os.path.exists(downloaded_path):
                    # Check if it's a valid tar.gz file
                    if not tarfile.is_tarfile(downloaded_path):
                        logger.error(f"Downloaded file is not a valid tar file: {arxiv_id_with_version}")
                        os.remove(downloaded_path)
                        return None
                
                logger.info(f"Successfully downloaded source for {arxiv_id_with_version}")
                return downloaded_path
                
            except StopIteration:
                logger.error(f"Paper {arxiv_id_with_version} not found for download")
                return None
            except Exception as e:
                logger.warning(f"Download attempt {attempt + 1}/{MAX_RETRIES} failed for {arxiv_id_with_version}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error(f"Failed to download source for {arxiv_id_with_version} after {MAX_RETRIES} attempts")
                    return None
