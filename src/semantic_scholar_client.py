"""
Semantic Scholar API client for fetching citations
"""
import requests
import time
from config import SEMANTIC_SCHOLAR_DELAY, MAX_RETRIES, RETRY_DELAY
from logger import setup_logger

logger = setup_logger(__name__)

class SemanticScholarClient:
    """Client for interacting with Semantic Scholar API"""
    
    def __init__(self, api_key=None):
        self.base_url = "https://api.semanticscholar.org/graph/v1"
        self.api_key = api_key
        self.last_request_time = 0
        self.headers = {}
        if api_key:
            self.headers['x-api-key'] = api_key
    
    def _rate_limit(self):
        """Apply rate limiting between API calls"""
        elapsed = time.time() - self.last_request_time
        if elapsed < SEMANTIC_SCHOLAR_DELAY:
            time.sleep(SEMANTIC_SCHOLAR_DELAY - elapsed)
        self.last_request_time = time.time()
    
    def get_paper_references(self, arxiv_id):
        """
        Get references for a paper
        
        Args:
            arxiv_id: arXiv ID (e.g., "2310.12345")
            
        Returns:
            list: List of reference dictionaries with arXiv IDs and metadata
        """
        for attempt in range(MAX_RETRIES):
            try:
                self._rate_limit()
                
                url = f"{self.base_url}/paper/arXiv:{arxiv_id}"
                params = {
                    'fields': 'references,references.paperId,references.externalIds,references.title,references.authors,references.year,references.publicationDate'
                }
                
                response = requests.get(url, params=params, headers=self.headers, timeout=30)
                
                if response.status_code == 404:
                    logger.warning(f"Paper {arxiv_id} not found in Semantic Scholar")
                    return []
                
                if response.status_code == 429:
                    logger.warning(f"Rate limit exceeded for {arxiv_id}, waiting...")
                    time.sleep(RETRY_DELAY * 2)
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                references = []
                refs_data = data.get('references', [])
                
                for ref in refs_data:
                    ref_info = {
                        'title': ref.get('title', ''),
                        'authors': [author.get('name', '') for author in ref.get('authors', [])],
                        'year': ref.get('year'),
                        'publication_date': ref.get('publicationDate'),
                        'semantic_scholar_id': ref.get('paperId', '')
                    }
                    
                    # Extract arXiv ID if available
                    external_ids = ref.get('externalIds', {})
                    if external_ids and 'ArXiv' in external_ids:
                        ref_info['arxiv_id'] = external_ids['ArXiv']
                    
                    references.append(ref_info)
                
                logger.info(f"Fetched {len(references)} references for {arxiv_id} ({len([r for r in references if 'arxiv_id' in r])} with arXiv IDs)")
                return references
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES} failed for {arxiv_id}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error(f"Failed to fetch references for {arxiv_id} after {MAX_RETRIES} attempts")
                    return []
