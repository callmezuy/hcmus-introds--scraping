"""
Semantic Scholar API client for fetching references
"""
import requests
import time
import os
import json
import hashlib
import re
from config import SEMANTIC_SCHOLAR_DELAY, MAX_RETRIES, RETRY_DELAY
from logger import setup_logger

logger = setup_logger(__name__)


class SemanticScholarClient:
    """Client for interacting with Semantic Scholar API"""

    def __init__(self, api_key=None, monitor=None):
        self.base_url = "https://api.semanticscholar.org/graph/v1"
        self.api_key = api_key
        self.last_request_time = 0
        self.headers = {}
        self.monitor = monitor
        if api_key:
            self.headers['x-api-key'] = api_key

    def _rate_limit(self):
        """Apply rate limiting between API calls"""
        elapsed = time.time() - self.last_request_time
        if elapsed < SEMANTIC_SCHOLAR_DELAY:
            time.sleep(SEMANTIC_SCHOLAR_DELAY - elapsed)

    def get_paper_references(self, arxiv_id):
        """
        Get references for a paper and return a dict keyed by cited arXiv ID.

        Args:
            arxiv_id: arXiv ID (e.g., "2310.12345")

        Returns:
            dict: Mapping from cited arXiv ID -> metadata dict with keys:
                  'title', 'authors' (list), 'submission_date' (ISO), 'semantic_scholar_id'
        """

        for attempt in range(MAX_RETRIES):
            try:
                self._rate_limit()

                url = f"{self.base_url}/paper/arXiv:{arxiv_id}"
                params = {
                    'fields': 'references,references.paperId,references.externalIds,references.title,references.authors,references.year,references.publicationDate'
                }

                start = time.time()
                response = requests.get(url, params=params, headers=self.headers, timeout=30)
                elapsed = time.time() - start
                self.last_request_time = time.time()

                if self.monitor:
                    try:
                        self.monitor.incr_http_requests(1)
                        self.monitor.add_network_time(elapsed)
                    except Exception:
                        pass

                if response.status_code == 404:
                    logger.warning(f"Paper {arxiv_id} not found in Semantic Scholar")
                    return {}

                if response.status_code == 429:
                    logger.warning(f"Rate limit exceeded for {arxiv_id}, waiting...")
                    if self.monitor:
                        try:
                            self.monitor.incr_http_429(1)
                        except Exception:
                            pass
                    time.sleep(RETRY_DELAY * (2 ** attempt))
                    continue

                response.raise_for_status()
                data = response.json()

                refs_data = data.get('references', []) or []
                references_dict = {}

                for ref in refs_data:
                    external_ids = ref.get('externalIds') or {}
                    ssid = ref.get('paperId') or ''

                    # Detect common external ids
                    arxiv_ext = None
                    doi_ext = None
                    if isinstance(external_ids, dict):
                        arxiv_ext = external_ids.get('ArXiv') or external_ids.get('arXiv')
                        doi_ext = external_ids.get('DOI') or external_ids.get('doi')

                    # Normalize authors (preserve raw strings, trimmed)
                    authors = []
                    for a in ref.get('authors', []) or []:
                        name = ''
                        if isinstance(a, dict):
                            name = a.get('name') or a.get('author') or ''
                        elif isinstance(a, str):
                            name = a
                        try:
                            name = str(name).strip()
                        except Exception:
                            name = ''
                        if name:
                            authors.append(name)

                    title = ref.get('title', '') or ''
                    submission_date = ref.get('publicationDate') or ''

                    # Determine a stable key for the reference:
                    # Prefer arXiv (cleaned without version), then Semantic Scholar paperId,
                    # then DOI, otherwise a short hash of title+authors.
                    stable_key = None
                    original_id = ''

                    if arxiv_ext:
                        try:
                            arxiv_raw = str(arxiv_ext).strip()
                            if ':' in arxiv_raw:
                                arxiv_raw = arxiv_raw.split(':')[-1]
                            arxiv_clean = arxiv_raw.split('v')[0]
                            stable_key = str(arxiv_clean)
                            original_id = arxiv_raw
                        except Exception:
                            stable_key = str(arxiv_ext)
                            original_id = str(arxiv_ext)
                    elif ssid:
                        stable_key = f"SS:{ssid}"
                        original_id = ssid
                    elif doi_ext:
                        stable_key = f"DOI:{doi_ext}"
                        original_id = doi_ext
                    else:
                        # Fallback: deterministic short hash
                        h = hashlib.sha256()
                        h.update(title.encode('utf-8', errors='ignore'))
                        h.update(b'|')
                        h.update('||'.join(authors).encode('utf-8', errors='ignore'))
                        short = h.hexdigest()[:12]
                        stable_key = f"HASH:{short}"
                        original_id = ''

                    entry = {
                        'title': title,
                        'authors': authors,
                        'submission_date': submission_date,
                        'semantic_scholar_id': ssid,
                        'original_id': original_id,
                        'external_ids': external_ids or {}
                    }

                    # Use the stable key (string) as the dictionary key
                    try:
                        references_dict[str(stable_key)] = entry
                    except Exception:
                        # As a last resort, skip malformed refs
                        continue

                logger.info(f"Fetched {len(references_dict)} references (including non-arXiv) for {arxiv_id}")
                return references_dict

            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES} failed for {arxiv_id}: {e}")
                if self.monitor:
                    try:
                        self.monitor.incr_http_retries(1)
                    except Exception:
                        pass
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (2 ** attempt))
                else:
                    logger.error(f"Failed to fetch references for {arxiv_id} after {MAX_RETRIES} attempts")
                    return {}

    def write_references_json(self, paper_id, paper_dir):
        """
        Fetch references for `paper_id` and write `references.json` into `paper_dir`.

        Returns:
            bool: True if file written, False if nothing to write or on error.
        """

        try:
            refs = self.get_paper_references(paper_id) or {}
            references_dict = {}
            skipped_non_arxiv = 0

            # Keep only references whose stable key looks like an arXiv id (YYMM.NNNNN)
            arxiv_re = re.compile(r'^\d{4}\.\d+')
            for aid, meta in refs.items():
                try:
                    if isinstance(aid, str) and arxiv_re.match(aid):
                        # Convert dot to dash in keys to match existing format (yymm-id)
                        key = aid.replace('.', '-')
                        entry = {
                            'title': meta.get('title', ''),
                            'authors': meta.get('authors', []),
                            'submission_date': meta.get('submission_date', ''),
                            'semantic_scholar_id': meta.get('semantic_scholar_id', '')
                        }
                        references_dict[key] = entry
                    else:
                        skipped_non_arxiv += 1
                except Exception:
                    # Skip malformed entries
                    continue

            os.makedirs(paper_dir, exist_ok=True)
            ref_output = os.path.join(paper_dir, 'references.json')
            tmp_output = ref_output + '.tmp'
            try:
                with open(tmp_output, 'w', encoding='utf-8') as f:
                    json.dump(references_dict, f, indent=2, ensure_ascii=False)
                    try:
                        f.flush()
                        os.fsync(f.fileno())
                    except Exception:
                        pass

                try:
                    os.replace(tmp_output, ref_output)
                except Exception:
                    try:
                        if os.path.exists(ref_output):
                            os.remove(ref_output)
                    except Exception:
                        pass
                    os.rename(tmp_output, ref_output)

                logger.info(f"Wrote references.json for {paper_id} -> {ref_output}")
                # Increment monitor counter for references files written
                try:
                    if self.monitor:
                        self.monitor.increment_stat('references_files_written')
                except Exception:
                    pass
                return True
            except Exception as e:
                logger.error(f"Failed to write references.json for {paper_id}: {e}")
                try:
                    if os.path.exists(tmp_output):
                        os.remove(tmp_output)
                except Exception:
                    pass
                return False

        except Exception as e:
            logger.error(f"Error preparing references.json for {paper_id}: {e}")
            return False

