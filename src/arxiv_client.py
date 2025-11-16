"""
Clean arXiv API client for fetching paper metadata and sources.
"""

import os
import time
import json
import re
import arxiv
from datetime import datetime, timezone
import unicodedata

from config import ARXIV_API_DELAY, MAX_RETRIES, RETRY_DELAY, format_folder_name
from logger import setup_logger


logger = setup_logger(__name__)


class ArxivClient:
    """Client for interacting with the arXiv API."""

    def __init__(self, monitor=None):
        self.client = arxiv.Client()
        self.last_request_time = 0
        self.monitor = monitor

    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < ARXIV_API_DELAY:
            time.sleep(ARXIV_API_DELAY - elapsed)
        self.last_request_time = time.time()

    def get_batch_metadata(self, arxiv_ids, batch_size=100):
        """Fetch metadata for many arXiv IDs in batches.

        Returns a dict mapping arXiv id (without version) to metadata.
        """
        all_metadata = {}

        for i in range(0, len(arxiv_ids), batch_size):
            batch = arxiv_ids[i : i + batch_size]
            logger.info(f"Fetching batch {i // batch_size + 1}: {len(batch)} papers")

            for attempt in range(MAX_RETRIES):
                try:
                    self._rate_limit()

                    start = time.time()
                    search = arxiv.Search(id_list=batch, max_results=len(batch))
                    for paper in self.client.results(search):
                        arxiv_id = paper.entry_id.split("/")[-1].split("v")[0]
                        def _normalize_author(a):
                            # Preserve raw author string from arXiv (minimal trimming only)
                            try:
                                name = a.name if hasattr(a, 'name') else str(a)
                            except Exception:
                                name = str(a)
                            try:
                                name = str(name).strip()
                            except Exception:
                                name = ''
                            return name

                        metadata = {
                            "title": paper.title,
                            "authors": [_normalize_author(author) for author in paper.authors],
                            "submission_date": paper.published.isoformat(),
                            "revised_dates": [],
                            "journal_ref": paper.journal_ref,
                        }
                    
                        revised_dates = []
                        try:
                            vers_attr = getattr(paper, 'versions', None)
                            if vers_attr:
                                for v in vers_attr:
                                    # v may be dict-like or object with 'created'
                                    created = None
                                    try:
                                        if isinstance(v, dict):
                                            created = v.get('created') or v.get('date') or v.get('timestamp')
                                        else:
                                            created = getattr(v, 'created', None) or getattr(v, 'date', None)
                                    except Exception:
                                        created = None
                                    if created:
                                        # If datetime -> isoformat, else keep raw string
                                        try:
                                            if isinstance(created, datetime):
                                                if created.tzinfo is None:
                                                    created = created.replace(tzinfo=timezone.utc)
                                                revised_dates.append(created.isoformat())
                                            else:
                                                revised_dates.append(str(created))
                                        except Exception:
                                            try:
                                                revised_dates.append(str(created))
                                            except Exception:
                                                pass
                        except Exception:
                            pass

                        # Deduplicate preserving order
                        seen = set()
                        deduped = []
                        for d in revised_dates:
                            if not d:
                                continue
                            if d not in seen:
                                seen.add(d)
                                deduped.append(d)

                        metadata['revised_dates'] = deduped

                        all_metadata[arxiv_id] = metadata

                    elapsed = time.time() - start
                    if self.monitor:
                        try:
                            self.monitor.incr_http_requests(1)
                            self.monitor.add_network_time(elapsed)
                        except Exception:
                            pass

                    break

                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    logger.warning(f"Batch attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                    if attempt < MAX_RETRIES - 1:
                        wait_time = RETRY_DELAY * (2 ** attempt)
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Failed to fetch batch after {MAX_RETRIES} attempts")

        return all_metadata

    def get_paper_metadata(self, arxiv_id):
        return self.get_batch_metadata([arxiv_id], batch_size=1)

    def download_all_versions(self, arxiv_id, save_dir, max_versions=10):
        """Download successive versions v1..vN for a base arXiv id.

        Returns a list of tuples (downloaded_path, version_tag) for each successfully
        downloaded version. Stops when a version is not found or when download fails.
        """
        results = []

        # Normalize base id (strip any vN suffix)
        base = arxiv_id
        m = re.match(r"^(?P<base>.+?)v(?P<num>\d+)$", arxiv_id)
        if m:
            base = m.group('base')

        try:
            if save_dir:
                try:
                    os.makedirs(save_dir, exist_ok=True)
                except Exception:
                    pass

            for v in range(1, max_versions + 1):
                id_with_version = f"{base}v{v}"
                try:
                    search = arxiv.Search(id_list=[id_with_version])
                    paper = next(self.client.results(search))
                except StopIteration:
                    break

                try:
                    version_tag = ""
                    try:
                        eid = paper.entry_id.split('/')[-1]
                        if 'v' in eid:
                            version_tag = 'v' + eid.split('v')[-1]
                    except Exception:
                        version_tag = f'v{v}'

                    start = time.time()
                    downloaded_path = paper.download_source(dirpath=save_dir)
                    elapsed = time.time() - start
                    if self.monitor:
                        try:
                            self.monitor.add_network_time(elapsed)
                        except Exception:
                            pass

                    if downloaded_path and os.path.exists(downloaded_path):
                        results.append((downloaded_path, version_tag))
                    else:
                        # If the download didn't produce a file, stop further attempts
                        break
                except Exception as e:
                    logger.warning(f"Failed to download {id_with_version}: {e}")
                    # Stop on first download error
                    break

        except Exception as e:
            logger.error(f"Unexpected error while downloading versions for {arxiv_id}: {e}")

        return results

    def write_metadata_files(self, metadata_dict, data_dir):
        """Write per-paper `metadata.json` files atomically."""
        for pid, meta in (metadata_dict or {}).items():
            try:
                folder_name = format_folder_name(pid)
                paper_dir = os.path.join(data_dir, folder_name)
                os.makedirs(paper_dir, exist_ok=True)
                meta_file = os.path.join(paper_dir, "metadata.json")
                tmp_meta = meta_file + ".tmp"
                try:
                    with open(tmp_meta, "w", encoding="utf-8") as mf:
                        # preserve unicode characters in author names
                        json.dump(meta, mf, indent=2, ensure_ascii=False)
                        try:
                            mf.flush()
                            os.fsync(mf.fileno())
                        except Exception:
                            pass

                    try:
                        os.replace(tmp_meta, meta_file)
                    except Exception:
                        try:
                            if os.path.exists(meta_file):
                                os.remove(meta_file)
                        except Exception:
                            pass
                        os.rename(tmp_meta, meta_file)
                    # Increment monitor counter for metadata files written
                    try:
                        if self.monitor:
                            self.monitor.increment_stat('metadata_files_written')
                    except Exception:
                        pass
                except Exception as e:
                    logger.warning(f"Failed to write metadata.json for {pid}: {e}")
            except Exception:
                try:
                    logger.warning(f"Unexpected error writing metadata for {pid}")
                except Exception:
                    pass
