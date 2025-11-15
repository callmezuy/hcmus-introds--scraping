"""
Main scraper orchestrator
"""
import os
import json
import time
import threading
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    get_assigned_range, format_arxiv_id, format_folder_name,
    DATA_DIR, CACHE_DIR, MAX_WORKERS, SEMANTIC_SCHOLAR_WORKERS
)
from logger import setup_logger
from performance import PerformanceMonitor
from arxiv_client import ArxivClient
from semantic_scholar_client import SemanticScholarClient
from file_processor import FileProcessor

logger = setup_logger(__name__)

class ArxivScraper:
    """Main scraper class orchestrating the entire pipeline"""
    
    def __init__(self, student_id, max_papers=None, skip_large_bib=True, bib_size_threshold=5*1024*1024, skip_missing_source=True):
        """
        Initialize scraper
        
        Args:
            student_id: Student ID for getting assigned range
            max_papers: Maximum number of papers to process (None for all)
        """
        self.student_id = str(student_id)
        self.max_papers = max_papers
        # Monitor should be created before clients/processors that may use it
        self.monitor = PerformanceMonitor()
        self.arxiv_client = ArxivClient(monitor=self.monitor)
        self.semantic_scholar_client = SemanticScholarClient(api_key="9JcwvT4mJ39GR0cF7ntcB34Qg2pCJSS614DhOP2y", monitor=self.monitor)
        self.file_processor = FileProcessor(monitor=self.monitor)
        # Options for skipping large .bib files when copying
        self.skip_large_bib = skip_large_bib
        self.bib_size_threshold = bib_size_threshold
        
        # Get assigned paper range
        self.paper_range = get_assigned_range(student_id)
        logger.info(f"Assigned range: {self.paper_range}")
        if max_papers:
            logger.info(f"TEST MODE: Limited to {max_papers} papers")
        
        # Create directories
        self.data_dir = os.path.join(DATA_DIR, self.student_id)
        self.cache_dir = CACHE_DIR
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Cache files
        self.metadata_cache_file = os.path.join(self.cache_dir, f"{self.student_id}_metadata.json")
        self.citations_cache_file = os.path.join(self.cache_dir, f"{self.student_id}_citations.json")
        self.cited_dates_cache_file = os.path.join(self.cache_dir, f"{self.student_id}_cited_dates.json")
        # Behavior: whether to skip papers that have no downloadable source
        self.skip_missing_source = skip_missing_source
    
    def generate_paper_ids(self):
        """
        Generate list of arXiv IDs in assigned range
        
        Returns:
            list: List of arXiv IDs
        """
        paper_ids = []
        
        start_month = self.paper_range['start_month']
        start_id = self.paper_range['start_id']
        end_month = self.paper_range['end_month']
        end_id = self.paper_range['end_id']
        
        # Parse months
        start_year, start_mon = map(int, start_month.split('-'))
        end_year, end_mon = map(int, end_month.split('-'))
        
        current_year = start_year
        current_mon = start_mon
        current_id = start_id
        
        while (current_year < end_year) or (current_year == end_year and current_mon <= end_mon):
            month_str = f"{current_year}-{current_mon:02d}"
            
            # Determine the range for this month
            if current_year == start_year and current_mon == start_mon:
                id_start = start_id
            else:
                id_start = 0
            
            if current_year == end_year and current_mon == end_mon:
                id_end = end_id
            else:
                # Use a large number for months in between
                id_end = 99999
            
            # Generate IDs for this month
            for paper_id in range(id_start, id_end + 1):
                arxiv_id = format_arxiv_id(month_str, paper_id)
                paper_ids.append(arxiv_id)
                
                # Safety check - don't generate too many
                if len(paper_ids) > 10000:
                    logger.warning("Generated over 10000 IDs, stopping")
                    return paper_ids
            
            # Move to next month
            current_mon += 1
            if current_mon > 12:
                current_mon = 1
                current_year += 1
        
        logger.info(f"Generated {len(paper_ids)} paper IDs")
        return paper_ids
    
    def fetch_metadata(self, paper_ids):
        """Fetch only metadata (Stage 1.1)"""
        # Check cache
        if os.path.exists(self.metadata_cache_file):
            with open(self.metadata_cache_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            if len(metadata) >= len(paper_ids):
                logger.info(f"Loaded {len(metadata)} papers from metadata cache")
                return metadata
            logger.info(f"Resuming: loaded {len(metadata)} papers from partial cache")
        else:
            metadata = {}
        
        # Fetch remaining papers
        remaining_papers = [pid for pid in paper_ids if pid not in metadata]
        logger.info(f"Fetching {len(remaining_papers)} remaining papers")
        
        batch_size = 100  # arXiv API limit
        for i in range(0, len(remaining_papers), batch_size):
            batch = remaining_papers[i:i+batch_size]
            logger.info(f"Fetching batch {i//batch_size + 1}/{(len(remaining_papers)-1)//batch_size + 1}")
            
            batch_metadata = self.arxiv_client.get_batch_metadata(batch, batch_size=batch_size)
            metadata.update(batch_metadata)
            
            # Save cache after each batch
            with open(self.metadata_cache_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            logger.info(f"Progress saved: {len(metadata)}/{len(paper_ids)} papers")
        
        # Paper success/failure will be tracked in stage 2 (download)
        
        logger.info(f"Metadata complete: {len(metadata)} total papers")
        return metadata
    
    def fetch_citations(self, paper_ids):
        """Fetch citations using parallel threads with caching and resume support"""
        # Check cache (defensive: handle corrupt/null cache files)
        if os.path.exists(self.citations_cache_file):
            with open(self.citations_cache_file, 'r', encoding='utf-8') as f:
                try:
                    raw = json.load(f)
                except Exception as e:
                    logger.warning(f"Unable to parse citations cache, starting fresh: {e}")
                    raw = None

            # Ensure we have a dict; if file contained null or other type, reset to {}
            if isinstance(raw, dict):
                citations = raw
            else:
                citations = {}

            # If cache already contains enough entries, return it
            try:
                if len(citations) >= len(paper_ids):
                    logger.info(f"Loaded {len(citations)} citations from cache")
                    # Track cached citations statistics
                    for paper_id, references in citations.items():
                        if paper_id in paper_ids:
                            refs = references or []
                            self.monitor.increment_stat('total_references', len(refs))
                            arxiv_references = [ref for ref in refs if isinstance(ref, dict) and 'arxiv_id' in ref]
                            self.monitor.increment_stat('successful_references', len(arxiv_references))
                            self.monitor.increment_stat('failed_references', len(refs) - len(arxiv_references))
                    return citations
                logger.info(f"Resuming: loaded {len(citations)} citations from partial cache")
                # Track partial cached citations
                for paper_id, references in citations.items():
                    refs = references or []
                    self.monitor.increment_stat('total_references', len(refs))
                    arxiv_references = [ref for ref in refs if isinstance(ref, dict) and 'arxiv_id' in ref]
                    self.monitor.increment_stat('successful_references', len(arxiv_references))
                    self.monitor.increment_stat('failed_references', len(refs) - len(arxiv_references))
            except TypeError:
                # Unexpected cache content; reset and continue
                logger.warning("Citations cache contains non-iterable entries; resetting cache")
                citations = {}
        else:
            citations = {}
        
        # Fetch remaining citations with threading
        papers_needing_citations = [pid for pid in paper_ids if pid not in citations]
        logger.info(f"Fetching {len(papers_needing_citations)} citations using {SEMANTIC_SCHOLAR_WORKERS} threads")
        
        citations_lock = threading.Lock()
        completed_count = [0]  # Use list for mutability in closure

        def _fetch_single_paper_citations(paper_id):
            try:
                references = self.semantic_scholar_client.get_paper_references(paper_id)
                # Defensive: ensure references is a list (API or cache may return None)
                if not references or not isinstance(references, list):
                    references = []

                with citations_lock:
                    citations[paper_id] = references
                    completed_count[0] += 1

                    self.monitor.sample_memory()
                    self.monitor.increment_stat('total_references', len(references))
                    arxiv_references = [ref for ref in references if isinstance(ref, dict) and 'arxiv_id' in ref]
                    self.monitor.increment_stat('successful_references', len(arxiv_references))
                    self.monitor.increment_stat('failed_references', len(references) - len(arxiv_references))

                    # Save cache periodically
                    if completed_count[0] % 100 == 0:
                        try:
                            with open(self.citations_cache_file, 'w', encoding='utf-8') as f:
                                json.dump(citations, f, indent=2)
                            logger.info(f"Progress saved: {completed_count[0]}/{len(papers_needing_citations)} citations")
                        except Exception as e:
                            logger.error(f"Failed to save citations cache: {e}")

                # Attempt per-paper incremental write
                try:
                    folder_name = format_folder_name(paper_id)
                    paper_dir = os.path.join(self.data_dir, folder_name)
                    metadata_file = os.path.join(paper_dir, 'metadata.json')
                    if os.path.exists(metadata_file) and paper_id in citations:
                        # Load cited metadata cache if available (best-effort)
                        cited_meta = {}
                        try:
                            if os.path.exists(self.cited_dates_cache_file):
                                with open(self.cited_dates_cache_file, 'r', encoding='utf-8') as cf:
                                    cited_meta = json.load(cf) or {}
                        except Exception:
                            cited_meta = {}

                        # Save references for this paper now
                        try:
                            self._save_references_for_paper(paper_id, paper_dir, citations[paper_id], cited_meta)
                        except Exception as e:
                            logger.error(f"Error saving incremental references for {paper_id}: {e}")
                except Exception as e:
                    logger.error(f"Error during per-paper incremental write check for {paper_id}: {e}")

                return True
            except Exception as e:
                logger.error(f"Failed to fetch citation for {paper_id}: {e}")
                return False
        
        # Use ThreadPoolExecutor for parallel fetching
        with ThreadPoolExecutor(max_workers=SEMANTIC_SCHOLAR_WORKERS) as executor:
            futures = {executor.submit(_fetch_single_paper_citations, pid): pid for pid in papers_needing_citations}
            
            try:
                for future in as_completed(futures):
                    paper_id = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Exception for {paper_id}: {e}")
            except KeyboardInterrupt:
                logger.warning("Cancelling citation fetching...")
                executor.shutdown(wait=False, cancel_futures=True)
                raise
        
        # Final save
        with open(self.citations_cache_file, 'w', encoding='utf-8') as f:
            json.dump(citations, f, indent=2)
        
        logger.info(f"Completed fetching {len(citations)} citations")
        return citations
    
    def update_references_json(self, metadata, citations, cited_metadata):
        """Update references.json for all papers with full citation data"""
        if not citations:
            logger.warning("No citations available, skipping references.json update")
            return
            
        logger.info("Updating references.json files...")
        
        # Ensure cited_metadata is a dict (not None)
        if cited_metadata is None:
            cited_metadata = {}
        
        updated_count = 0

        # If in-memory metadata is available, use it; otherwise scan data directory
        if metadata:
            paper_ids_iter = list(metadata.keys())
        else:
            # Scan paper folders under data_dir and derive paper_ids from metadata.json files
            paper_ids_iter = []
            try:
                for folder_name in os.listdir(self.data_dir):
                    paper_dir = os.path.join(self.data_dir, folder_name)
                    if not os.path.isdir(paper_dir):
                        continue
                    metadata_file = os.path.join(paper_dir, 'metadata.json')
                    if not os.path.exists(metadata_file):
                        continue
                    try:
                        with open(metadata_file, 'r', encoding='utf-8') as mf:
                            md = json.load(mf)
                        # Prefer explicit arxiv_id in metadata, else try to infer from folder name
                        pid = md.get('arxiv_id') or md.get('id')
                        if not pid:
                            pid = folder_name
                        paper_ids_iter.append(pid)
                    except Exception:
                        continue
            except Exception as e:
                logger.error(f"Failed to scan data directory for papers: {e}")

        for paper_id in paper_ids_iter:
            folder_name = format_folder_name(paper_id)
            paper_dir = os.path.join(self.data_dir, folder_name)
            metadata_file = os.path.join(paper_dir, 'metadata.json')

            # Only create references.json if paper was successfully downloaded (has metadata.json)
            if not os.path.exists(metadata_file):
                continue

            if paper_id in citations:
                self._save_references_for_paper(paper_id, paper_dir, citations[paper_id], cited_metadata)
                updated_count += 1
        
        logger.info(f"Updated {updated_count} references.json files")
    
    def fetch_cited_metadata(self, citations, metadata=None, batch_size: int = 100):
        """
        Fetch metadata for cited papers using arXiv API batch lookups

        Args:
            citations: Dictionary of citations from stage 2
            metadata: Optional metadata dict of main papers (used when calling update_references_json)
            flush_batch_size: Number of cited paper metadata fetched before flushing cache and updating references.json

        Returns:
            dict: Dictionary mapping arxiv_id to metadata
        """
        logger.info("=" * 80)
        logger.info("Stage 1.3: Fetching metadata for cited papers")
        logger.info("=" * 80)
        
        stage_start = time.time()

        # Check if citations is valid
        if not citations:
            logger.warning("No citations available for Stage 1.3")
            return {}

        # Check cache
        if os.path.exists(self.cited_dates_cache_file):
            logger.info(f"Loading cited paper dates from cache: {self.cited_dates_cache_file}")
            with open(self.cited_dates_cache_file, 'r', encoding='utf-8') as f:
                cited_metadata = json.load(f)
                logger.info(f"Loaded metadata for {len(cited_metadata)} cited papers from cache")
                return cited_metadata

        # Extract all unique arXiv IDs from citations (skip None/invalid entries)
        all_arxiv_ids = []
        seen = set()
        for paper_refs in citations.values():
            if not paper_refs:
                # Some cache entries may be None; skip gracefully
                continue
            for ref in paper_refs:
                if not isinstance(ref, dict):
                    continue
                aid = ref.get('arxiv_id')
                if aid and aid not in seen:
                    seen.add(aid)
                    all_arxiv_ids.append(aid)

        logger.info(f"Found {len(all_arxiv_ids)} unique arXiv IDs in citations")

        cited_metadata = {}
        total_fetched = 0

        # Fetch in batches using arXiv API (max 100 per batch)
        for i in range(0, len(all_arxiv_ids), 100):
            batch = all_arxiv_ids[i:i+100]
            try:
                logger.info(f"Fetching arXiv metadata batch {i//100 + 1} ({len(batch)} ids)")
                batch_meta = self.arxiv_client.get_batch_metadata(batch, batch_size=100)

                # Merge and normalize
                for aid, meta in batch_meta.items():
                    if not meta:
                        continue

                    authors = self.arxiv_client.normalize_authors(meta.get('authors', []))

                    entry = {
                        'title': meta.get('title', ''),
                        'authors': authors,
                        'submission_date': meta.get('submission_date', '')
                    }

                    if aid not in cited_metadata:
                        cited_metadata[aid] = entry
                        total_fetched += 1

                # After each API batch, flush whatever we've collected so far and update references
                try:
                    with open(self.cited_dates_cache_file, 'w', encoding='utf-8') as f:
                        json.dump(cited_metadata, f, indent=2)
                    logger.info(f"Flushed cited metadata cache after API batch {i//100 + 1}: {len(cited_metadata)} items")
                except Exception as e:
                    logger.error(f"Failed to flush cited metadata cache after API batch: {e}")

                try:
                    if metadata and citations:
                        logger.info(f"Updating references.json after API batch {i//100 + 1}")
                        self.update_references_json(metadata, citations, cited_metadata)
                except Exception as e:
                    logger.error(f"Error updating references.json after API batch: {e}")

            except Exception as e:
                logger.error(f"Failed to fetch arXiv batch metadata: {e}")

        # Final save
        try:
            with open(self.cited_dates_cache_file, 'w', encoding='utf-8') as f:
                json.dump(cited_metadata, f, indent=2)
            logger.info(f"Saved cited paper metadata cache to {self.cited_dates_cache_file} ({len(cited_metadata)} items)")
        except Exception as e:
            logger.error(f"Failed to save final cited metadata cache: {e}")

        stage_time = time.time() - stage_start
        self.monitor.record_stage_time('Stage 1.3: Cited paper metadata', stage_time)

        return cited_metadata
    
    def download_and_process(self, paper_ids, metadata=None, citations=None, cited_metadata=None):
        """
        Download and process source files for papers
        
        Args:
            paper_ids: List of paper IDs to process
            metadata: Optional metadata dict (will fetch individually if not provided)
            citations: Optional citations dict
            cited_metadata: Optional cited metadata dict
        """
        logger.info("=" * 80)
        logger.info("Stage 2: Downloading and processing source files")
        logger.info("=" * 80)
        
        stage_start = time.time()
        
        # Filter papers that need processing
        papers_to_process = []
        skipped_count = 0
        
        for paper_id in paper_ids:
            folder_name = format_folder_name(paper_id)
            paper_dir = os.path.join(self.data_dir, folder_name)
            metadata_file = os.path.join(paper_dir, 'metadata.json')
            
            if os.path.exists(metadata_file):
                skipped_count += 1
            else:
                papers_to_process.append(paper_id)
        
        logger.info(f"Processing {len(papers_to_process)} papers using {MAX_WORKERS} threads ({skipped_count} already done)")
        
        processed_count = [0]
        process_lock = threading.Lock()
        
        def _process_paper_with_error_handling(paper_id):
            try:
                paper_start = time.time()
                paper_meta = metadata.get(paper_id) if metadata else None
                success = self._process_single_paper(paper_id, paper_meta, citations, cited_metadata)
                paper_time = time.time() - paper_start
                
                with process_lock:
                    self.monitor.increment_stat('total_papers')
                    if success:
                        processed_count[0] += 1
                        self.monitor.increment_stat('successful_papers')
                        self.monitor.record_paper_time(paper_id, paper_time)
                    else:
                        self.monitor.increment_stat('failed_papers')
                    
                    # Log progress every 50 papers
                    total_done = processed_count[0] + skipped_count
                    if total_done % 50 == 0:
                        logger.info(f"Progress: {total_done}/{len(paper_ids)} papers ({processed_count[0]} processed, {skipped_count} skipped)")
                
                return success
            except Exception as e:
                logger.error(f"Exception processing {paper_id}: {e}")
                return False
        
        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_process_paper_with_error_handling, pid): pid for pid in papers_to_process}
            
            try:
                for future in as_completed(futures):
                    paper_id = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Fatal exception for {paper_id}: {e}")
            except KeyboardInterrupt:
                logger.warning("Cancelling paper downloads...")
                executor.shutdown(wait=False, cancel_futures=True)
                raise
        
        logger.info(f"Stage 2 completed: {processed_count[0]} processed, {skipped_count} skipped")
        
        # Update references.json for skipped papers (only if they have metadata.json)
        if citations and cited_metadata is not None:
            logger.info("Updating references.json for skipped papers...")
            skipped_updated = 0
            for paper_id in paper_ids:
                folder_name = format_folder_name(paper_id)
                paper_dir = os.path.join(self.data_dir, folder_name)
                metadata_file = os.path.join(paper_dir, 'metadata.json')
                references_file = os.path.join(paper_dir, 'references.json')
                
                # Only create references.json if paper was successfully downloaded (has metadata.json)
                if os.path.exists(metadata_file) and not os.path.exists(references_file):
                    if paper_id in citations:
                        self._save_references_for_paper(paper_id, paper_dir, citations[paper_id], cited_metadata)
                        skipped_updated += 1
            
            if skipped_updated > 0:
                logger.info(f"Created references.json for {skipped_updated} skipped papers")
        
        stage_time = time.time() - stage_start
        self.monitor.record_stage_time('Stage 2: Download and processing', stage_time)
    
    def _process_single_paper(self, paper_id, paper_metadata=None, citations=None, cited_metadata=None):
        """
        Process a single paper: download ALL versions, extract, clean, organize
        
        Note: Downloads all versions (v1, v2, v3...) by trying until 404.
              Uses citations/references from the LATEST version only.
        
        Args:
            paper_id: arXiv ID (without version suffix)
            paper_metadata: Paper metadata dictionary (optional, will fetch if None)
            citations: Optional citations dict for this paper (from latest version)
            cited_metadata: Optional cited paper metadata
        """
        paper_start = time.time()
        
        # Create paper directory
        folder_name = format_folder_name(paper_id)
        paper_dir = os.path.join(self.data_dir, folder_name)
        os.makedirs(paper_dir, exist_ok=True)
        
        tex_dir = os.path.join(paper_dir, 'tex')
        temp_dir = os.path.join(self.cache_dir, f"temp_{folder_name}")
        
        try:
            # Try downloading versions blindly: v1, v2, v3,... until 404
            version = 1
            downloaded_versions = []
            
            while version <= 20:  # Safety limit
                version_id = f"{paper_id}v{version}"
                logger.info(f"Attempting to download {version_id}")
                tar_path = self.arxiv_client.download_source_version(version_id, self.cache_dir)
                
                if not tar_path or not os.path.exists(tar_path):
                    # Version doesn't exist — we've reached the end.
                    if downloaded_versions:
                        logger.info(f"Found {len(downloaded_versions)} version(s) for {paper_id}")
                    else:
                        logger.info(f"No downloadable source found for {paper_id} (v1 missing)")
                    break
                
                downloaded_versions.append((version, version_id, tar_path))
                version += 1
            
            logger.info(f"Successfully downloaded {len(downloaded_versions)} version(s) for {paper_id}")
            
            # Process each downloaded version
            for version_num, version_id, tar_path in downloaded_versions:
                # Extract to version-specific temp directory
                version_temp_dir = os.path.join(self.cache_dir, f"temp_{folder_name}_{version_num}")
                os.makedirs(version_temp_dir, exist_ok=True)
                
                if not self.file_processor.extract_tarball(tar_path, version_temp_dir):
                    logger.warning(f"Failed to extract {version_id}, skipping this version")
                    if os.path.exists(tar_path):
                        os.remove(tar_path)
                    continue
                
                # Get size after extraction (before removing figures)
                size_before = self.file_processor.get_directory_size(version_temp_dir)
                self.monitor.increment_stat('total_size_bytes', size_before)
                
                # Remove figures
                files_removed, bytes_saved = self.file_processor.remove_figures(version_temp_dir)
                
                # Get size after removing figures
                size_after_figures = self.file_processor.get_directory_size(version_temp_dir)
                self.monitor.increment_stat('total_size_after_cleanup', size_after_figures)
                
                # Create version subdirectory in tex/ (e.g., tex/2402-10011v1/, tex/2402-10011v2/)
                version_tex_dir = os.path.join(tex_dir, f"{folder_name}v{version_num}")
                
                # Copy tex and bib files to version subdirectory (preserving structure and filenames)
                self.file_processor.copy_tex_and_bib_files(
                    version_temp_dir,
                    version_tex_dir,
                    skip_large_bib=self.skip_large_bib,
                    bib_size_threshold=self.bib_size_threshold
                )
                
                # Cleanup version temp dir and tar file
                self.file_processor.cleanup_temp_dir(version_temp_dir)
                if os.path.exists(tar_path):
                    os.remove(tar_path)
            
            # Check if at least one version was downloaded
            if not os.path.exists(tex_dir) or not os.listdir(tex_dir):
                logger.info(f"Skipping paper {paper_id}: no downloadable source (skipping by default)")
                try:
                    self.monitor.increment_stat('skipped_papers', 1)
                except Exception:
                    pass
                return False
            
            # Fetch and save metadata if not provided
            if paper_metadata is None:
                paper_metadata = self.arxiv_client.get_paper_metadata(paper_id)
                if not paper_metadata:
                    logger.warning(f"Failed to fetch metadata for {paper_id}")
                    paper_metadata = {'arxiv_id': paper_id, 'title': 'Unknown', 'authors': []}
            
            # Save metadata
            metadata_output = os.path.join(paper_dir, 'metadata.json')
            with open(metadata_output, 'w', encoding='utf-8') as f:
                json.dump(paper_metadata, f, indent=2)
            # Record metadata file written
            try:
                self.monitor.incr_error('metadata_files_written', 1)
            except Exception:
                pass
            
            # Attempt to write references immediately after metadata is saved.
            try:
                citations_to_use = citations or {}
                cited_meta_to_use = cited_metadata or {}

                # Best-effort: load citations cache if we don't have in-memory citations
                if not citations_to_use or paper_id not in citations_to_use:
                    try:
                        if os.path.exists(self.citations_cache_file):
                            with open(self.citations_cache_file, 'r', encoding='utf-8') as cf:
                                loaded_cits = json.load(cf) or {}
                                if isinstance(loaded_cits, dict):
                                    citations_to_use = loaded_cits
                    except Exception:
                        citations_to_use = citations_to_use or {}

                # Best-effort: load cited metadata cache if absent
                if not cited_meta_to_use:
                    try:
                        if os.path.exists(self.cited_dates_cache_file):
                            with open(self.cited_dates_cache_file, 'r', encoding='utf-8') as df:
                                loaded_cited = json.load(df) or {}
                                if isinstance(loaded_cited, dict):
                                    cited_meta_to_use = loaded_cited
                    except Exception:
                        cited_meta_to_use = cited_meta_to_use or {}

                if citations_to_use and paper_id in citations_to_use:
                    self._save_references_for_paper(paper_id, paper_dir, citations_to_use[paper_id], cited_meta_to_use)
                    logger.info(f"Incremental references.json written for {paper_id} after metadata save")
            except Exception as e:
                logger.error(f"Failed to write incremental references for {paper_id}: {e}")
            
            logger.info(f"Successfully processed {paper_id} in {time.time() - paper_start:.2f}s")
            return True
            
        except Exception as e:
            logger.error(f"Error processing {paper_id}: {e}")
            self.file_processor.cleanup_temp_dir(temp_dir)
            return False
    
    def _save_references_for_paper(self, paper_id, paper_dir, refs, cited_metadata):
        """
        Save references.json for a single paper
        
        Args:
            paper_id: arXiv ID
            paper_dir: Paper directory path
            refs: List of references for this paper
            cited_metadata: Cited paper metadata dict
        """
        references_dict = {}
        
        for ref in refs:
            if 'arxiv_id' in ref:
                ref_id = ref['arxiv_id']
                ref_folder_name = format_folder_name(ref_id)
                
                # Build standardized ref metadata via arXiv client (prefer cited metadata)
                cited_entry = cited_metadata.get(ref_id) if cited_metadata and ref_id in cited_metadata else None
                ref_meta = self.arxiv_client.build_reference_metadata(ref, cited_entry=cited_entry)
                
                references_dict[ref_folder_name] = ref_meta
        
        # Save references.json (with change-detection to avoid unnecessary rewrites)
        ref_output = os.path.join(paper_dir, 'references.json')
        try:
            # If a references file already exists, read and compare to avoid rewriting
            should_write = True
            if os.path.exists(ref_output):
                try:
                    with open(ref_output, 'r', encoding='utf-8') as rf:
                        existing = json.load(rf)
                    if existing == references_dict:
                        should_write = False
                except Exception:
                    # If we can't read/parse existing file, proceed to overwrite
                    should_write = True

            if not should_write:
                try:
                    self.monitor.incr_error('references_files_skipped', 1)
                except Exception:
                    pass
                logger.debug(f"SKIP writing references.json for {paper_id}: content unchanged")
                return

            # Write atomically to avoid partial files: write to temp then replace
            tmp_output = ref_output + '.tmp'
            with open(tmp_output, 'w', encoding='utf-8') as f:
                json.dump(references_dict, f, indent=2)
                try:
                    f.flush()
                    os.fsync(f.fileno())
                except Exception:
                    pass

            # Replace (atomic on most platforms)
            try:
                os.replace(tmp_output, ref_output)
            except Exception:
                # Fallback to rename
                try:
                    os.remove(ref_output)
                except Exception:
                    pass
                os.rename(tmp_output, ref_output)

            # Record references file written
            try:
                self.monitor.incr_error('references_files_written', 1)
            except Exception:
                pass

            # Debug: log absolute path, existence, and size
            try:
                abs_path = os.path.abspath(ref_output)
                exists = os.path.exists(abs_path)
                size = os.path.getsize(abs_path) if exists else 0
                logger.info(f"WROTE references.json -> {abs_path} (exists={exists}, size={size} bytes)")
            except Exception as e:
                logger.error(f"Failed to stat references.json after write: {e}")
        except Exception as e:
            logger.error(f"Failed to write references.json for {paper_id} at {ref_output}: {e}")
    
    def run(self):
        """Run the complete scraping pipeline with parallel stages"""
        logger.info("=" * 80)
        logger.info(f"Starting arXiv scraper for student {self.student_id}")
        logger.info("=" * 80)
        
        self.monitor.start()
        
        try:
            paper_ids = self.generate_paper_ids()
            
            # Limit papers if in test mode
            if self.max_papers:
                paper_ids = paper_ids[:self.max_papers]
                logger.info(f"TEST MODE: Limited to {len(paper_ids)} papers")
            
            logger.info(f"Will process {len(paper_ids)} papers")
            
            # Run 4 stages in parallel using separate threads
            logger.info("Launching parallel stages: metadata, download, citations, cited_metadata...")
            
            metadata = {}
            citations = {}
            cited_metadata = {}
            stage_errors = []
            
            def stage1_metadata():
                try:
                    logger.info("Stage 1.1: Fetching metadata...")
                    nonlocal metadata
                    metadata = self.fetch_metadata(paper_ids)
                except Exception as e:
                    logger.error(f"Stage 1.1 failed: {e}")
                    stage_errors.append(('metadata', e))
            
            def stage2_download():
                try:
                    logger.info("Stage 2: Downloading and processing source files...")
                    self.download_and_process(paper_ids, metadata=None, citations=None, cited_metadata=None)
                except Exception as e:
                    logger.error(f"Stage 2 failed: {e}")
                    stage_errors.append(('download', e))
            
            def stage3_citations():
                nonlocal citations, cited_metadata
                try:
                    logger.info("Stage 1.2: Fetching citations...")
                    citations = self.fetch_citations(paper_ids)
                    
                    # After citations are fetched, fetch cited metadata
                    if citations:
                        logger.info("Stage 1.3: Fetching cited paper metadata...")
                        cited_metadata = self.fetch_cited_metadata(citations, metadata)
                    else:
                        logger.warning("No citations available, skipping Stage 1.3")
                        cited_metadata = {}
                except Exception as e:
                    logger.error(f"Stage 1.2/1.3 failed: {e}")
                    stage_errors.append(('citations', e))
            
            # Launch all 3 stages in parallel (Stage 1.3 runs inside Stage 1.2 thread)
            import threading
            threads = [
                threading.Thread(target=stage1_metadata, name="Stage-Metadata"),
                threading.Thread(target=stage2_download, name="Stage-Download"),
                threading.Thread(target=stage3_citations, name="Stage-Citations-CitedMetadata")
            ]
            
            for thread in threads:
                thread.start()
            
            # Wait for all stages to complete
            for thread in threads:
                thread.join()
            
            if stage_errors:
                logger.error(f"Some stages failed: {stage_errors}")
            
            # Update references.json files with full citation data
            if metadata and citations:
                logger.info("Updating references.json with citation metadata...")
                self.update_references_json(metadata, citations, cited_metadata)
            else:
                logger.warning("Cannot update references.json: missing metadata or citations")
            
            logger.info("=" * 80)
            logger.info("Scraping completed successfully!")
            logger.info("=" * 80)
            
        except KeyboardInterrupt:
            logger.warning("Scraping interrupted by user (Ctrl+C)")
            logger.info("Progress has been saved to cache. Run again to resume.")
        except Exception as e:
            logger.error(f"Fatal error during scraping: {e}", exc_info=True)
        
        finally:
            self.monitor.stop()
            
            # Save performance report
            try:
                final_bytes = self.file_processor.get_directory_size(self.data_dir)
                self.monitor.set_final_output_bytes(final_bytes)
                # also update disk peak if larger
                self.monitor.record_disk_peak(final_bytes)
            except Exception:
                final_bytes = 0

            report = self.monitor.get_summary_dict()
            report_file = os.path.join(self.data_dir, 'performance_report.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Performance report saved to {report_file}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='ArXiv scraper runner')
    parser.add_argument('student_id', help='Student ID used for output folders')
    parser.add_argument('max_papers', nargs='?', type=int, default=None, help='(Optional) limit number of papers (test mode)')
    parser.add_argument('-p', '--paper', dest='paper_id', help='Process a single paper by arXiv id (e.g. 2402.10011)')
    # By default we skip large .bib files; provide flag to disable that behavior
    parser.set_defaults(skip_large_bib=True)
    parser.add_argument('--no-skip-large-bib', dest='skip_large_bib', action='store_false', help='Do not skip copying .bib files larger than threshold')
    parser.add_argument('--bib-threshold-mb', dest='bib_threshold_mb', type=float, default=5.0, help='Threshold in MB for skipping .bib files (default: 5)')

    parser.set_defaults(skip_no_source=True)
    args = parser.parse_args()

    student_id = args.student_id
    max_papers = args.max_papers

    # Convert threshold MB to bytes
    bib_threshold_bytes = int(args.bib_threshold_mb * 1024 * 1024)
    scraper = ArxivScraper(student_id, max_papers=max_papers, skip_large_bib=args.skip_large_bib, bib_size_threshold=bib_threshold_bytes, skip_missing_source=args.skip_no_source)

    if args.paper_id:
        # Run single-paper processing (minimal run)
        scraper.monitor.start()
        try:
            # Fetch citations for this single paper so references.json can be created
            logger.info(f"Fetching citations for single paper {args.paper_id}")
            citations = scraper.fetch_citations([args.paper_id])

            # Fetch cited metadata (arXiv batch lookups / optional local cache) if we have citations
            cited_metadata = {}
            if citations and args.paper_id in citations:
                cited_metadata = scraper.fetch_cited_metadata(citations, metadata={})

            success = scraper._process_single_paper(args.paper_id, paper_metadata=None, citations=citations, cited_metadata=cited_metadata)
            if not success:
                logger.error(f"Single-paper processing failed for {args.paper_id}")
                sys.exit(2)
        except KeyboardInterrupt:
            logger.warning("Single-paper run interrupted by user")
            sys.exit(130)
        finally:
            scraper.monitor.stop()
            # Save a minimal performance report
            report = scraper.monitor.get_summary_dict()
            report_file = os.path.join(scraper.data_dir, 'performance_report_single.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Performance report saved to {report_file}")

        return

    scraper.run()

if __name__ == '__main__':
    main()
