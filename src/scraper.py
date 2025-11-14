"""
Main scraper orchestrator
"""
import os
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    get_assigned_range, format_arxiv_id, format_folder_name,
    DATA_DIR, CACHE_DIR, KAGGLE_METADATA_PATH, MAX_WORKERS
)
from logger import setup_logger
from performance import PerformanceMonitor
from arxiv_client import ArxivClient
from semantic_scholar_client import SemanticScholarClient
from kaggle_handler import KaggleMetadataHandler
from file_processor import FileProcessor

logger = setup_logger(__name__)

class ArxivScraper:
    """Main scraper class orchestrating the entire pipeline"""
    
    def __init__(self, student_id, max_papers=None):
        """
        Initialize scraper
        
        Args:
            student_id: Student ID for getting assigned range
            max_papers: Maximum number of papers to process (None for all)
        """
        self.student_id = str(student_id)
        self.max_papers = max_papers
        self.arxiv_client = ArxivClient()
        self.semantic_scholar_client = SemanticScholarClient(api_key="9JcwvT4mJ39GR0cF7ntcB34Qg2pCJSS614DhOP2y")
        self.kaggle_handler = KaggleMetadataHandler(KAGGLE_METADATA_PATH)
        self.file_processor = FileProcessor()
        self.monitor = PerformanceMonitor()
        
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
        # Check cache
        if os.path.exists(self.citations_cache_file):
            with open(self.citations_cache_file, 'r', encoding='utf-8') as f:
                citations = json.load(f)
            if len(citations) >= len(paper_ids):
                logger.info(f"Loaded {len(citations)} citations from cache")
                # Track cached citations statistics
                for paper_id, references in citations.items():
                    if paper_id in paper_ids:
                        self.monitor.increment_stat('total_references', len(references))
                        arxiv_references = [ref for ref in references if 'arxiv_id' in ref]
                        self.monitor.increment_stat('successful_references', len(arxiv_references))
                        self.monitor.increment_stat('failed_references', len(references) - len(arxiv_references))
                return citations
            logger.info(f"Resuming: loaded {len(citations)} citations from partial cache")
            # Track partial cached citations
            for paper_id, references in citations.items():
                self.monitor.increment_stat('total_references', len(references))
                arxiv_references = [ref for ref in references if 'arxiv_id' in ref]
                self.monitor.increment_stat('successful_references', len(arxiv_references))
                self.monitor.increment_stat('failed_references', len(references) - len(arxiv_references))
        else:
            citations = {}
        
        # Fetch remaining citations with threading
        papers_needing_citations = [pid for pid in paper_ids if pid not in citations]
        logger.info(f"Fetching {len(papers_needing_citations)} citations using {MAX_WORKERS} threads")
        
        citations_lock = threading.Lock()
        completed_count = [0]  # Use list for mutability in closure
        
        def fetch_single_citation(paper_id):
            try:
                references = self.semantic_scholar_client.get_paper_references(paper_id)
                
                with citations_lock:
                    citations[paper_id] = references
                    completed_count[0] += 1
                    
                    self.monitor.sample_memory()
                    self.monitor.increment_stat('total_references', len(references))
                    arxiv_references = [ref for ref in references if 'arxiv_id' in ref]
                    self.monitor.increment_stat('successful_references', len(arxiv_references))
                    self.monitor.increment_stat('failed_references', len(references) - len(arxiv_references))
                    
                    # Save cache periodically
                    if completed_count[0] % 100 == 0:
                        with open(self.citations_cache_file, 'w', encoding='utf-8') as f:
                            json.dump(citations, f, indent=2)
                        logger.info(f"Progress saved: {completed_count[0]}/{len(papers_needing_citations)} citations")
                
                return True
            except Exception as e:
                logger.error(f"Failed to fetch citation for {paper_id}: {e}")
                return False
        
        # Use ThreadPoolExecutor for parallel fetching
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_single_citation, pid): pid for pid in papers_needing_citations}
            
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
        logger.info("Updating references.json files...")
        
        updated_count = 0
        for paper_id in metadata.keys():
            folder_name = format_folder_name(paper_id)
            paper_dir = os.path.join(self.data_dir, folder_name)
            
            if not os.path.exists(paper_dir):
                continue
            
            if paper_id in citations:
                self._save_references_for_paper(paper_id, paper_dir, citations[paper_id], cited_metadata)
                updated_count += 1
        
        logger.info(f"Updated {updated_count} references.json files")
    
    def fetch_cited_metadata(self, citations):
        """
        Fetch metadata for cited papers using Kaggle dataset
        
        Args:
            citations: Dictionary of citations from stage 2
            
        Returns:
            dict: Dictionary mapping arxiv_id to metadata
        """
        logger.info("=" * 80)
        logger.info("STAGE 1.3: Fetching metadata for cited papers")
        logger.info("=" * 80)
        
        stage_start = time.time()
        
        # Check cache
        if os.path.exists(self.cited_dates_cache_file):
            logger.info(f"Loading cited paper dates from cache: {self.cited_dates_cache_file}")
            with open(self.cited_dates_cache_file, 'r', encoding='utf-8') as f:
                cited_metadata = json.load(f)
                logger.info(f"Loaded metadata for {len(cited_metadata)} cited papers from cache")
                return cited_metadata
        
        # Check if Kaggle file exists
        if not self.kaggle_handler.check_file_exists():
            logger.warning("Skipping Stage 1.3: Kaggle metadata file not available")
            logger.info("References will be saved without publication dates")
            return {}
        
        # Extract all unique arXiv IDs from citations
        all_arxiv_ids = set()
        for paper_refs in citations.values():
            for ref in paper_refs:
                if 'arxiv_id' in ref:
                    all_arxiv_ids.add(ref['arxiv_id'])
        
        logger.info(f"Found {len(all_arxiv_ids)} unique arXiv IDs in citations")
        
        # Fetch metadata from Kaggle dataset
        cited_metadata = self.kaggle_handler.load_metadata_for_ids(list(all_arxiv_ids))
        
        # Save to cache
        with open(self.cited_dates_cache_file, 'w', encoding='utf-8') as f:
            json.dump(cited_metadata, f, indent=2)
        logger.info(f"Saved cited paper metadata cache to {self.cited_dates_cache_file}")
        
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
        logger.info("STAGE 2: Downloading and processing source files")
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
        
        def process_paper_wrapper(paper_id):
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
            futures = {executor.submit(process_paper_wrapper, pid): pid for pid in papers_to_process}
            
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
                    # Version doesn't exist, we've reached the end
                    if version == 1:
                        logger.error(f"Failed to download even v1 for {paper_id}")
                        return False
                    else:
                        logger.info(f"Found {len(downloaded_versions)} version(s) for {paper_id}")
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
                self.file_processor.copy_tex_files(version_temp_dir, version_tex_dir)
                
                # Cleanup version temp dir and tar file
                self.file_processor.cleanup_temp_dir(version_temp_dir)
                if os.path.exists(tar_path):
                    os.remove(tar_path)
            
            # Check if at least one version was downloaded
            if not os.path.exists(tex_dir) or not os.listdir(tex_dir):
                logger.error(f"Failed to download any version for {paper_id}")
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
            
            # Save references.json if citations provided
            if citations and paper_id in citations:
                self._save_references_for_paper(paper_id, paper_dir, citations[paper_id], cited_metadata)
            
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
                
                # Get metadata from cited_metadata or use basic info
                if cited_metadata and ref_id in cited_metadata:
                    ref_meta = cited_metadata[ref_id]
                else:
                    # Fallback to what we have from Semantic Scholar
                    ref_meta = {
                        'arxiv_id': ref_id,
                        'title': ref.get('title', ''),
                        'authors': ref.get('authors', []),
                        'submission_date': ref.get('publication_date', ''),
                        'semantic_scholar_id': ref.get('semantic_scholar_id', '')
                    }
                
                references_dict[ref_folder_name] = ref_meta
        
        # Save references.json
        ref_output = os.path.join(paper_dir, 'references.json')
        with open(ref_output, 'w', encoding='utf-8') as f:
            json.dump(references_dict, f, indent=2)
    
    def save_references_json(self, metadata, citations, cited_metadata):
        """
        Save references.json for each paper
        
        Args:
            metadata: Main paper metadata
            citations: Citations dictionary
            cited_metadata: Cited paper metadata
        """
        logger.info("Saving references.json files...")
        
        for paper_id, refs in citations.items():
            folder_name = format_folder_name(paper_id)
            paper_dir = os.path.join(self.data_dir, folder_name)
            
            if not os.path.exists(paper_dir):
                continue
            
            references_dict = {}
            
            for ref in refs:
                if 'arxiv_id' in ref:
                    ref_id = ref['arxiv_id']
                    ref_folder_name = format_folder_name(ref_id)
                    
                    # Get metadata from cited_metadata or use basic info
                    if ref_id in cited_metadata:
                        ref_meta = cited_metadata[ref_id]
                    else:
                        # Fallback to what we have from Semantic Scholar
                        ref_meta = {
                            'arxiv_id': ref_id,
                            'title': ref.get('title', ''),
                            'authors': ref.get('authors', []),
                            'submission_date': ref.get('publication_date', ''),
                            'revised_dates': []
                        }
                    
                    references_dict[ref_folder_name] = ref_meta
            
            # Save references.json
            ref_output = os.path.join(paper_dir, 'references.json')
            with open(ref_output, 'w', encoding='utf-8') as f:
                json.dump(references_dict, f, indent=2)
        
        logger.info("Finished saving references.json files")
    
    def run(self):
        """Run the complete scraping pipeline with parallel stages"""
        logger.info("=" * 80)
        logger.info(f"Starting arXiv scraper for student {self.student_id}")
        logger.info("=" * 80)
        
        self.monitor.start()
        
        try:
            # Generate paper IDs
            paper_ids = self.generate_paper_ids()
            
            # Limit papers if in test mode
            if self.max_papers:
                paper_ids = paper_ids[:self.max_papers]
                logger.info(f"TEST MODE: Limited to {len(paper_ids)} papers")
            
            logger.info(f"Will process {len(paper_ids)} papers")
            
            # Run 3 stages in parallel using separate threads
            logger.info("Launching parallel stages: metadata, download, citations...")
            
            metadata = {}
            citations = {}
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
                try:
                    logger.info("Stage 1.2: Fetching citations...")
                    nonlocal citations
                    citations = self.fetch_citations(paper_ids)
                except Exception as e:
                    logger.error(f"Stage 1.2 failed: {e}")
                    stage_errors.append(('citations', e))
            
            # Launch all 3 stages in parallel
            import threading
            threads = [
                threading.Thread(target=stage1_metadata, name="Stage-Metadata"),
                threading.Thread(target=stage2_download, name="Stage-Download"),
                threading.Thread(target=stage3_citations, name="Stage-Citations")
            ]
            
            for thread in threads:
                thread.start()
            
            # Wait for all stages to complete
            for thread in threads:
                thread.join()
            
            if stage_errors:
                logger.error(f"Some stages failed: {stage_errors}")
            
            # Stage 1.3: Fetch cited paper metadata
            cited_metadata = self.fetch_cited_metadata(citations)
            
            # Update references.json files with full citation data
            logger.info("Updating references.json with citation metadata...")
            self.update_references_json(metadata, citations, cited_metadata)
            
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
            report = self.monitor.get_summary_dict()
            report_file = os.path.join(self.data_dir, 'performance_report.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Performance report saved to {report_file}")

def main():
    """Main entry point"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python scraper.py <student_id> [max_papers]")
        print("Example: python scraper.py 23127XXX")
        print("Example (test mode): python scraper.py 23127XXX 3")
        sys.exit(1)
    
    student_id = sys.argv[1]
    max_papers = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    scraper = ArxivScraper(student_id, max_papers=max_papers)
    scraper.run()

if __name__ == '__main__':
    main()
