"""
Main scraper orchestrator
"""
import os
import json
import re
import time
import threading
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    get_assigned_range, format_arxiv_id, format_folder_name,
    DATA_DIR, CACHE_DIR, MAX_WORKERS, SEMANTIC_SCHOLAR_WORKERS, SEMANTIC_SCHOLAR_API_KEY
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
        self.semantic_scholar_client = SemanticScholarClient(api_key=SEMANTIC_SCHOLAR_API_KEY, monitor=self.monitor)
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
        self.references_cache_file = os.path.join(self.cache_dir, f"{self.student_id}_references.json")
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
        """Entry Discovery (Stage 1)"""
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
        
        batch_size = 100
        for i in range(0, len(remaining_papers), batch_size):
            batch = remaining_papers[i:i+batch_size]
            logger.info(f"Fetching batch {i//batch_size + 1}/{(len(remaining_papers)-1)//batch_size + 1}")
            
            batch_metadata = self.arxiv_client.get_batch_metadata(batch, batch_size=batch_size)
            metadata.update(batch_metadata)
            
            # Save cache after each batch
            with open(self.metadata_cache_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            logger.info(f"Progress saved: {len(metadata)}/{len(paper_ids)} papers")
            # Also write per-paper metadata.json files so metadata stage is independent
            try:
                self.arxiv_client.write_metadata_files(batch_metadata, self.data_dir)
            except Exception as e:
                logger.warning(f"Failed to write per-paper metadata files for batch: {e}")
        
        
        logger.info(f"Metadata complete: {len(metadata)} total papers")
        return metadata
    
    def fetch_references(self, paper_ids):
        # Load cache if present
        if os.path.exists(self.references_cache_file):
            try:
                with open(self.references_cache_file, 'r', encoding='utf-8') as f:
                    references = json.load(f) or {}
                if not isinstance(references, dict):
                    logger.warning("References cache malformed, starting fresh")
                    references = {}
                else:
                    logger.info(f"Loaded {len(references)} references from cache")
            except Exception as e:
                logger.warning(f"Unable to read references cache, starting fresh: {e}")
                references = {}
        else:
            references = {}

        # Determine which papers need fetching
        papers_to_fetch = [pid for pid in paper_ids if pid not in references]
        logger.info(f"Fetching references for {len(papers_to_fetch)} papers using {SEMANTIC_SCHOLAR_WORKERS} threads")

        refs_lock = threading.Lock()
        completed = [0]

        def _fetch_and_write(pid):
            try:
                refs = self.semantic_scholar_client.get_paper_references(pid)
                if not isinstance(refs, dict):
                    refs = {}

                # Update cache
                with refs_lock:
                    references[pid] = refs
                    completed[0] += 1
                    # monitor stats
                    try:
                        # Count how many returned references have arXiv IDs (we only write those)
                        arxiv_re = re.compile(r'^\d{4}\.\d+')
                        arxiv_count = 0
                        non_arxiv_count = 0
                        if isinstance(refs, dict):
                            for k in refs.keys():
                                if isinstance(k, str) and arxiv_re.match(k):
                                    arxiv_count += 1
                                else:
                                    non_arxiv_count += 1

                        self.monitor.increment_stat('successful_references', arxiv_count)
                        self.monitor.increment_stat('failed_references', non_arxiv_count)
                        self.monitor.increment_stat('total_references', arxiv_count + non_arxiv_count)
                    except Exception:
                        pass

                    # Periodically persist cache
                    if completed[0] % 50 == 0:
                        try:
                            with open(self.references_cache_file, 'w', encoding='utf-8') as cf:
                                json.dump(references, cf, indent=2)
                            logger.info(f"Saved progress for {completed[0]} papers")
                        except Exception as e:
                            logger.error(f"Failed to save references cache: {e}")

                # Write per-paper references.json using SemanticScholarClient helper
                try:
                    folder_name = format_folder_name(pid)
                    paper_dir = os.path.join(self.data_dir, folder_name)
                    # Delegate actual writing to the semantic client helper
                    try:
                        written = self.semantic_scholar_client.write_references_json(pid, paper_dir)
                        if written:
                            logger.info(f"Wrote references.json for {pid} via SemanticScholarClient")
                    except Exception as e:
                        logger.error(f"SemanticScholarClient failed to write references.json for {pid}: {e}")

                except Exception as e:
                    logger.error(f"Failed during per-paper write prep for {pid}: {e}")

                return True
            except Exception as e:
                logger.error(f"Failed to fetch references for {pid}: {e}")
                return False

        # Parallel fetch
        if papers_to_fetch:
            with ThreadPoolExecutor(max_workers=SEMANTIC_SCHOLAR_WORKERS) as executor:
                futures = {executor.submit(_fetch_and_write, pid): pid for pid in papers_to_fetch}
                try:
                    for fut in as_completed(futures):
                        pid = futures[fut]
                        try:
                            ok = fut.result()
                            # if fetch returned False, count as failed reference fetch
                            if ok is False:
                                try:
                                    self.monitor.increment_stat('failed_references')
                                except Exception:
                                    pass
                        except Exception as e:
                            logger.error(f"Error fetching references for {pid}: {e}")
                            # Count a failed reference fetch for this paper
                            try:
                                self.monitor.increment_stat('failed_references')
                            except Exception:
                                pass
                except KeyboardInterrupt:
                    logger.warning("Cancelling reference fetching...")
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise

        # Final cache persist
        try:
            with open(self.references_cache_file, 'w', encoding='utf-8') as cf:
                json.dump(references, cf, indent=2)
        except Exception as e:
            logger.error(f"Failed to write final references cache: {e}")

        logger.info(f"Completed fetching references for {len(references)} papers")
        return references
    

    def download_and_process(self, paper_ids):
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

        # Decide which papers to download: for independence, process all provided IDs
        papers_to_process = list(paper_ids)

        logger.info(f"Downloading and processing {len(papers_to_process)} papers using {MAX_WORKERS} threads")

        processed = [0]
        lock = threading.Lock()

        def _download_process_one(pid):
            folder_name = format_folder_name(pid)
            paper_dir = os.path.join(self.data_dir, folder_name)
            os.makedirs(paper_dir, exist_ok=True)

            temp_dir = os.path.join(paper_dir, 'tmp_download')
            try:
                # Download all available versions sequentially (v1..vN)
                downloads = self.arxiv_client.download_all_versions(pid, save_dir=temp_dir, max_versions=20)

                if not downloads:
                    logger.warning(f"No source versions downloaded for {pid}")
                    # Create placeholder versioned folder with marker
                    try:
                        versioned_subfolder = pid
                        versioned_folder_name = format_folder_name(versioned_subfolder)
                        dest_tex_dir = os.path.join(paper_dir, 'tex', versioned_folder_name)
                        os.makedirs(dest_tex_dir, exist_ok=True)
                        marker = os.path.join(dest_tex_dir, 'NO_SOURCE_AVAILABLE.txt')
                        with open(marker, 'w', encoding='utf-8') as mf:
                            mf.write('Full source archive not available from arXiv for this paper.\n')
                        logger.info(f"Created placeholder tex folder for {pid} at {dest_tex_dir}")
                    except Exception:
                        logger.warning(f"Failed to create placeholder tex folder for {pid}")

                    if self.skip_missing_source:
                        return 'skipped'
                    else:
                        return False

                # For each downloaded version, extract and copy files into versioned subfolder
                for downloaded, version_tag in downloads:
                    extract_dir = None
                    try:
                        extract_dir = os.path.join(temp_dir, f'extracted_{version_tag or "nov"}')
                        extracted_ok = self.file_processor.extract_archive(downloaded, extract_dir)
                        if not extracted_ok:
                            logger.warning(f"Extraction failed for {pid} {version_tag}, keeping downloaded file for inspection")

                        try:
                            versioned_subfolder = pid
                            if version_tag:
                                versioned_subfolder = f"{pid}{version_tag}"
                            versioned_folder_name = format_folder_name(versioned_subfolder)
                            dest_tex_dir = os.path.join(paper_dir, 'tex', versioned_folder_name)
                            copied = self.file_processor.copy_tex_and_bib_files(extract_dir, dest_tex_dir, skip_large_bib=self.skip_large_bib, bib_size_threshold=self.bib_size_threshold)
                        except Exception as e:
                            logger.warning(f"Failed to copy tex/bib for {pid} {version_tag}: {e}")

                        try:
                            self.file_processor.remove_figures(extract_dir)
                        except Exception:
                            pass

                    finally:
                        try:
                            self.file_processor.cleanup_temp_dir(extract_dir)
                        except Exception:
                            pass

                return True
            finally:
                try:
                    self.file_processor.cleanup_temp_dir(temp_dir)
                except Exception:
                    pass

        # Parallel execution
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_download_process_one, pid): pid for pid in papers_to_process}
            try:
                for fut in as_completed(futures):
                    pid = futures[fut]
                    try:
                        ok = fut.result()
                        with lock:
                            self.monitor.increment_stat('total_papers')
                            if ok is True:
                                processed[0] += 1
                                self.monitor.increment_stat('successful_papers')
                            elif ok == 'skipped':
                                processed[0] += 1
                                self.monitor.increment_stat('skipped_papers')
                            else:
                                self.monitor.increment_stat('failed_papers')
                    except Exception as e:
                        logger.error(f"Error processing {pid}: {e}")
                        try:
                            with lock:
                                self.monitor.increment_stat('total_papers')
                                self.monitor.increment_stat('failed_papers')
                        except Exception:
                            pass
            except KeyboardInterrupt:
                logger.warning("Cancelling downloads...")
                executor.shutdown(wait=False, cancel_futures=True)
                raise

        logger.info(f"Stage 2 completed: {processed[0]} processed")

        stage_time = time.time() - stage_start
        try:
            self.monitor.record_stage_time('Stage 2: Download and processing', stage_time)
        except Exception:
            pass

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
            
            
            metadata = {}
            references = {}
            stage_errors = []
            
            def stage1_metadata():
                start = time.time()
                try:
                    logger.info("Stage 1.1: Fetching metadata...")
                    nonlocal metadata
                    metadata = self.fetch_metadata(paper_ids)
                except Exception as e:
                    logger.error(f"Stage 1.1 failed: {e}")
                    stage_errors.append(('metadata', e))
                finally:
                    try:
                        self.monitor.record_stage_time('Stage 1: Metadata', time.time() - start)
                    except Exception:
                        pass
            
            def stage2_download():
                try:
                    logger.info("Stage 2: Downloading and processing source files...")
                    self.download_and_process(paper_ids)
                except Exception as e:
                    logger.error(f"Stage 2 failed: {e}")
                    stage_errors.append(('download', e))
            
            def stage3_references():
                nonlocal references
                start = time.time()
                try:
                    logger.info("Stage 3: Fetching references...")
                    references = self.fetch_references(paper_ids)
                    # We don't currently have a fetch_cited_metadata implementation here;
                    # leave cited_metadata empty (the semantic client writes per-paper references.json).
                    cited_metadata = {}
                except Exception as e:
                    logger.error(f"Stage 3 failed: {e}")
                    stage_errors.append(('references', e))
                finally:
                    try:
                        self.monitor.record_stage_time('Stage 3: References', time.time() - start)
                    except Exception:
                        pass
            
            # Launch all 3 stages in parallel (Stage 1.3 runs inside Stage 1.2 thread)
            import threading
            threads = [
                threading.Thread(target=stage1_metadata, name="Stage-Metadata"),
                threading.Thread(target=stage2_download, name="Stage-Download"),
                threading.Thread(target=stage3_references, name="Stage-References")
            ]
            
            for thread in threads:
                thread.start()
            
            # Wait for all stages to complete
            for thread in threads:
                thread.join()
            # After all stages complete
            if stage_errors:
                logger.error(f"Some stages failed: {stage_errors}")

            # Metadata and references should have been written by their respective stages
            if metadata and references:
                logger.info("Metadata and references fetched; per-paper files should be present.")
            else:
                logger.warning("Metadata or references missing after fetch stages")

            logger.info("=" * 80)
            logger.info("Scraping completed successfully!")
            logger.info("=" * 80)

        except KeyboardInterrupt:
            logger.warning("Scraping interrupted by user (Ctrl+C)")
            logger.info("Progress has been saved to cache. Run again to resume.")
        except Exception as e:
            logger.error(f"Fatal error during scraping: {e}", exc_info=True)
        
        finally:
            # Recompute on-disk stats so the monitor reflects actual files produced
            try:
                final_bytes = self.file_processor.get_directory_size(self.data_dir)
            except Exception:
                final_bytes = 0

            try:
                # Update monitor counters from the data directory before stopping
                self.monitor.compute_stats_from_data_dir(self.data_dir)
            except Exception:
                pass

            try:
                self.monitor.set_final_output_bytes(final_bytes)
                # also update disk peak if larger
                self.monitor.record_disk_peak(final_bytes)
            except Exception:
                pass

            # Stop monitor (will log summary using updated counters)
            try:
                self.monitor.stop()
            except Exception:
                pass

            # Save performance report
            report = self.monitor.get_summary_dict()
            report_file = os.path.join(self.data_dir, 'performance_report.json')
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Performance report saved to {report_file}")

    def process_single_paper(self, paper_id):
        """Helper to process a single paper by delegating to download_and_process."""
        try:
            self.download_and_process([paper_id])
            return True
        except Exception as e:
            logger.error(f"_process_single_paper failed for {paper_id}: {e}")
            return False

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
            # Fetch references for this single paper so references.json can be created
            logger.info(f"Fetching references for single paper {args.paper_id}")
            citations = scraper.fetch_references([args.paper_id])
            cited_metadata = {}

            success = scraper.process_single_paper(args.paper_id)
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
