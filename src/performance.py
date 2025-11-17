"""
Performance monitoring utilities
"""
import time
import psutil
import os, json
from logger import setup_logger
from pathlib import Path

logger = setup_logger(__name__)

class PerformanceMonitor:
    """Monitor performance metrics during scraping"""
    
    def __init__(self):
        self.start_time: float = 0.0
        self.end_time: float = 0.0
        self.process = psutil.Process(os.getpid())
        self.initial_memory = None
        self.peak_memory = 0
        self.memory_samples = []
        self.disk_peak_bytes = 0
        self.final_output_bytes = 0
        self.paper_times = {}
        # Per-paper per-stage durations: {paper_id: { 'metadata': float, 'processing': float, 'references': float }}
        self.paper_stage_times = {}

        # Statistics
        self.stats = {
            'total_papers': 0,
            'successful_papers': 0,
            'failed_papers': 0,
            'total_references': 0,
            'successful_references': 0,
            'failed_references': 0,
            'stage_times': {},
            'stage_memory_peak': {},
            'references_files_written': 0,
            'metadata_files_written': 0,
        }
    
    def start(self):
        """Start monitoring"""
        self.start_time = time.time()
        self.initial_memory = self.process.memory_info().rss / (1024 * 1024)  # MB
        logger.info(f"Performance monitoring started. Initial memory: {self.initial_memory:.2f} MB")
    
    def sample_memory(self):
        """Sample current memory usage"""
        current_memory = self.process.memory_info().rss / (1024 * 1024)  # MB
        self.memory_samples.append(current_memory)
        if current_memory > self.peak_memory:
            self.peak_memory = current_memory
    
    def record_stage_time(self, stage_name, duration):
        """Record time for a specific stage"""
        self.stats['stage_times'][stage_name] = duration
        self.sample_memory()
        cur_mem = self.process.memory_info().rss / (1024 * 1024)
        prev = self.stats['stage_memory_peak'].get(stage_name, 0)
        if cur_mem > prev:
            self.stats['stage_memory_peak'][stage_name] = cur_mem

        logger.info(f"Stage '{stage_name}' completed in {duration:.2f} seconds; memory={cur_mem:.2f} MB")


    def record_disk_peak(self, bytes_used: int):
        try:
            if bytes_used and bytes_used > self.disk_peak_bytes:
                self.disk_peak_bytes = bytes_used
        except Exception:
            pass

    def set_final_output_bytes(self, bytes_used: int):
        try:
            self.final_output_bytes = int(bytes_used or 0)
            # ensure disk peak at least final size
            if self.final_output_bytes > self.disk_peak_bytes:
                self.disk_peak_bytes = self.final_output_bytes
        except Exception:
            pass

    def incr_error(self, key: str, count: int = 1):
        if key in self.stats:
            self.stats[key] += count
    
    def increment_stat(self, stat_name, value=1):
        """Increment a statistic"""
        if stat_name in self.stats:
            self.stats[stat_name] += value

    def record_paper_time(self, paper_id: str, duration: float):
        """Record time taken to process a single paper (seconds)."""
        try:
            if not paper_id:
                return
            self.paper_times[str(paper_id)] = float(duration or 0.0)
        except Exception:
            pass

    def record_paper_stage_duration(self, paper_id: str, stage: str, duration: float):
        """Record duration (seconds) for a specific stage of a paper.

        Stages are user-defined but we use: 'metadata', 'processing', 'references'.
        When a 'references' duration is recorded we compute the total and populate
        `paper_times[paper_id]` as the sum of available stage durations.
        """
        try:
            if not paper_id or not stage:
                return
            pid = str(paper_id)
            stg = str(stage)
            self.paper_stage_times.setdefault(pid, {})
            self.paper_stage_times[pid][stg] = float(duration or 0.0)

            # Compute total from recorded stages and store in paper_times
            total = sum(self.paper_stage_times[pid].values())
            self.paper_times[pid] = float(total)
        except Exception:
            pass
    
    def stop(self):
        """Stop monitoring and calculate final metrics"""
        self.end_time = time.time()
        self.sample_memory()
        
        total_time = self.end_time - self.start_time
        avg_memory = sum(self.memory_samples) / len(self.memory_samples) if self.memory_samples else 0
        
        logger.info("=" * 80)
        logger.info("PERFORMANCE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        logger.info(f"Initial memory: {self.initial_memory:.2f} MB")
        logger.info(f"Peak memory: {self.peak_memory:.2f} MB")
        logger.info(f"Average memory: {avg_memory:.2f} MB")
        # Disk statistics
        try:
            peak_mb = self.disk_peak_bytes / (1024 * 1024)
            final_mb = self.final_output_bytes / (1024 * 1024)
            logger.info(f"Peak disk usage: {peak_mb:.2f} MB")
            logger.info(f"Final output size: {final_mb:.2f} MB")
        except Exception:
            pass
        
        # Paper statistics
        success_rate = (self.stats['successful_papers'] / self.stats['total_papers'] * 100) if self.stats['total_papers'] > 0 else 0
        logger.info(f"\nPaper Statistics:")
        logger.info(f"  Total papers: {self.stats['total_papers']}")
        logger.info(f"  Successful: {self.stats['successful_papers']}")
        logger.info(f"  Failed (including skipped): {self.stats['failed_papers']}")
        logger.info(f"  Success rate: {success_rate:.2f}%")
        
        # Reference statistics
        ref_success_rate = (self.stats['successful_references'] / self.stats['total_references'] * 100) if self.stats['total_references'] > 0 else 0
        logger.info(f"\nReference Statistics:")
        logger.info(f"  Total references: {self.stats['total_references']}")
        logger.info(f"  Successful: {self.stats['successful_references']}")
        logger.info(f"  Failed: {self.stats['failed_references']}")
        logger.info(f"  Success rate: {ref_success_rate:.2f}%")

        if self.stats['successful_papers'] > 0:
            avg_refs = self.stats['total_references'] / self.stats['successful_papers']
            logger.info(f"  Average references per paper: {avg_refs:.2f}")

        # Capture per-paper recorded times.
        try:
            if self.paper_times:
                pts = list(self.paper_times.values())
                avg_pt = sum(pts) / len(pts)
                logger.info(f"\nPer-paper timings collected: {len(pts)} entries; avg per-paper time={avg_pt:.2f}s")
            else:
                logger.info("\nPer-paper timings: none recorded")
        except Exception:
            pass
        # Average
        try:
            if self.paper_times:
                pts = list(self.paper_times.values())
                avg_pt = sum(pts) / len(pts)
                logger.info(f"Per-paper timings collected: {len(pts)} entries; avg per-paper time={avg_pt:.2f}s")
        except Exception:
            pass
        
        # Stage times
        if self.stats['stage_times']:
            logger.info(f"\nStage Execution Times:")
            for stage, duration in self.stats['stage_times'].items():
                percentage = (duration / total_time * 100) if total_time > 0 else 0
                logger.info(f"  {stage}: {duration:.2f}s ({percentage:.2f}%)")
            # Stage memory peaks
            if self.stats.get('stage_memory_peak'):
                logger.info("\nStage Memory Peaks (MB):")
                for stage, mem in self.stats['stage_memory_peak'].items():
                    logger.info(f"  {stage}: {mem:.2f} MB")
        
        logger.info("=" * 80)
        
        # Return a flattened summary (merge statistics into top-level)
        return self.get_summary_dict()
    
    def get_summary_dict(self):
        """Get performance summary as dictionary"""
        total_time = (self.end_time - self.start_time) if self.end_time else 0
        avg_memory = sum(self.memory_samples) / len(self.memory_samples) if self.memory_samples else 0
        summary = {
            'total_time_seconds': total_time,
            'total_time_minutes': total_time / 60,
            'initial_memory_mb': self.initial_memory,
            'peak_memory_mb': self.peak_memory,
            'average_memory_mb': avg_memory,
        }
        max_disk_bytes = int(self.disk_peak_bytes or 0)
        final_bytes = int(self.final_output_bytes or 0)

        avg_paper_time = (sum(self.paper_times.values()) / len(self.paper_times)) if self.paper_times else 0

        summary.update({
            'max_disk_bytes': max_disk_bytes,
            'max_disk_mb': max_disk_bytes / (1024 * 1024) if max_disk_bytes else 0,
            'final_output_bytes': final_bytes,
            'final_output_mb': final_bytes / (1024 * 1024) if final_bytes else 0,
            'paper_times': self.paper_times,
            'avg_paper_time_seconds': avg_paper_time,
        })

        summary.update(self.stats)
        return summary

    def compute_stats_from_data_dir(self, data_dir_path: str):
        """Recompute several counters from on-disk files under `data_dir_path`.

        This scans per-paper folders and updates statistics such as:
        - total_papers
        - metadata_files_written
        - references_files_written
        - total_references

        It is safe to call this before `stop()` so that logged summaries reflect
        the actual files on disk rather than incremental counters that may
        have been missed during parallel execution.
        """
        data_dir = Path(data_dir_path)
        if not data_dir.exists() or not data_dir.is_dir():
            return

        paper_dirs = [d for d in data_dir.iterdir() if d.is_dir()]

        total_papers = len(paper_dirs)
        metadata_files = 0
        references_files = 0
        total_references = 0
        papers_with_source = 0

        for p in paper_dirs:
            if (p / 'metadata.json').exists():
                metadata_files += 1
            if (p / 'references.json').exists():
                references_files += 1
                try:
                    with open(p / 'references.json', 'r', encoding='utf-8') as rf:
                        refs = json.load(rf)
                        if isinstance(refs, dict):
                            total_references += len(refs)
                except Exception:
                    pass

            # detect source presence under `tex/` (any .tex or other non-placeholder file)
            tex_dir = p / 'tex'
            has_source = False
            if tex_dir.exists():
                for root, dirs, files in os.walk(tex_dir):
                    for fn in files:
                        if fn.lower().endswith('.tex'):
                            has_source = True
                            break
                        if fn != 'NO_SOURCE_AVAILABLE.txt':
                            has_source = True
                            break
                    if has_source:
                        break
            if has_source:
                papers_with_source += 1

        skipped_papers = total_papers - papers_with_source

        # Update stats
        self.stats['total_papers'] = total_papers
        failed_existing = self.stats.get('failed_papers', 0)
        failed_total = failed_existing + skipped_papers
        self.stats['failed_papers'] = failed_total
        successful = max(0, total_papers - failed_total)
        self.stats['successful_papers'] = successful
        self.stats['metadata_files_written'] = metadata_files
        self.stats['references_files_written'] = references_files
        runtime_total = self.stats.get('successful_references', 0) + self.stats.get('failed_references', 0)
        if runtime_total > 0:
            self.stats['total_references'] = runtime_total
        else:
            self.stats['total_references'] = total_references