"""
Performance monitoring utilities
"""
import time
import psutil
import os
from logger import setup_logger

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
        
        # Statistics
        self.stats = {
            'total_papers': 0,
            'successful_papers': 0,
            'failed_papers': 0,
            'total_references': 0,
            'successful_references': 0,
            'failed_references': 0,
            'total_size_bytes': 0,
            'total_size_after_cleanup': 0,
            'stage_times': {},
            'paper_times': []
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
        logger.info(f"Stage '{stage_name}' completed in {duration:.2f} seconds")
    
    def record_paper_time(self, paper_id, duration):
        """Record time to process a single paper"""
        self.stats['paper_times'].append(duration)
    
    def increment_stat(self, stat_name, value=1):
        """Increment a statistic"""
        if stat_name in self.stats:
            self.stats[stat_name] += value
    
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
        
        # Paper statistics
        success_rate = (self.stats['successful_papers'] / self.stats['total_papers'] * 100) if self.stats['total_papers'] > 0 else 0
        logger.info(f"\nPaper Statistics:")
        logger.info(f"  Total papers: {self.stats['total_papers']}")
        logger.info(f"  Successful: {self.stats['successful_papers']}")
        logger.info(f"  Failed: {self.stats['failed_papers']}")
        logger.info(f"  Success rate: {success_rate:.2f}%")
        
        if self.stats['paper_times']:
            avg_paper_time = sum(self.stats['paper_times']) / len(self.stats['paper_times'])
            logger.info(f"  Average time per paper: {avg_paper_time:.2f} seconds")
        
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
        
        # Storage statistics
        if self.stats['total_size_bytes'] > 0:
            size_before_mb = self.stats['total_size_bytes'] / (1024 * 1024)
            size_after_mb = self.stats['total_size_after_cleanup'] / (1024 * 1024)
            reduction = ((self.stats['total_size_bytes'] - self.stats['total_size_after_cleanup']) / self.stats['total_size_bytes'] * 100)
            
            logger.info(f"\nStorage Statistics:")
            logger.info(f"  Total size before cleanup: {size_before_mb:.2f} MB")
            logger.info(f"  Total size after cleanup: {size_after_mb:.2f} MB")
            logger.info(f"  Size reduction: {reduction:.2f}%")
            
            if self.stats['successful_papers'] > 0:
                avg_size_before = size_before_mb / self.stats['successful_papers']
                avg_size_after = size_after_mb / self.stats['successful_papers']
                logger.info(f"  Average paper size before: {avg_size_before:.2f} MB")
                logger.info(f"  Average paper size after: {avg_size_after:.2f} MB")
        
        # Stage times
        if self.stats['stage_times']:
            logger.info(f"\nStage Execution Times:")
            for stage, duration in self.stats['stage_times'].items():
                percentage = (duration / total_time * 100) if total_time > 0 else 0
                logger.info(f"  {stage}: {duration:.2f}s ({percentage:.2f}%)")
        
        logger.info("=" * 80)
        
        return self.stats
    
    def get_summary_dict(self):
        """Get performance summary as dictionary"""
        total_time = (self.end_time - self.start_time) if self.end_time else 0
        avg_memory = sum(self.memory_samples) / len(self.memory_samples) if self.memory_samples else 0
        
        return {
            'total_time_seconds': total_time,
            'total_time_minutes': total_time / 60,
            'initial_memory_mb': self.initial_memory,
            'peak_memory_mb': self.peak_memory,
            'average_memory_mb': avg_memory,
            'statistics': self.stats
        }
