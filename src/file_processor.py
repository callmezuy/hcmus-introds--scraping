"""
File processor for extracting and cleaning arXiv source files
"""
import os
import tarfile
import shutil
from logger import setup_logger

logger = setup_logger(__name__)

class FileProcessor:
    """Processor for arXiv source files"""

    def __init__(self, monitor=None):
        self.monitor = monitor
    
    def extract_tarball(self, tar_path, extract_dir):
        """
        Extract tar.gz file
        
        Args:
            tar_path: Path to .tar.gz file
            extract_dir: Directory to extract to
            
        Returns:
            bool: True if successful
        """
        try:
            # Validate it's a gzip file
            if not tarfile.is_tarfile(tar_path):
                logger.error(f"Not a valid tar file: {tar_path}")
                # Delete invalid file
                if os.path.exists(tar_path):
                    os.remove(tar_path)
                    logger.info(f"Deleted invalid file: {tar_path}")
                return False
            
            with tarfile.open(tar_path, 'r:gz') as tar:
                tar.extractall(path=extract_dir)
            logger.info(f"Extracted {tar_path} to {extract_dir}")
            return True
        except Exception as e:
            logger.error(f"Failed to extract {tar_path}: {e}")
            # Delete corrupted file
            if os.path.exists(tar_path):
                os.remove(tar_path)
                logger.info(f"Deleted corrupted file: {tar_path}")
            if self.monitor:
                try:
                    self.monitor.incr_error('extraction_failures', 1)
                except Exception:
                    pass
            return False
    
    def find_tex_files(self, directory):
        """
        Find all .tex files in directory
        
        Args:
            directory: Directory to search
            
        Returns:
            list: List of .tex file paths
        """
        tex_files = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.tex'):
                    tex_files.append(os.path.join(root, file))
        return tex_files
    
    def remove_figures(self, directory):
        """
        Remove figure files to reduce size
        
        Args:
            directory: Directory to clean
            
        Returns:
            tuple: (files_removed, bytes_saved)
        """
        figure_extensions = ['.png', '.jpg', '.jpeg', '.pdf', '.eps', '.ps', '.svg', '.gif', '.tif', '.tiff']
        files_removed = 0
        bytes_saved = 0
        
        for root, dirs, files in os.walk(directory):
            for file in files:
                if any(file.lower().endswith(ext) for ext in figure_extensions):
                    file_path = os.path.join(root, file)
                    try:
                        file_size = os.path.getsize(file_path)
                        os.remove(file_path)
                        files_removed += 1
                        bytes_saved += file_size
                    except Exception as e:
                        logger.warning(f"Failed to remove {file_path}: {e}")
        
        if files_removed > 0:
            logger.info(f"Removed {files_removed} figure files, saved {bytes_saved / (1024*1024):.2f} MB")
        
        return files_removed, bytes_saved
    
    def get_directory_size(self, directory):
        """
        Calculate total size of directory
        
        Args:
            directory: Directory path
            
        Returns:
            int: Size in bytes
        """
        total_size = 0
        for root, dirs, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    total_size += os.path.getsize(file_path)
                except Exception:
                    pass
        return total_size
    
    def copy_tex_and_bib_files(self, source_dir, dest_dir, skip_large_bib=True, bib_size_threshold=5*1024*1024):
        """
        Copy all .tex and .bib files from source to destination, preserving directory structure
        
        Args:
            source_dir: Source directory
            dest_dir: Destination directory
            skip_large_bib: If True, skip copying .bib files larger than bib_size_threshold bytes
            bib_size_threshold: size in bytes above which .bib files are skipped (default 5MB)

        Returns:
            int: Number of files copied
        """
        os.makedirs(dest_dir, exist_ok=True)
        
        # Find all .tex and .bib files
        all_files = []
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                if file.endswith('.tex') or file.endswith('.bib'):
                    all_files.append(os.path.join(root, file))
        
        copied = 0
        skipped_bib = 0
        for file_path in all_files:
            try:
                # Preserve relative path from source_dir
                rel_path = os.path.relpath(file_path, source_dir)
                dest_path = os.path.join(dest_dir, rel_path)
                
                # Create subdirectories if needed
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)

                # Optionally skip large .bib files
                if skip_large_bib and file_path.endswith('.bib'):
                    try:
                        size = os.path.getsize(file_path)
                        if size > bib_size_threshold:
                            skipped_bib += 1
                            logger.info(f"Skipping large .bib file (> {bib_size_threshold} bytes): {file_path}")
                            continue
                    except Exception:
                        # If size check fails, fall back to copying
                        pass

                shutil.copy2(file_path, dest_path)
                copied += 1
            except Exception as e:
                logger.warning(f"Failed to copy {file_path}: {e}")
        
        if skipped_bib > 0:
            logger.info(f"Skipped {skipped_bib} large .bib files")
            if self.monitor:
                try:
                    self.monitor.incr_error('skipped_bib_count', skipped_bib)
                except Exception:
                    pass
        logger.info(f"Copied {copied} .tex and .bib files to {dest_dir}")
        return copied
    
    def cleanup_temp_dir(self, directory):
        """
        Remove temporary directory
        
        Args:
            directory: Directory to remove
        """
        try:
            if os.path.exists(directory):
                shutil.rmtree(directory)
                logger.debug(f"Cleaned up temporary directory: {directory}")
        except Exception as e:
            logger.warning(f"Failed to cleanup {directory}: {e}")
