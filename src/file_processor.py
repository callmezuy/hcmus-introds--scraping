"""
File processor for extracting and cleaning arXiv source files
"""
import os
import tarfile
import zipfile
import gzip
import shutil
from pathlib import Path
from typing import BinaryIO, cast
from logger import setup_logger

logger = setup_logger(__name__)

class FileProcessor:
    """Processor for arXiv source files"""

    def __init__(self, monitor=None):
        self.monitor = monitor

    def extract_archive(self, archive_path, extract_dir):
        """
        Extract various archive/file types into `extract_dir`.

        Supported types:
        - tar.gz / .tar
        - .zip
        - single-file gzipped (.tex.gz, .bib.gz)
        - plain .tex or .bib files (copied into extract_dir)

        Args:
            archive_path: Path to downloaded file
            extract_dir: Directory to extract to

        Returns:
            bool: True if extraction/copy succeeded and extract_dir contains files
        """
        try:
            os.makedirs(extract_dir, exist_ok=True)

            p = Path(archive_path)

            # tar files
            if tarfile.is_tarfile(archive_path):
                with tarfile.open(archive_path, 'r:*') as tar:
                    tar.extractall(path=extract_dir)
                logger.info(f"Extracted tar archive {archive_path} to {extract_dir}")
                return True

            # zip files
            if zipfile.is_zipfile(archive_path):
                with zipfile.ZipFile(archive_path, 'r') as zf:
                    zf.extractall(path=extract_dir)
                logger.info(f"Extracted zip archive {archive_path} to {extract_dir}")
                return True

            # gzipped single file (.tex.gz or .bib.gz) or tar.gz
            if p.suffix == '.gz':
                # Read magic bytes to decide real content type
                try:
                    with open(archive_path, 'rb') as fh:
                        magic = fh.read(8)
                except Exception as e:
                    logger.warning(f"Failed to read file header for {archive_path}: {e}")
                    magic = b''

                # PDF files sometimes get misnamed with .tar.gz; detect and copy as PDF
                if magic.startswith(b'%PDF'):
                    try:
                        dest = os.path.join(extract_dir, p.name.replace('.tar.gz', '.pdf'))
                        shutil.copy2(archive_path, dest)
                        logger.info(f"Saved PDF (mislabelled) {archive_path} to {dest}")
                        return True
                    except Exception as e:
                        logger.warning(f"Failed to copy mislabelled PDF {archive_path}: {e}")

                # HTML/error pages
                if magic.lstrip().startswith(b'<'):
                    try:
                        dest = os.path.join(extract_dir, p.name + '.html')
                        shutil.copy2(archive_path, dest)
                        logger.warning(f"Saved HTML/error page from {archive_path} to {dest}")
                        return False
                    except Exception as e:
                        logger.warning(f"Failed to save HTML from {archive_path}: {e}")

                # Check for gzip magic header (0x1f 0x8b)
                if magic.startswith(b'\x1f\x8b'):
                    try:
                        # Decompress to a temporary path (remove .gz)
                        decompressed_name = p.stem  # removes only last .gz
                        decompressed_path = os.path.join(extract_dir, decompressed_name)
                        with gzip.open(archive_path, 'rb') as f_in_raw, open(decompressed_path, 'wb') as f_out:
                            f_in = cast(BinaryIO, f_in_raw)
                            shutil.copyfileobj(f_in, f_out)
                        # If decompressed result is a tar, extract it
                        if tarfile.is_tarfile(decompressed_path):
                            with tarfile.open(decompressed_path, 'r:*') as tar:
                                tar.extractall(path=extract_dir)
                            logger.info(f"Decompressed and extracted tar from {archive_path} to {extract_dir}")
                            try:
                                os.remove(decompressed_path)
                            except Exception:
                                pass
                            return True
                        else:
                            # If not a tar, keep decompressed file (e.g., .tex.gz -> .tex)
                            logger.info(f"Decompressed {archive_path} to {decompressed_path}")
                            return True
                    except Exception as e:
                        logger.warning(f"Failed to decompress gz file {archive_path}: {e}")
                else:
                    # Not a gzip file despite .gz extension
                    logger.warning(f"File {archive_path} has .gz extension but is not gzip; header={magic[:8]!r}")

            # plain .tex or .bib files - copy into extract_dir
            if p.suffix in {'.tex', '.bib'}:
                try:
                    dest = os.path.join(extract_dir, p.name)
                    shutil.copy2(archive_path, dest)
                    logger.info(f"Copied single source file {archive_path} to {dest}")
                    return True
                except Exception as e:
                    logger.warning(f"Failed to copy single source file {archive_path}: {e}")

            # Unknown/unsupported format: log and return False (caller may decide to keep/remove)
            logger.warning(f"Unsupported archive/file type for extraction: {archive_path}")
            return False

        except Exception as e:
            logger.error(f"Failed to extract {archive_path}: {e}")
            if os.path.exists(archive_path):
                try:
                    os.remove(archive_path)
                    logger.info(f"Deleted corrupted file: {archive_path}")
                except Exception:
                    pass
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
