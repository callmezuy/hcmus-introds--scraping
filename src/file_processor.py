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
    
    def __init__(self):
        pass
    
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
    
    def find_bib_files(self, directory):
        """
        Find all .bib files in directory
        
        Args:
            directory: Directory to search
            
        Returns:
            list: List of .bib file paths
        """
        bib_files = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.bib'):
                    bib_files.append(os.path.join(root, file))
        return bib_files
    
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
    
    def copy_tex_files(self, source_dir, dest_dir):
        """
        Copy all .tex and .bib files from source to destination, preserving directory structure
        
        Args:
            source_dir: Source directory
            dest_dir: Destination directory
            
        Returns:
            int: Number of files copied
        """
        os.makedirs(dest_dir, exist_ok=True)
        tex_files = self.find_tex_files(source_dir)
        bib_files = self.find_bib_files(source_dir)
        all_files = tex_files + bib_files
        
        copied = 0
        for file_path in all_files:
            try:
                # Preserve relative path from source_dir
                rel_path = os.path.relpath(file_path, source_dir)
                dest_path = os.path.join(dest_dir, rel_path)
                
                # Create subdirectories if needed
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                
                shutil.copy2(file_path, dest_path)
                copied += 1
            except Exception as e:
                logger.warning(f"Failed to copy {file_path}: {e}")
        
        logger.info(f"Copied {copied} .tex and .bib files to {dest_dir}")
        return copied
    
    def merge_bib_files(self, source_dir, output_file, append=False):
        """
        Merge all .bib files into one
        
        Args:
            source_dir: Source directory
            output_file: Output .bib file path
            append: If True, append to existing file instead of overwriting
            
        Returns:
            int: Number of .bib files merged
        """
        bib_files = self.find_bib_files(source_dir)
        
        if not bib_files:
            logger.warning(f"No .bib files found in {source_dir}")
            return 0
        
        try:
            mode = 'a' if append else 'w'
            with open(output_file, mode, encoding='utf-8', errors='ignore') as outf:
                for bib_file in bib_files:
                    try:
                        with open(bib_file, 'r', encoding='utf-8', errors='ignore') as inf:
                            content = inf.read()
                            outf.write(content)
                            outf.write('\n\n')
                    except Exception as e:
                        logger.warning(f"Failed to read {bib_file}: {e}")
            
            logger.info(f"Merged {len(bib_files)} .bib files into {output_file}")
            return len(bib_files)
        
        except Exception as e:
            logger.error(f"Failed to create merged .bib file: {e}")
            return 0
    
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
