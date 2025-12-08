"""
Enhanced file operations utilities with backup, verification, and safety features.
"""

import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config_manager import ConfigManager
from .logging_setup import get_logger

logger = get_logger(__name__)


class FileOperationError(Exception):
    """Exception raised during file operations."""
    pass


class FileOperations:
    """Enhanced file operations with backup, verification, and rollback capabilities."""
    
    def __init__(
        self,
        config_manager: ConfigManager,
        backup_enabled: bool = True,
        verification_enabled: bool = True
    ):
        """
        Initialize file operations handler.
        
        Args:
            config_manager: Configuration manager instance
            backup_enabled: Whether to create backups before operations
            verification_enabled: Whether to verify file operations
        """
        self.config_manager = config_manager
        self.backup_config = config_manager.get_backup_config()
        self.backup_enabled = backup_enabled and self.backup_config['enabled']
        self.verification_enabled = verification_enabled
        
        # Initialize backup directory
        if self.backup_enabled:
            self.backup_dir = Path(self.backup_config['backup_dir'])
            self.backup_dir.mkdir(exist_ok=True)
            self._cleanup_old_backups()
        
        # Operation log for rollback
        self.operation_log: List[Dict[str, Any]] = []
        self._log_lock = threading.Lock()
        
        logger.info(f"File operations initialized (backup: {self.backup_enabled}, verification: {verification_enabled})")
    
    def _cleanup_old_backups(self) -> None:
        """Clean up old backup directories."""
        try:
            backup_dirs = [
                d for d in self.backup_dir.iterdir()
                if d.is_dir() and d.name.startswith('backup_')
            ]
            
            # Sort by creation time
            backup_dirs.sort(key=lambda x: x.stat().st_ctime)
            
            # Remove old backups
            max_backups = self.backup_config['max_backups']
            while len(backup_dirs) >= max_backups:
                old_backup = backup_dirs.pop(0)
                shutil.rmtree(old_backup)
                logger.info(f"Removed old backup: {old_backup}")
                
        except Exception as e:
            logger.warning(f"Failed to cleanup old backups: {e}")
    
    def _create_backup_entry(self, operation: str, source: Path, destination: Optional[Path] = None) -> str:
        """
        Create a backup entry for an operation.
        
        Args:
            operation: Type of operation (move, copy, delete)
            source: Source file path
            destination: Destination file path (for move/copy operations)
            
        Returns:
            Backup ID
        """
        if not self.backup_enabled:
            return ""
        
        backup_id = str(uuid.uuid4())
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        operation_backup_dir = self.backup_dir / f"backup_{timestamp}_{backup_id}"
        operation_backup_dir.mkdir(exist_ok=True)
        
        try:
            # Create backup of source file
            if source.exists():
                backup_file = operation_backup_dir / source.name
                shutil.copy2(source, backup_file)
                logger.debug(f"Created backup: {backup_file}")
            
            # Log the operation
            operation_info = {
                'backup_id': backup_id,
                'operation': operation,
                'source': str(source),
                'destination': str(destination) if destination else None,
                'timestamp': timestamp,
                'backup_dir': str(operation_backup_dir)
            }
            
            with self._log_lock:
                self.operation_log.append(operation_info)
            
            # Save operation log
            log_file = operation_backup_dir / 'operation_log.json'
            with open(log_file, 'w') as f:
                json.dump(operation_info, f, indent=2)
            
            return backup_id
            
        except Exception as e:
            logger.error(f"Failed to create backup for {source}: {e}")
            return ""
    
    def _verify_file_integrity(self, file_path: Path, expected_hash: Optional[str] = None) -> bool:
        """
        Verify file integrity using hash comparison.
        
        Args:
            file_path: Path to file to verify
            expected_hash: Expected MD5 hash (if None, just check existence)
            
        Returns:
            True if file is valid
        """
        if not self.verification_enabled:
            return True
        
        try:
            if not file_path.exists():
                return False
            
            if expected_hash is None:
                return True
            
            # Calculate MD5 hash
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            
            calculated_hash = hash_md5.hexdigest()
            return calculated_hash == expected_hash
            
        except Exception as e:
            logger.error(f"Failed to verify file integrity for {file_path}: {e}")
            return False
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of a file."""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Failed to calculate hash for {file_path}: {e}")
            return ""
    
    def safe_move(
        self,
        source: Path,
        destination: Path,
        preserve_name: bool = False,
        handle_duplicates: str = "uuid"  # "uuid", "skip", "overwrite"
    ) -> bool:
        """
        Safely move a file with backup and verification.
        
        Args:
            source: Source file path
            destination: Destination directory or file path
            preserve_name: Whether to preserve original filename
            handle_duplicates: How to handle duplicate files
            
        Returns:
            True if operation succeeded
        """
        try:
            if not source.exists():
                logger.error(f"Source file does not exist: {source}")
                return False
            
            # Determine final destination path
            if destination.is_dir() or not destination.suffix:
                if preserve_name:
                    final_dest = destination / source.name
                else:
                    # Generate UUID-based filename to prevent conflicts
                    extension = source.suffix
                    unique_name = f"{uuid.uuid4()}{extension}"
                    final_dest = destination / unique_name
            else:
                final_dest = destination
            
            # Create destination directory if it doesn't exist
            final_dest.parent.mkdir(parents=True, exist_ok=True)
            
            # Handle duplicate files
            if final_dest.exists():
                if handle_duplicates == "skip":
                    logger.info(f"Skipping duplicate file: {final_dest}")
                    return True
                elif handle_duplicates == "uuid":
                    # Generate new unique name
                    stem = final_dest.stem
                    extension = final_dest.suffix
                    unique_name = f"{stem}_{uuid.uuid4()}{extension}"
                    final_dest = final_dest.parent / unique_name
                elif handle_duplicates == "overwrite":
                    pass  # Will overwrite
                else:
                    logger.error(f"Unknown duplicate handling strategy: {handle_duplicates}")
                    return False
            
            # Calculate source hash for verification
            source_hash = self._calculate_file_hash(source) if self.verification_enabled else None
            
            # Create backup
            backup_id = self._create_backup_entry("move", source, final_dest)
            
            # Perform move operation
            shutil.move(str(source), str(final_dest))
            
            # Verify operation
            if not self._verify_file_integrity(final_dest, source_hash):
                logger.error(f"File integrity verification failed for {final_dest}")
                # Attempt rollback
                self.rollback_operation(backup_id)
                return False
            
            logger.debug(f"Successfully moved {source} to {final_dest}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to move file from {source} to {destination}: {e}")
            return False
    
    def safe_copy(
        self,
        source: Path,
        destination: Path,
        preserve_metadata: bool = True
    ) -> bool:
        """
        Safely copy a file with verification.
        
        Args:
            source: Source file path
            destination: Destination path
            preserve_metadata: Whether to preserve file metadata
            
        Returns:
            True if operation succeeded
        """
        try:
            if not source.exists():
                logger.error(f"Source file does not exist: {source}")
                return False
            
            # Create destination directory
            destination.parent.mkdir(parents=True, exist_ok=True)
            
            # Calculate source hash
            source_hash = self._calculate_file_hash(source) if self.verification_enabled else None
            
            # Perform copy
            if preserve_metadata:
                shutil.copy2(str(source), str(destination))
            else:
                shutil.copy(str(source), str(destination))
            
            # Verify copy
            if not self._verify_file_integrity(destination, source_hash):
                logger.error(f"Copy verification failed for {destination}")
                if destination.exists():
                    destination.unlink()
                return False
            
            logger.debug(f"Successfully copied {source} to {destination}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy file from {source} to {destination}: {e}")
            return False
    
    def safe_delete(self, file_path: Path) -> bool:
        """
        Safely delete a file with backup.
        
        Args:
            file_path: Path to file to delete
            
        Returns:
            True if operation succeeded
        """
        try:
            if not file_path.exists():
                logger.warning(f"File does not exist (already deleted?): {file_path}")
                return True
            
            # Create backup
            backup_id = self._create_backup_entry("delete", file_path)
            
            # Delete file
            file_path.unlink()
            
            logger.debug(f"Successfully deleted {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete file {file_path}: {e}")
            return False
    
    def organize_file_by_date(
        self,
        source_file: Path,
        base_destination: Path,
        file_date: datetime,
        preserve_name: bool = False
    ) -> Optional[Path]:
        """
        Organize a file into year/month/day structure based on date.
        
        Args:
            source_file: Source file to organize
            base_destination: Base destination directory
            file_date: Date to use for organization
            preserve_name: Whether to preserve original filename
            
        Returns:
            Final destination path if successful, None otherwise
        """
        try:
            # Create date-based directory structure
            year_dir = base_destination / str(file_date.year)
            month_dir = year_dir / f"{file_date.month:02d}"
            day_dir = month_dir / f"{file_date.day:02d}"
            
            # Move file
            if self.safe_move(source_file, day_dir, preserve_name=preserve_name):
                # Return the actual destination path
                if preserve_name:
                    return day_dir / source_file.name
                else:
                    # Find the file that was created (with UUID name)
                    for file in day_dir.iterdir():
                        if file.is_file() and file.suffix == source_file.suffix:
                            # This is a simple heuristic - in practice you'd want
                            # more robust tracking of the actual destination
                            return file
                    return None
            else:
                return None
                
        except Exception as e:
            logger.error(f"Failed to organize file {source_file}: {e}")
            return None
    
    def batch_move_files(
        self,
        file_operations: List[Tuple[Path, Path]],
        preserve_names: bool = False,
        max_workers: int = 4
    ) -> Dict[str, int]:
        """
        Move multiple files in batches with parallel processing.
        
        Args:
            file_operations: List of (source, destination) tuples
            preserve_names: Whether to preserve original filenames
            max_workers: Maximum number of worker threads
            
        Returns:
            Dictionary with operation statistics
        """
        stats = {
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }
        
        def move_single_file(source_dest_tuple):
            source, destination = source_dest_tuple
            try:
                if self.safe_move(source, destination, preserve_name=preserve_names):
                    return 'successful'
                else:
                    return 'failed'
            except Exception as e:
                logger.error(f"Batch move failed for {source}: {e}")
                return 'failed'
        
        # Process files in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_operation = {
                executor.submit(move_single_file, operation): operation
                for operation in file_operations
            }
            
            for future in as_completed(future_to_operation):
                result = future.result()
                stats[result] += 1
        
        logger.info(f"Batch move completed: {stats}")
        return stats
    
    def rollback_operation(self, backup_id: str) -> bool:
        """
        Rollback a file operation using its backup.
        
        Args:
            backup_id: Backup ID to rollback
            
        Returns:
            True if rollback succeeded
        """
        if not self.backup_enabled:
            logger.warning("Backup not enabled, cannot rollback")
            return False
        
        try:
            # Find operation in log
            operation_info = None
            with self._log_lock:
                for op in self.operation_log:
                    if op['backup_id'] == backup_id:
                        operation_info = op
                        break
            
            if not operation_info:
                logger.error(f"Operation not found for backup ID: {backup_id}")
                return False
            
            backup_dir = Path(operation_info['backup_dir'])
            if not backup_dir.exists():
                logger.error(f"Backup directory not found: {backup_dir}")
                return False
            
            # Restore from backup
            source_path = Path(operation_info['source'])
            backup_file = backup_dir / source_path.name
            
            if backup_file.exists():
                # Restore the original file
                source_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(backup_file, source_path)
                
                # If this was a move operation, remove the destination
                if operation_info['operation'] == 'move' and operation_info['destination']:
                    dest_path = Path(operation_info['destination'])
                    if dest_path.exists():
                        dest_path.unlink()
                
                logger.info(f"Successfully rolled back operation {backup_id}")
                return True
            else:
                logger.error(f"Backup file not found: {backup_file}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to rollback operation {backup_id}: {e}")
            return False
    
    def get_directory_size(self, directory: Path) -> int:
        """
        Get total size of a directory in bytes.
        
        Args:
            directory: Directory to measure
            
        Returns:
            Total size in bytes
        """
        total_size = 0
        try:
            for dirpath, dirnames, filenames in os.walk(directory):
                for filename in filenames:
                    file_path = Path(dirpath) / filename
                    try:
                        total_size += file_path.stat().st_size
                    except (OSError, FileNotFoundError):
                        continue
        except Exception as e:
            logger.error(f"Failed to calculate directory size for {directory}: {e}")
        
        return total_size
    
    def clean_empty_directories(self, root_directory: Path, dry_run: bool = False) -> int:
        """
        Remove empty directories recursively.
        
        Args:
            root_directory: Root directory to clean
            dry_run: If True, only report what would be deleted
            
        Returns:
            Number of directories removed
        """
        removed_count = 0
        
        try:
            # Walk bottom-up to handle nested empty directories
            for dirpath, dirnames, filenames in os.walk(root_directory, topdown=False):
                current_dir = Path(dirpath)
                
                # Skip root directory
                if current_dir == root_directory:
                    continue
                
                try:
                    # Check if directory is empty
                    if not any(current_dir.iterdir()):
                        if dry_run:
                            logger.info(f"Would remove empty directory: {current_dir}")
                        else:
                            current_dir.rmdir()
                            logger.debug(f"Removed empty directory: {current_dir}")
                        removed_count += 1
                except OSError as e:
                    logger.warning(f"Could not process directory {current_dir}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to clean empty directories in {root_directory}: {e}")
        
        return removed_count