"""
Enhanced EXIF processing utilities with improved performance and error handling.
"""

import hashlib
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from exiftool import ExifToolHelper

from .config_manager import ConfigManager
from .logging_setup import get_logger

logger = get_logger(__name__)


class ExifProcessingError(Exception):
    """Exception raised during EXIF processing."""
    pass


class ExifProcessor:
    """Enhanced EXIF processor with caching, batching, and performance optimizations."""
    
    def __init__(
        self,
        config_manager: ConfigManager,
        cache_size: int = 1000,
        batch_size: int = 50,
        max_workers: int = 4
    ):
        """
        Initialize EXIF processor.
        
        Args:
            config_manager: Configuration manager instance
            cache_size: Size of metadata cache
            batch_size: Batch size for processing files
            max_workers: Maximum number of worker threads
        """
        self.config_manager = config_manager
        self.datetime_tags = config_manager.get_datetime_tags()
        self.cache_size = cache_size
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        # Metadata cache with LRU-like behavior
        self._metadata_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_access_order: List[str] = []
        self._cache_lock = threading.Lock()
        
        # Statistics
        self.stats = {
            'files_processed': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'extraction_failures': 0
        }
        
        logger.info(f"EXIF processor initialized with cache_size={cache_size}, batch_size={batch_size}")
    
    def _get_file_hash(self, file_path: Path) -> str:
        """
        Generate a hash for file caching based on path and modification time.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Hash string for caching
        """
        try:
            stat = file_path.stat()
            cache_key = f"{file_path}_{stat.st_mtime}_{stat.st_size}"
            return hashlib.md5(cache_key.encode()).hexdigest()
        except OSError:
            return hashlib.md5(str(file_path).encode()).hexdigest()
    
    def _get_cached_metadata(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """
        Get cached metadata for a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Cached metadata or None
        """
        cache_key = self._get_file_hash(file_path)
        
        with self._cache_lock:
            if cache_key in self._metadata_cache:
                # Move to end for LRU
                self._cache_access_order.remove(cache_key)
                self._cache_access_order.append(cache_key)
                self.stats['cache_hits'] += 1
                return self._metadata_cache[cache_key]
            
            self.stats['cache_misses'] += 1
            return None
    
    def _cache_metadata(self, file_path: Path, metadata: Dict[str, Any]) -> None:
        """
        Cache metadata for a file.
        
        Args:
            file_path: Path to the file
            metadata: Metadata to cache
        """
        cache_key = self._get_file_hash(file_path)
        
        with self._cache_lock:
            # Remove oldest entries if cache is full
            while len(self._metadata_cache) >= self.cache_size:
                oldest_key = self._cache_access_order.pop(0)
                del self._metadata_cache[oldest_key]
            
            self._metadata_cache[cache_key] = metadata
            self._cache_access_order.append(cache_key)
    
    def extract_datetime(self, file_path: Path) -> Optional[datetime]:
        """
        Extract datetime from file EXIF data.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Datetime object or None if not found
        """
        if not file_path.exists():
            logger.warning(f"File does not exist: {file_path}")
            return None
        
        # Check cache first
        cached_metadata = self._get_cached_metadata(file_path)
        if cached_metadata is not None:
            return cached_metadata.get('datetime')
        
        try:
            with ExifToolHelper() as et:
                metadata_list = et.get_tags(
                    files=[str(file_path)],
                    tags=self.datetime_tags
                )
                
                if not metadata_list:
                    logger.debug(f"No metadata found for {file_path}")
                    self._cache_metadata(file_path, {'datetime': None})
                    return None
                
                metadata = metadata_list[0]
                
                # Try datetime tags in priority order
                for tag in self.datetime_tags:
                    if tag in metadata:
                        date_str = metadata[tag]
                        try:
                            parsed_date = datetime.strptime(date_str, '%Y:%m:%d %H:%M:%S')
                            self._cache_metadata(file_path, {'datetime': parsed_date, 'source_tag': tag})
                            logger.debug(f"Extracted datetime from {tag}: {parsed_date} for {file_path}")
                            return parsed_date
                        except ValueError as e:
                            logger.warning(f"Invalid date format in {tag}: {date_str} for {file_path}: {e}")
                            continue
                
                # No valid datetime found
                self._cache_metadata(file_path, {'datetime': None})
                logger.debug(f"No valid datetime found in EXIF for {file_path}")
                return None
                
        except Exception as e:
            self.stats['extraction_failures'] += 1
            logger.error(f"Failed to extract EXIF datetime from {file_path}: {e}")
            return None
        finally:
            self.stats['files_processed'] += 1
    
    def extract_all_metadata(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract all metadata from a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary containing all metadata
        """
        if not file_path.exists():
            logger.warning(f"File does not exist: {file_path}")
            return {}
        
        # Check cache first
        cache_key = self._get_file_hash(file_path)
        with self._cache_lock:
            if cache_key in self._metadata_cache:
                full_metadata = self._metadata_cache[cache_key]
                if 'full_metadata' in full_metadata:
                    self.stats['cache_hits'] += 1
                    return full_metadata['full_metadata']
        
        try:
            with ExifToolHelper() as et:
                metadata_list = et.get_metadata_batch([str(file_path)])
                
                if not metadata_list:
                    logger.debug(f"No metadata found for {file_path}")
                    return {}
                
                metadata = metadata_list[0]
                
                # Cache the full metadata
                with self._cache_lock:
                    if cache_key in self._metadata_cache:
                        self._metadata_cache[cache_key]['full_metadata'] = metadata
                    else:
                        self._metadata_cache[cache_key] = {'full_metadata': metadata}
                        self._cache_access_order.append(cache_key)
                
                return metadata
                
        except Exception as e:
            self.stats['extraction_failures'] += 1
            logger.error(f"Failed to extract metadata from {file_path}: {e}")
            return {}
        finally:
            self.stats['files_processed'] += 1
    
    def process_files_batch(
        self, 
        file_paths: List[Path],
        extract_full_metadata: bool = False
    ) -> Dict[Path, Dict[str, Any]]:
        """
        Process a batch of files for EXIF data.
        
        Args:
            file_paths: List of file paths to process
            extract_full_metadata: Whether to extract full metadata or just datetime
            
        Returns:
            Dictionary mapping file paths to their metadata
        """
        results = {}
        
        # Split into cached and uncached files
        cached_files = []
        uncached_files = []
        
        for file_path in file_paths:
            cached_metadata = self._get_cached_metadata(file_path)
            if cached_metadata is not None:
                if extract_full_metadata:
                    if 'full_metadata' in cached_metadata:
                        results[file_path] = cached_metadata['full_metadata']
                        cached_files.append(file_path)
                    else:
                        uncached_files.append(file_path)
                else:
                    results[file_path] = {'datetime': cached_metadata.get('datetime')}
                    cached_files.append(file_path)
            else:
                uncached_files.append(file_path)
        
        logger.debug(f"Batch processing: {len(cached_files)} cached, {len(uncached_files)} uncached")
        
        # Process uncached files
        if uncached_files:
            try:
                with ExifToolHelper() as et:
                    if extract_full_metadata:
                        metadata_list = et.get_metadata_batch([str(f) for f in uncached_files])
                        for file_path, metadata in zip(uncached_files, metadata_list):
                            results[file_path] = metadata
                            self._cache_metadata(file_path, {'full_metadata': metadata})
                    else:
                        # Extract only datetime tags for better performance
                        metadata_list = et.get_tags(
                            files=[str(f) for f in uncached_files],
                            tags=self.datetime_tags
                        )
                        
                        for file_path, metadata in zip(uncached_files, metadata_list):
                            datetime_result = None
                            source_tag = None
                            
                            for tag in self.datetime_tags:
                                if tag in metadata:
                                    try:
                                        datetime_result = datetime.strptime(
                                            metadata[tag], '%Y:%m:%d %H:%M:%S'
                                        )
                                        source_tag = tag
                                        break
                                    except ValueError:
                                        continue
                            
                            results[file_path] = {'datetime': datetime_result}
                            self._cache_metadata(
                                file_path, 
                                {'datetime': datetime_result, 'source_tag': source_tag}
                            )
            
            except Exception as e:
                logger.error(f"Batch processing failed: {e}")
                # Fallback to individual processing
                for file_path in uncached_files:
                    if extract_full_metadata:
                        results[file_path] = self.extract_all_metadata(file_path)
                    else:
                        results[file_path] = {'datetime': self.extract_datetime(file_path)}
        
        return results
    
    def get_files_without_datetime(
        self, 
        file_paths: List[Path],
        use_parallel: bool = True
    ) -> Tuple[List[Path], List[Path]]:
        """
        Categorize files into those with and without datetime metadata.
        
        Args:
            file_paths: List of file paths to check
            use_parallel: Whether to use parallel processing
            
        Returns:
            Tuple of (files_with_datetime, files_without_datetime)
        """
        files_with_datetime = []
        files_without_datetime = []
        
        if use_parallel and len(file_paths) > self.batch_size:
            # Process in batches with parallel execution
            batches = [
                file_paths[i:i + self.batch_size]
                for i in range(0, len(file_paths), self.batch_size)
            ]
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_batch = {
                    executor.submit(self.process_files_batch, batch): batch
                    for batch in batches
                }
                
                for future in as_completed(future_to_batch):
                    try:
                        batch_results = future.result()
                        for file_path, metadata in batch_results.items():
                            if metadata.get('datetime'):
                                files_with_datetime.append(file_path)
                            else:
                                files_without_datetime.append(file_path)
                    except Exception as e:
                        batch = future_to_batch[future]
                        logger.error(f"Batch processing failed: {e}")
                        # Add all files in failed batch to without_datetime
                        files_without_datetime.extend(batch)
        else:
            # Sequential processing
            for file_path in file_paths:
                datetime_result = self.extract_datetime(file_path)
                if datetime_result:
                    files_with_datetime.append(file_path)
                else:
                    files_without_datetime.append(file_path)
        
        return files_with_datetime, files_without_datetime
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics."""
        cache_hit_rate = (
            self.stats['cache_hits'] / 
            (self.stats['cache_hits'] + self.stats['cache_misses'])
        ) if (self.stats['cache_hits'] + self.stats['cache_misses']) > 0 else 0
        
        return {
            **self.stats,
            'cache_size': len(self._metadata_cache),
            'cache_hit_rate': cache_hit_rate
        }
    
    def clear_cache(self) -> None:
        """Clear the metadata cache."""
        with self._cache_lock:
            self._metadata_cache.clear()
            self._cache_access_order.clear()
        logger.info("EXIF metadata cache cleared")