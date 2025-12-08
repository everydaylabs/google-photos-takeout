#!/usr/bin/env python3
"""
Enhanced Google Photos Takeout organizer with backup, filtering, and resumability.

This script organizes media files from Google Photos Takeout exports into a
date-based directory structure (year/month/day) using EXIF metadata.
"""

import argparse
import json
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from utils import (
    ConfigManager, ExifProcessor, FileOperations,
    setup_logging, get_logger, ProgressTracker,
    FilterConfig, ProcessingConfig
)

logger = get_logger(__name__)


class EnhancedOrganizeTakeout:
    """Enhanced takeout organizer with backup, filtering, and progress tracking."""
    
    def __init__(
        self,
        source_folder: str,
        destination_folder: str,
        config_path: str = "config.ini"
    ):
        """
        Initialize the enhanced takeout organizer.
        
        Args:
            source_folder: Path to source folder containing media files
            destination_folder: Path to destination folder for organized files
            config_path: Path to configuration file
        """
        self.source_folder = Path(source_folder)
        self.destination_folder = Path(destination_folder)
        
        # Validate paths
        if not self.source_folder.exists():
            raise FileNotFoundError(f"Source folder does not exist: {source_folder}")
        
        self.destination_folder.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.config_manager = ConfigManager(config_path)
        self.processing_config = self.config_manager.get_processing_config()
        self.filter_config = self.config_manager.get_filter_config()
        
        self.exif_processor = ExifProcessor(
            self.config_manager,
            max_workers=self.processing_config.max_workers
        )
        
        self.file_operations = FileOperations(
            self.config_manager,
            backup_enabled=self.processing_config.backup_enabled,
            verification_enabled=self.processing_config.verification_enabled
        )
        
        # Get file extensions
        self.picture_extensions, self.video_extensions = self.config_manager.get_extensions()
        self.all_media_extensions = set(self.picture_extensions + self.video_extensions)
        
        # Organization results
        self.organization_results: Dict[str, Any] = {
            'files_processed': 0,
            'files_with_datetime': 0,
            'files_without_datetime': 0,
            'files_failed': 0,
            'files_skipped': 0,
            'files_by_year': {},
            'failed_files': [],
            'undated_files': [],
            'filtered_out_files': []
        }
        
        # Thread-safe counter
        self._stats_lock = threading.Lock()
        
        logger.info(f"Initialized organizer: {source_folder} -> {destination_folder}")
    
    def organize_files(
        self,
        dry_run: bool = False,
        preserve_names: bool = False,
        use_filters: bool = True,
        resume_from_checkpoint: bool = True
    ) -> Dict[str, Any]:
        """
        Organize media files by date with enhanced features.
        
        Args:
            dry_run: If True, only simulate the organization
            preserve_names: Whether to preserve original filenames
            use_filters: Whether to apply configured filters
            resume_from_checkpoint: Whether to resume from previous checkpoint
            
        Returns:
            Organization results and statistics
        """
        logger.info("Starting enhanced file organization")
        
        if dry_run:
            logger.info("DRY RUN MODE: No files will be moved")
        
        # Collect all media files
        media_files = self._collect_media_files()
        
        # Apply filters if enabled
        if use_filters:
            media_files = self._apply_filters(media_files)
        
        if not media_files:
            logger.warning("No files to organize after filtering")
            return self.organization_results
        
        # Initialize progress tracker
        checkpoint_file = "organize_checkpoint.json" if resume_from_checkpoint else None
        progress = ProgressTracker(
            total_items=len(media_files),
            description="Organizing files",
            enable_progress_bar=self.processing_config.enable_progress_bar,
            checkpoint_file=checkpoint_file
        )
        
        try:
            # Process files in batches
            self._process_files_in_batches(
                media_files,
                dry_run,
                preserve_names,
                progress
            )
            
            # Generate summary statistics
            self._generate_summary_statistics()
            
            # Export results
            self._export_organization_results()
            
        finally:
            progress.finish()
        
        logger.info("File organization completed successfully")
        return self.organization_results
    
    def _collect_media_files(self) -> List[Path]:
        """Collect all media files from source folder."""
        logger.info("Collecting media files from source folder")
        
        media_files = []
        for file_path in self.source_folder.rglob('*'):
            if (file_path.is_file() and 
                file_path.suffix.lower() in self.all_media_extensions):
                media_files.append(file_path)
        
        logger.info(f"Found {len(media_files)} media files")
        return media_files
    
    def _apply_filters(self, media_files: List[Path]) -> List[Path]:
        """
        Apply configured filters to media files.
        
        Args:
            media_files: List of media file paths
            
        Returns:
            Filtered list of media files
        """
        logger.info("Applying filters to media files")
        original_count = len(media_files)
        filtered_files = []
        
        for file_path in media_files:
            if self._should_include_file(file_path):
                filtered_files.append(file_path)
            else:
                self.organization_results['filtered_out_files'].append(str(file_path))
        
        filtered_count = original_count - len(filtered_files)
        if filtered_count > 0:
            logger.info(f"Filtered out {filtered_count} files based on criteria")
        
        return filtered_files
    
    def _should_include_file(self, file_path: Path) -> bool:
        """
        Check if file should be included based on filter configuration.
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if file should be included
        """
        try:
            # Extension filters
            if self.filter_config.include_extensions:
                if file_path.suffix.lower() not in self.filter_config.include_extensions:
                    return False
            
            if self.filter_config.exclude_extensions:
                if file_path.suffix.lower() in self.filter_config.exclude_extensions:
                    return False
            
            # Size filters
            if self.filter_config.min_file_size or self.filter_config.max_file_size:
                file_size = file_path.stat().st_size
                
                if (self.filter_config.min_file_size and 
                    file_size < self.filter_config.min_file_size):
                    return False
                
                if (self.filter_config.max_file_size and 
                    file_size > self.filter_config.max_file_size):
                    return False
            
            # Date range filters
            if (self.filter_config.date_range_start or 
                self.filter_config.date_range_end):
                
                file_date = self.exif_processor.extract_datetime(file_path)
                if not file_date:
                    # Use file modification time as fallback
                    file_date = datetime.fromtimestamp(file_path.stat().st_mtime)
                
                if self.filter_config.date_range_start:
                    start_date = datetime.strptime(self.filter_config.date_range_start, '%Y-%m-%d')
                    if file_date < start_date:
                        return False
                
                if self.filter_config.date_range_end:
                    end_date = datetime.strptime(self.filter_config.date_range_end, '%Y-%m-%d')
                    if file_date > end_date:
                        return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Error applying filters to {file_path}: {e}")
            return True  # Include file if filter check fails
    
    def _process_files_in_batches(
        self,
        media_files: List[Path],
        dry_run: bool,
        preserve_names: bool,
        progress: ProgressTracker
    ) -> None:
        """
        Process files in batches with parallel processing.
        
        Args:
            media_files: List of media file paths to process
            dry_run: Whether to simulate operations
            preserve_names: Whether to preserve original filenames
            progress: Progress tracker instance
        """
        batch_size = self.processing_config.batch_size
        max_workers = self.processing_config.max_workers
        
        # Split files into batches
        batches = [
            media_files[i:i + batch_size]
            for i in range(0, len(media_files), batch_size)
        ]
        
        logger.info(f"Processing {len(media_files)} files in {len(batches)} batches")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit batch processing tasks
            future_to_batch = {
                executor.submit(
                    self._process_file_batch,
                    batch, dry_run, preserve_names, progress
                ): i for i, batch in enumerate(batches)
            }
            
            # Process completed batches
            for future in as_completed(future_to_batch):
                batch_index = future_to_batch[future]
                try:
                    batch_results = future.result()
                    logger.debug(f"Completed batch {batch_index + 1}/{len(batches)}")
                except Exception as e:
                    logger.error(f"Batch {batch_index + 1} failed: {e}")
    
    def _process_file_batch(
        self,
        file_batch: List[Path],
        dry_run: bool,
        preserve_names: bool,
        progress: ProgressTracker
    ) -> Dict[str, int]:
        """
        Process a batch of files.
        
        Args:
            file_batch: List of files in this batch
            dry_run: Whether to simulate operations
            preserve_names: Whether to preserve original filenames
            progress: Progress tracker instance
            
        Returns:
            Statistics for this batch
        """
        batch_stats = {
            'processed': 0,
            'with_datetime': 0,
            'without_datetime': 0,
            'failed': 0,
            'skipped': 0
        }
        
        for i, file_path in enumerate(file_batch):
            # Check if we should skip this file (for resumability)
            global_index = progress.get_processed_items()
            if progress.should_skip_item(global_index):
                batch_stats['skipped'] += 1
                progress.update(current_item=str(file_path))
                continue
            
            try:
                result = self._process_single_file(file_path, dry_run, preserve_names)
                
                if result['success']:
                    batch_stats['processed'] += 1
                    if result['had_datetime']:
                        batch_stats['with_datetime'] += 1
                    else:
                        batch_stats['without_datetime'] += 1
                else:
                    batch_stats['failed'] += 1
                
                progress.update(current_item=str(file_path))
                
            except Exception as e:
                logger.error(f"Failed to process file {file_path}: {e}")
                batch_stats['failed'] += 1
                progress.update(failed=True)
        
        # Update global statistics
        with self._stats_lock:
            self.organization_results['files_processed'] += batch_stats['processed']
            self.organization_results['files_with_datetime'] += batch_stats['with_datetime']
            self.organization_results['files_without_datetime'] += batch_stats['without_datetime']
            self.organization_results['files_failed'] += batch_stats['failed']
            self.organization_results['files_skipped'] += batch_stats['skipped']
        
        return batch_stats
    
    def _process_single_file(
        self,
        file_path: Path,
        dry_run: bool,
        preserve_names: bool
    ) -> Dict[str, Any]:
        """
        Process a single file for organization.
        
        Args:
            file_path: Path to the file to process
            dry_run: Whether to simulate the operation
            preserve_names: Whether to preserve original filename
            
        Returns:
            Result dictionary with success status and metadata
        """
        try:
            # Extract datetime from EXIF
            file_datetime = self.exif_processor.extract_datetime(file_path)
            had_datetime = file_datetime is not None
            
            if file_datetime:
                # Create date-based directory structure
                target_dir = (self.destination_folder / 
                             str(file_datetime.year) / 
                             f"{file_datetime.month:02d}" / 
                             f"{file_datetime.day:02d}")
            else:
                # Place in undated folder
                target_dir = self.destination_folder / "undated"
                self.organization_results['undated_files'].append(str(file_path))
            
            if dry_run:
                logger.debug(f"DRY RUN: Would move {file_path} to {target_dir}")
                return {'success': True, 'had_datetime': had_datetime}
            
            # Perform the actual file move
            success = self.file_operations.safe_move(
                source=file_path,
                destination=target_dir,
                preserve_name=preserve_names,
                handle_duplicates="uuid"  # Fixed: prevents file overwriting
            )
            
            if success:
                # Track files by year for statistics
                year = file_datetime.year if file_datetime else 'undated'
                with self._stats_lock:
                    if year not in self.organization_results['files_by_year']:
                        self.organization_results['files_by_year'][year] = 0
                    self.organization_results['files_by_year'][year] += 1
                
                logger.debug(f"Successfully organized {file_path} to {target_dir}")
                return {'success': True, 'had_datetime': had_datetime}
            else:
                self.organization_results['failed_files'].append({
                    'file': str(file_path),
                    'error': 'File move operation failed'
                })
                return {'success': False, 'had_datetime': had_datetime}
                
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            self.organization_results['failed_files'].append({
                'file': str(file_path),
                'error': str(e)
            })
            return {'success': False, 'had_datetime': False}
    
    def _generate_summary_statistics(self) -> None:
        """Generate comprehensive summary statistics."""
        logger.info("Generating summary statistics")
        
        total_files = (self.organization_results['files_processed'] + 
                      self.organization_results['files_failed'] +
                      self.organization_results['files_skipped'])
        
        if total_files > 0:
            success_rate = (self.organization_results['files_processed'] / total_files) * 100
            datetime_rate = (self.organization_results['files_with_datetime'] / 
                           max(self.organization_results['files_processed'], 1)) * 100
            
            self.organization_results['summary_statistics'] = {
                'total_files_found': total_files,
                'success_rate_percent': round(success_rate, 2),
                'datetime_extraction_rate_percent': round(datetime_rate, 2),
                'years_covered': list(self.organization_results['files_by_year'].keys()),
                'most_active_year': max(
                    self.organization_results['files_by_year'].items(),
                    key=lambda x: x[1],
                    default=('None', 0)
                )[0]
            }
    
    def _export_organization_results(self) -> None:
        """Export organization results and statistics."""
        logger.info("Exporting organization results")
        
        # Export undated files list
        if self.organization_results['undated_files']:
            undated_file = self.destination_folder.parent / 'files_without_datetime.json'
            with open(undated_file, 'w') as f:
                json.dump(self.organization_results['undated_files'], f, indent=4)
            logger.info(f"Exported {len(self.organization_results['undated_files'])} undated files to {undated_file}")
        
        # Export failed files list
        if self.organization_results['failed_files']:
            failed_file = self.destination_folder.parent / 'failed_files.json'
            with open(failed_file, 'w') as f:
                json.dump(self.organization_results['failed_files'], f, indent=4)
            logger.info(f"Exported {len(self.organization_results['failed_files'])} failed files to {failed_file}")
        
        # Export filtered files list
        if self.organization_results['filtered_out_files']:
            filtered_file = self.destination_folder.parent / 'filtered_files.json'
            with open(filtered_file, 'w') as f:
                json.dump(self.organization_results['filtered_out_files'], f, indent=4)
            logger.info(f"Exported {len(self.organization_results['filtered_out_files'])} filtered files to {filtered_file}")
        
        # Export comprehensive results
        results_file = self.destination_folder.parent / 'organization_results.json'
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'source_folder': str(self.source_folder),
            'destination_folder': str(self.destination_folder),
            'processing_config': self.processing_config.__dict__,
            'filter_config': self.filter_config.__dict__,
            'results': self.organization_results,
            'exif_processor_stats': self.exif_processor.get_statistics()
        }
        
        with open(results_file, 'w') as f:
            json.dump(export_data, f, indent=4, default=str)
        
        logger.info(f"Organization results exported to {results_file}")
        
        # Update last run information
        self.config_manager.update_last_run(
            timestamp=datetime.now().isoformat(),
            source_folder=str(self.source_folder),
            destination_folder=str(self.destination_folder)
        )
    
    def get_organization_summary(self) -> Dict[str, Any]:
        """Get a summary of organization results."""
        return {
            'files_processed': self.organization_results['files_processed'],
            'files_with_datetime': self.organization_results['files_with_datetime'],
            'files_without_datetime': self.organization_results['files_without_datetime'],
            'files_failed': self.organization_results['files_failed'],
            'files_by_year': self.organization_results['files_by_year'],
            'summary_statistics': self.organization_results.get('summary_statistics', {})
        }


def main() -> int:
    """Main function for the enhanced organize takeout script."""
    parser = argparse.ArgumentParser(
        description="Enhanced Google Photos Takeout organizer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/source /path/to/destination
  %(prog)s /path/to/source /path/to/destination --dry-run
  %(prog)s /path/to/source /path/to/destination --preserve-names --workers 8
  %(prog)s /path/to/source /path/to/destination --config custom_config.ini --no-resume
        """
    )
    
    parser.add_argument(
        'source_folder',
        type=str,
        help='Path to source folder containing photos/videos'
    )
    parser.add_argument(
        'destination_folder',
        type=str,
        help='Path to destination folder for organized files'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config.ini',
        help='Path to configuration file (default: config.ini)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulate organization without moving files'
    )
    parser.add_argument(
        '--preserve-names',
        action='store_true',
        help='Preserve original filenames instead of using UUIDs'
    )
    parser.add_argument(
        '--no-filters',
        action='store_true',
        help='Disable filter application'
    )
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Do not resume from previous checkpoint'
    )
    parser.add_argument(
        '--workers',
        type=int,
        help='Number of worker threads (overrides config)'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    parser.add_argument(
        '--no-file-logging',
        action='store_true',
        help='Disable file logging'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(
        level=args.log_level,
        enable_file_logging=not args.no_file_logging
    )
    
    try:
        # Initialize organizer
        organizer = EnhancedOrganizeTakeout(
            source_folder=args.source_folder,
            destination_folder=args.destination_folder,
            config_path=args.config
        )
        
        # Override workers if specified
        if args.workers:
            organizer.processing_config.max_workers = args.workers
        
        # Perform organization
        results = organizer.organize_files(
            dry_run=args.dry_run,
            preserve_names=args.preserve_names,
            use_filters=not args.no_filters,
            resume_from_checkpoint=not args.no_resume
        )
        
        # Print summary
        print("\n" + "="*50)
        print("ORGANIZATION SUMMARY")
        print("="*50)
        
        summary = organizer.get_organization_summary()
        print(f"Files processed: {summary['files_processed']}")
        print(f"Files with datetime: {summary['files_with_datetime']}")
        print(f"Files without datetime: {summary['files_without_datetime']}")
        print(f"Files failed: {summary['files_failed']}")
        
        if summary['files_by_year']:
            print(f"\nFiles by year:")
            for year, count in sorted(summary['files_by_year'].items()):
                print(f"  {year}: {count} files")
        
        if 'summary_statistics' in summary and summary['summary_statistics']:
            stats = summary['summary_statistics']
            print(f"\nSuccess rate: {stats.get('success_rate_percent', 0):.1f}%")
            print(f"DateTime extraction rate: {stats.get('datetime_extraction_rate_percent', 0):.1f}%")
            if stats.get('most_active_year') != 'None':
                print(f"Most active year: {stats.get('most_active_year')}")
        
        print(f"\nResults exported to: {args.destination_folder}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("Organization interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Organization failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())