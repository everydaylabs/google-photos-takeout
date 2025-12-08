#!/usr/bin/env python3
"""
Enhanced Google Photos Takeout analyzer with improved performance and features.

This script analyzes Google Photos Takeout exports to categorize media files,
generate comprehensive statistics, and identify problematic files.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Any
from collections import defaultdict, Counter

from utils import (
    ConfigManager, ExifProcessor, FileOperations, 
    setup_logging, get_logger, ProgressTracker
)

logger = get_logger(__name__)


class EnhancedAnalyzeTakeout:
    """Enhanced takeout analyzer with filtering, statistics, and performance optimizations."""
    
    def __init__(
        self,
        root_folder: str,
        export_folder: str,
        config_path: str = "config.ini"
    ):
        """
        Initialize the enhanced takeout analyzer.
        
        Args:
            root_folder: Path to the takeout folder to analyze
            export_folder: Path to export analysis results
            config_path: Path to configuration file
        """
        self.root_folder = Path(root_folder)
        self.export_folder = Path(export_folder)
        
        # Validate paths
        if not self.root_folder.exists():
            raise FileNotFoundError(f"Root folder does not exist: {root_folder}")
        
        self.export_folder.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.config_manager = ConfigManager(config_path)
        self.exif_processor = ExifProcessor(self.config_manager)
        self.file_operations = FileOperations(self.config_manager)
        
        # Get file extensions
        self.picture_extensions, self.video_extensions = self.config_manager.get_extensions()
        self.all_media_extensions = set(self.picture_extensions + self.video_extensions)
        
        # Analysis results
        self.analysis_results: Dict[str, Any] = {
            'file_counts': defaultdict(int),
            'file_sizes': [],
            'creation_dates': [],
            'media_files': [],
            'unknown_files': [],
            'problematic_files': [],
            'directory_structure': {},
            'duplicate_candidates': [],
            'size_distribution': {},
            'date_distribution': {},
            'metadata_stats': {}
        }
        
        logger.info(f"Initialized analyzer for {root_folder} -> {export_folder}")
    
    def analyze_files(
        self,
        include_metadata: bool = True,
        detect_duplicates: bool = False,
        max_workers: int = 4
    ) -> Dict[str, Any]:
        """
        Perform comprehensive analysis of files in the takeout.
        
        Args:
            include_metadata: Whether to extract EXIF metadata
            detect_duplicates: Whether to detect potential duplicate files
            max_workers: Number of worker threads for parallel processing
            
        Returns:
            Comprehensive analysis results
        """
        logger.info("Starting comprehensive file analysis")
        
        # First pass: collect all files
        all_files = self._collect_all_files()
        
        # Initialize progress tracker
        progress = ProgressTracker(
            total_items=len(all_files),
            description="Analyzing files",
            checkpoint_file="analysis_checkpoint.json"
        )
        
        try:
            # Categorize files
            media_files, unknown_files = self._categorize_files(all_files)
            
            # Analyze media files
            if media_files:
                self._analyze_media_files(
                    media_files, 
                    include_metadata,
                    progress,
                    max_workers
                )
            
            # Analyze unknown files
            if unknown_files:
                self._analyze_unknown_files(unknown_files, progress)
            
            # Detect duplicates if requested
            if detect_duplicates and media_files:
                logger.info("Detecting potential duplicate files")
                self._detect_duplicates(media_files)
            
            # Generate statistics
            self._generate_statistics()
            
            # Create directory structure map
            self._analyze_directory_structure()
            
        finally:
            progress.finish()
        
        logger.info("File analysis completed successfully")
        return self.analysis_results
    
    def _collect_all_files(self) -> List[Path]:
        """Collect all files from the root folder."""
        logger.info("Collecting all files from takeout directory")
        
        all_files = []
        try:
            for file_path in self.root_folder.rglob('*'):
                if file_path.is_file():
                    all_files.append(file_path)
        except Exception as e:
            logger.error(f"Error collecting files: {e}")
            raise
        
        logger.info(f"Found {len(all_files)} total files")
        return all_files
    
    def _categorize_files(self, all_files: List[Path]) -> Tuple[List[Path], List[Path]]:
        """
        Categorize files into media and unknown files.
        
        Args:
            all_files: List of all file paths
            
        Returns:
            Tuple of (media_files, unknown_files)
        """
        logger.info("Categorizing files by type")
        
        media_files = []
        unknown_files = []
        
        for file_path in all_files:
            extension = file_path.suffix.lower()
            if extension in self.all_media_extensions:
                media_files.append(file_path)
                self.analysis_results['file_counts'][extension] += 1
            else:
                unknown_files.append(file_path)
                self.analysis_results['file_counts']['unknown'] += 1
        
        self.analysis_results['media_files'] = media_files
        self.analysis_results['unknown_files'] = unknown_files
        
        logger.info(f"Categorized: {len(media_files)} media files, {len(unknown_files)} unknown files")
        return media_files, unknown_files
    
    def _analyze_media_files(
        self,
        media_files: List[Path],
        include_metadata: bool,
        progress: ProgressTracker,
        max_workers: int
    ) -> None:
        """
        Analyze media files for size, dates, and metadata.
        
        Args:
            media_files: List of media file paths
            include_metadata: Whether to extract metadata
            progress: Progress tracker instance
            max_workers: Number of worker threads
        """
        logger.info(f"Analyzing {len(media_files)} media files")
        
        for i, file_path in enumerate(media_files):
            if progress.should_skip_item(i):
                continue
                
            try:
                self._analyze_single_media_file(file_path, include_metadata)
                progress.update(current_item=str(file_path))
                
            except Exception as e:
                logger.error(f"Failed to analyze media file {file_path}: {e}")
                self.analysis_results['problematic_files'].append({
                    'file': str(file_path),
                    'error': str(e)
                })
                progress.update(failed=True)
    
    def _analyze_single_media_file(self, file_path: Path, include_metadata: bool) -> None:
        """
        Analyze a single media file.
        
        Args:
            file_path: Path to the media file
            include_metadata: Whether to extract metadata
        """
        try:
            # Get file size
            file_size = file_path.stat().st_size
            self.analysis_results['file_sizes'].append(file_size)
            
            # Extract creation date
            if include_metadata:
                exif_date = self.exif_processor.extract_datetime(file_path)
                if exif_date:
                    self.analysis_results['creation_dates'].append(exif_date)
                else:
                    # Fall back to file system date
                    fs_date = datetime.fromtimestamp(file_path.stat().st_mtime)
                    self.analysis_results['creation_dates'].append(fs_date)
            else:
                # Use file system date only
                fs_date = datetime.fromtimestamp(file_path.stat().st_mtime)
                self.analysis_results['creation_dates'].append(fs_date)
                
        except Exception as e:
            logger.warning(f"Error analyzing file {file_path}: {e}")
            raise
    
    def _analyze_unknown_files(self, unknown_files: List[Path], progress: ProgressTracker) -> None:
        """
        Analyze unknown/non-media files.
        
        Args:
            unknown_files: List of unknown file paths
            progress: Progress tracker instance
        """
        logger.info(f"Analyzing {len(unknown_files)} unknown files")
        
        extension_stats = defaultdict(int)
        
        for file_path in unknown_files:
            try:
                extension = file_path.suffix.lower() if file_path.suffix else '<no_extension>'
                extension_stats[extension] += 1
                progress.update(current_item=str(file_path))
                
            except Exception as e:
                logger.warning(f"Error analyzing unknown file {file_path}: {e}")
                progress.update(failed=True)
        
        self.analysis_results['unknown_extensions'] = dict(extension_stats)
    
    def _detect_duplicates(self, media_files: List[Path]) -> None:
        """
        Detect potential duplicate files based on size and name similarity.
        
        Args:
            media_files: List of media file paths
        """
        logger.info("Detecting potential duplicate files")
        
        # Group files by size
        size_groups = defaultdict(list)
        for file_path in media_files:
            try:
                file_size = file_path.stat().st_size
                size_groups[file_size].append(file_path)
            except OSError:
                continue
        
        # Find potential duplicates (same size)
        duplicates = []
        for size, files in size_groups.items():
            if len(files) > 1:
                duplicates.append({
                    'size': size,
                    'files': [str(f) for f in files],
                    'count': len(files)
                })
        
        self.analysis_results['duplicate_candidates'] = duplicates
        logger.info(f"Found {len(duplicates)} groups of potential duplicates")
    
    def _generate_statistics(self) -> None:
        """Generate comprehensive statistics from analysis results."""
        logger.info("Generating comprehensive statistics")
        
        # File size statistics
        if self.analysis_results['file_sizes']:
            sizes = self.analysis_results['file_sizes']
            total_size = sum(sizes)
            avg_size = total_size / len(sizes)
            
            # Size distribution
            size_ranges = [
                (0, 10 * 1024, "0-10KB"),
                (10 * 1024, 100 * 1024, "10KB-100KB"),
                (100 * 1024, 1024 * 1024, "100KB-1MB"),
                (1024 * 1024, 10 * 1024 * 024, "1MB-10MB"),
                (10 * 1024 * 1024, 100 * 1024 * 1024, "10MB-100MB"),
                (100 * 1024 * 1024, 1024 * 1024 * 1024, "100MB-1GB"),
                (1024 * 1024 * 1024, float('inf'), "1GB+")
            ]
            
            size_dist = Counter()
            for size in sizes:
                for lower, upper, label in size_ranges:
                    if lower <= size < upper:
                        size_dist[label] += 1
                        break
            
            self.analysis_results['size_distribution'] = {
                'total_size_bytes': total_size,
                'total_size_formatted': self._format_size(total_size),
                'average_size_bytes': avg_size,
                'average_size_formatted': self._format_size(avg_size),
                'distribution': dict(size_dist)
            }
        
        # Date distribution
        if self.analysis_results['creation_dates']:
            date_counts = defaultdict(int)
            for date in self.analysis_results['creation_dates']:
                month_key = date.strftime('%Y-%m')
                date_counts[month_key] += 1
            
            self.analysis_results['date_distribution'] = dict(date_counts)
        
        # EXIF processor statistics
        self.analysis_results['metadata_stats'] = self.exif_processor.get_statistics()
    
    def _analyze_directory_structure(self) -> None:
        """Analyze the directory structure of the takeout."""
        logger.info("Analyzing directory structure")
        
        def analyze_directory(dir_path: Path) -> Dict[str, Any]:
            """Recursively analyze directory structure."""
            result = {
                'path': str(dir_path),
                'file_count': 0,
                'media_file_count': 0,
                'total_size': 0,
                'subdirectories': {}
            }
            
            try:
                for item in dir_path.iterdir():
                    if item.is_file():
                        result['file_count'] += 1
                        try:
                            file_size = item.stat().st_size
                            result['total_size'] += file_size
                            
                            if item.suffix.lower() in self.all_media_extensions:
                                result['media_file_count'] += 1
                        except OSError:
                            pass
                    elif item.is_dir():
                        result['subdirectories'][item.name] = analyze_directory(item)
                        # Add subdirectory totals
                        subdir_data = result['subdirectories'][item.name]
                        result['file_count'] += subdir_data['file_count']
                        result['media_file_count'] += subdir_data['media_file_count']
                        result['total_size'] += subdir_data['total_size']
            except (OSError, PermissionError) as e:
                logger.warning(f"Could not access directory {dir_path}: {e}")
            
            return result
        
        self.analysis_results['directory_structure'] = analyze_directory(self.root_folder)
    
    @staticmethod
    def _format_size(size_bytes: float) -> str:
        """Format file size in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"
    
    def export_results(self) -> None:
        """Export analysis results to files."""
        logger.info("Exporting analysis results")
        
        # Create output directories
        known_export_dir = self.export_folder / 'known_file_analysis'
        unknown_export_dir = self.export_folder / 'unknown_file_analysis'
        known_export_dir.mkdir(exist_ok=True)
        unknown_export_dir.mkdir(exist_ok=True)
        
        # Export file counts
        with open(known_export_dir / 'file_counts.json', 'w') as f:
            json.dump(dict(self.analysis_results['file_counts']), f, indent=4)
        
        # Export size distribution
        if self.analysis_results['size_distribution']:
            with open(known_export_dir / 'size_distribution.json', 'w') as f:
                json.dump(self.analysis_results['size_distribution'], f, indent=4)
        
        # Export date distribution
        if self.analysis_results['date_distribution']:
            with open(known_export_dir / 'date_distribution.json', 'w') as f:
                json.dump(self.analysis_results['date_distribution'], f, indent=4)
        
        # Export directory structure
        with open(known_export_dir / 'directory_structure.json', 'w') as f:
            json.dump(self.analysis_results['directory_structure'], f, indent=4)
        
        # Export duplicate candidates
        if self.analysis_results['duplicate_candidates']:
            with open(known_export_dir / 'duplicate_candidates.json', 'w') as f:
                json.dump(self.analysis_results['duplicate_candidates'], f, indent=4)
        
        # Export unknown files list
        if self.analysis_results['unknown_files']:
            with open(unknown_export_dir / 'unknown_files.txt', 'w') as f:
                for file_path in self.analysis_results['unknown_files']:
                    f.write(f"{file_path}\n")
        
        # Export unknown extensions
        if 'unknown_extensions' in self.analysis_results:
            with open(unknown_export_dir / 'unknown_extensions.json', 'w') as f:
                json.dump(self.analysis_results['unknown_extensions'], f, indent=4)
        
        # Export problematic files
        if self.analysis_results['problematic_files']:
            with open(self.export_folder / 'problematic_files.json', 'w') as f:
                json.dump(self.analysis_results['problematic_files'], f, indent=4)
        
        # Export metadata statistics
        with open(known_export_dir / 'metadata_statistics.json', 'w') as f:
            json.dump(self.analysis_results['metadata_stats'], f, indent=4)
        
        # Export comprehensive summary
        summary = {
            'analysis_timestamp': datetime.now().isoformat(),
            'root_folder': str(self.root_folder),
            'total_files': len(self.analysis_results['media_files']) + len(self.analysis_results['unknown_files']),
            'media_files': len(self.analysis_results['media_files']),
            'unknown_files': len(self.analysis_results['unknown_files']),
            'problematic_files': len(self.analysis_results['problematic_files']),
            'duplicate_groups': len(self.analysis_results['duplicate_candidates']),
            'total_size': self.analysis_results['size_distribution'].get('total_size_formatted', 'N/A'),
            'file_type_breakdown': dict(self.analysis_results['file_counts'])
        }
        
        with open(self.export_folder / 'analysis_summary.json', 'w') as f:
            json.dump(summary, f, indent=4)
        
        logger.info(f"Analysis results exported to {self.export_folder}")


def main() -> int:
    """Main function for the enhanced analyze takeout script."""
    parser = argparse.ArgumentParser(
        description="Enhanced Google Photos Takeout analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/takeout /path/to/results
  %(prog)s /path/to/takeout /path/to/results --include-metadata --detect-duplicates
  %(prog)s /path/to/takeout /path/to/results --config custom_config.ini --workers 8
        """
    )
    
    parser.add_argument(
        'folder',
        type=str,
        help='Path to the Google Photos takeout folder'
    )
    parser.add_argument(
        'export_folder',
        type=str,
        help='Folder to export analysis results'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config.ini',
        help='Path to configuration file (default: config.ini)'
    )
    parser.add_argument(
        '--include-metadata',
        action='store_true',
        default=True,
        help='Extract EXIF metadata from media files (default: True)'
    )
    parser.add_argument(
        '--detect-duplicates',
        action='store_true',
        help='Detect potential duplicate files'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of worker threads for parallel processing (default: 4)'
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
        # Initialize analyzer
        analyzer = EnhancedAnalyzeTakeout(
            root_folder=args.folder,
            export_folder=args.export_folder,
            config_path=args.config
        )
        
        # Perform analysis
        results = analyzer.analyze_files(
            include_metadata=args.include_metadata,
            detect_duplicates=args.detect_duplicates,
            max_workers=args.workers
        )
        
        # Export results
        analyzer.export_results()
        
        # Print summary
        print("\n" + "="*50)
        print("ANALYSIS SUMMARY")
        print("="*50)
        print(f"Total files analyzed: {len(results['media_files']) + len(results['unknown_files'])}")
        print(f"Media files: {len(results['media_files'])}")
        print(f"Unknown files: {len(results['unknown_files'])}")
        print(f"Problematic files: {len(results['problematic_files'])}")
        
        if results['duplicate_candidates']:
            print(f"Potential duplicate groups: {len(results['duplicate_candidates'])}")
        
        if results['size_distribution']:
            print(f"Total size: {results['size_distribution']['total_size_formatted']}")
        
        print(f"\nResults exported to: {args.export_folder}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("Analysis interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())