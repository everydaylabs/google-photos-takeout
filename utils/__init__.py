"""
Google Photos Takeout utilities package.

This package provides utilities for processing Google Photos Takeout exports.
"""

from .config_manager import ConfigManager
from .exif_processor import ExifProcessor
from .file_operations import FileOperations
from .logging_setup import setup_logging
from .progress_tracker import ProgressTracker

__all__ = [
    'ConfigManager',
    'ExifProcessor', 
    'FileOperations',
    'setup_logging',
    'ProgressTracker'
]

__version__ = "2.0.0"