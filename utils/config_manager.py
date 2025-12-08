"""
Enhanced configuration management for the Google Photos Takeout project.
"""

import os
import json
from configparser import ConfigParser
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass, asdict

from .logging_setup import get_logger

logger = get_logger(__name__)


@dataclass
class ProcessingConfig:
    """Configuration for file processing."""
    max_workers: int = 4
    batch_size: int = 100
    enable_progress_bar: bool = True
    dry_run: bool = False
    backup_enabled: bool = True
    verification_enabled: bool = True


@dataclass
class FilterConfig:
    """Configuration for file filtering."""
    date_range_start: Optional[str] = None  # YYYY-MM-DD format
    date_range_end: Optional[str] = None    # YYYY-MM-DD format
    include_extensions: Optional[List[str]] = None
    exclude_extensions: Optional[List[str]] = None
    min_file_size: Optional[int] = None  # bytes
    max_file_size: Optional[int] = None  # bytes


class ConfigManager:
    """Enhanced configuration manager with validation and defaults."""
    
    DEFAULT_CONFIG_PATH = "config.ini"
    DEFAULT_SETTINGS_PATH = "settings.json"
    
    def __init__(
        self, 
        config_path: str = DEFAULT_CONFIG_PATH,
        settings_path: str = DEFAULT_SETTINGS_PATH
    ):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to INI configuration file
            settings_path: Path to JSON settings file
        """
        self.config_path = Path(config_path)
        self.settings_path = Path(settings_path)
        self.config = ConfigParser()
        self.settings = {}
        
        self._load_config()
        self._load_settings()
        logger.info("Configuration manager initialized")
    
    def _load_config(self) -> None:
        """Load INI configuration file."""
        if not self.config_path.exists():
            self._create_default_config()
        
        self.config.read(self.config_path)
        logger.debug(f"Loaded configuration from {self.config_path}")
    
    def _load_settings(self) -> None:
        """Load JSON settings file."""
        if self.settings_path.exists():
            try:
                with open(self.settings_path, 'r', encoding='utf-8') as f:
                    self.settings = json.load(f)
                logger.debug(f"Loaded settings from {self.settings_path}")
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Failed to load settings: {e}")
                self.settings = {}
        else:
            self._create_default_settings()
    
    def _create_default_config(self) -> None:
        """Create default configuration file."""
        self.config.add_section('Extensions')
        self.config.set(
            'Extensions', 
            'PICTURE_EXTENSIONS', 
            '.jpg, .jpeg, .png, .gif, .bmp, .tiff, .tif, .webp, .heif, .heic, '
            '.svg, .raw, .arw, .cr2, .nef, .orf, .sr2, .cr3, .dng'
        )
        self.config.set(
            'Extensions',
            'VIDEO_EXTENSIONS',
            '.mp4, .avi, .mov, .wmv, .flv, .mkv, .webm, .mpeg, .mpg, .m4v, '
            '.3gp, .3g2, .asf, .rm, .rmvb, .vob, .ogv'
        )
        
        self.config.add_section('Processing')
        self.config.set('Processing', 'max_workers', '4')
        self.config.set('Processing', 'batch_size', '100')
        self.config.set('Processing', 'enable_progress_bar', 'true')
        
        self.config.add_section('Backup')
        self.config.set('Backup', 'enabled', 'true')
        self.config.set('Backup', 'backup_dir', 'backups')
        self.config.set('Backup', 'max_backups', '5')
        
        with open(self.config_path, 'w', encoding='utf-8') as f:
            self.config.write(f)
        
        logger.info(f"Created default configuration at {self.config_path}")
    
    def _create_default_settings(self) -> None:
        """Create default settings file."""
        default_settings = {
            "processing": asdict(ProcessingConfig()),
            "filters": asdict(FilterConfig()),
            "last_run": {
                "timestamp": None,
                "source_folder": None,
                "destination_folder": None
            }
        }
        
        self.settings = default_settings
        self.save_settings()
        logger.info(f"Created default settings at {self.settings_path}")
    
    def get_extensions(self) -> Tuple[List[str], List[str]]:
        """
        Get picture and video extensions from configuration.
        
        Returns:
            Tuple of (picture_extensions, video_extensions)
        """
        try:
            picture_extensions = [
                ext.strip() for ext in 
                self.config.get('Extensions', 'PICTURE_EXTENSIONS').split(',')
            ]
            video_extensions = [
                ext.strip() for ext in 
                self.config.get('Extensions', 'VIDEO_EXTENSIONS').split(',')
            ]
            return picture_extensions, video_extensions
        except Exception as e:
            logger.error(f"Failed to load extensions: {e}")
            raise
    
    def get_all_extensions(self) -> List[str]:
        """Get all supported extensions."""
        picture_exts, video_exts = self.get_extensions()
        return picture_exts + video_exts
    
    def get_processing_config(self) -> ProcessingConfig:
        """Get processing configuration."""
        config_dict = self.settings.get("processing", {})
        
        # Override with INI values if available
        if self.config.has_section('Processing'):
            try:
                config_dict.update({
                    'max_workers': self.config.getint('Processing', 'max_workers'),
                    'batch_size': self.config.getint('Processing', 'batch_size'),
                    'enable_progress_bar': self.config.getboolean('Processing', 'enable_progress_bar')
                })
            except Exception as e:
                logger.warning(f"Error reading processing config from INI: {e}")
        
        return ProcessingConfig(**config_dict)
    
    def get_filter_config(self) -> FilterConfig:
        """Get filter configuration."""
        config_dict = self.settings.get("filters", {})
        return FilterConfig(**config_dict)
    
    def get_backup_config(self) -> Dict[str, Any]:
        """Get backup configuration."""
        backup_config = {
            'enabled': True,
            'backup_dir': 'backups',
            'max_backups': 5
        }
        
        if self.config.has_section('Backup'):
            try:
                backup_config.update({
                    'enabled': self.config.getboolean('Backup', 'enabled'),
                    'backup_dir': self.config.get('Backup', 'backup_dir'),
                    'max_backups': self.config.getint('Backup', 'max_backups')
                })
            except Exception as e:
                logger.warning(f"Error reading backup config: {e}")
        
        return backup_config
    
    def update_processing_config(self, config: ProcessingConfig) -> None:
        """Update processing configuration."""
        self.settings["processing"] = asdict(config)
        self.save_settings()
    
    def update_filter_config(self, config: FilterConfig) -> None:
        """Update filter configuration."""
        self.settings["filters"] = asdict(config)
        self.save_settings()
    
    def save_settings(self) -> None:
        """Save settings to JSON file."""
        try:
            with open(self.settings_path, 'w', encoding='utf-8') as f:
                json.dump(self.settings, f, indent=4, default=str)
            logger.debug(f"Settings saved to {self.settings_path}")
        except OSError as e:
            logger.error(f"Failed to save settings: {e}")
            raise
    
    def update_last_run(
        self, 
        timestamp: str, 
        source_folder: Optional[str] = None,
        destination_folder: Optional[str] = None
    ) -> None:
        """Update last run information."""
        if "last_run" not in self.settings:
            self.settings["last_run"] = {}
        
        self.settings["last_run"]["timestamp"] = timestamp
        if source_folder:
            self.settings["last_run"]["source_folder"] = source_folder
        if destination_folder:
            self.settings["last_run"]["destination_folder"] = destination_folder
        
        self.save_settings()
    
    def get_datetime_tags(self) -> List[str]:
        """Get EXIF datetime tags in priority order."""
        return [
            'EXIF:DateTimeOriginal',
            'EXIF:CreateDate',
            'EXIF:ModifyDate', 
            'EXIF:DateTimeDigitized',
            'QuickTime:CreateDate',
            'ICC_Profile:ProfileDateTime'
        ]