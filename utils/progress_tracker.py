"""
Progress tracking utilities for long-running operations.
"""

import time
import threading
from typing import Optional, Callable, Dict, Any
from dataclasses import dataclass
from pathlib import Path

from .logging_setup import get_logger

logger = get_logger(__name__)


@dataclass
class ProgressStats:
    """Statistics for progress tracking."""
    total_items: int = 0
    processed_items: int = 0
    failed_items: int = 0
    start_time: float = 0.0
    current_item: Optional[str] = None
    
    @property
    def completion_percentage(self) -> float:
        """Calculate completion percentage."""
        if self.total_items == 0:
            return 0.0
        return (self.processed_items / self.total_items) * 100
    
    @property
    def elapsed_time(self) -> float:
        """Calculate elapsed time in seconds."""
        return time.time() - self.start_time
    
    @property
    def items_per_second(self) -> float:
        """Calculate processing rate."""
        elapsed = self.elapsed_time
        if elapsed == 0:
            return 0.0
        return self.processed_items / elapsed
    
    @property
    def eta_seconds(self) -> float:
        """Calculate estimated time to completion."""
        rate = self.items_per_second
        if rate == 0:
            return 0.0
        remaining = self.total_items - self.processed_items
        return remaining / rate


class ProgressTracker:
    """Enhanced progress tracker with checkpointing and resumability."""
    
    def __init__(
        self,
        total_items: int,
        description: str = "Processing",
        enable_progress_bar: bool = True,
        checkpoint_file: Optional[str] = None,
        update_callback: Optional[Callable[[ProgressStats], None]] = None
    ):
        """
        Initialize progress tracker.
        
        Args:
            total_items: Total number of items to process
            description: Description of the operation
            enable_progress_bar: Whether to show progress bar
            checkpoint_file: Optional file for checkpointing progress
            update_callback: Optional callback for progress updates
        """
        self.stats = ProgressStats(total_items=total_items, start_time=time.time())
        self.description = description
        self.enable_progress_bar = enable_progress_bar
        self.checkpoint_file = Path(checkpoint_file) if checkpoint_file else None
        self.update_callback = update_callback
        
        self._lock = threading.Lock()
        self._last_update = 0.0
        self._update_interval = 0.5  # Update every 0.5 seconds
        
        # Try to load existing checkpoint
        self._load_checkpoint()
        
        logger.info(f"Progress tracker initialized: {self.description}")
        if self.stats.processed_items > 0:
            logger.info(f"Resuming from checkpoint: {self.stats.processed_items}/{self.stats.total_items}")
    
    def _load_checkpoint(self) -> None:
        """Load progress from checkpoint file."""
        if not self.checkpoint_file or not self.checkpoint_file.exists():
            return
        
        try:
            import json
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
            
            self.stats.processed_items = data.get('processed_items', 0)
            self.stats.failed_items = data.get('failed_items', 0)
            logger.info(f"Loaded checkpoint: {self.stats.processed_items} items processed")
        
        except Exception as e:
            logger.warning(f"Failed to load checkpoint: {e}")
    
    def _save_checkpoint(self) -> None:
        """Save progress to checkpoint file."""
        if not self.checkpoint_file:
            return
        
        try:
            import json
            self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
            
            data = {
                'processed_items': self.stats.processed_items,
                'failed_items': self.stats.failed_items,
                'timestamp': time.time()
            }
            
            with open(self.checkpoint_file, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")
    
    def update(
        self, 
        increment: int = 1,
        current_item: Optional[str] = None,
        failed: bool = False
    ) -> None:
        """
        Update progress.
        
        Args:
            increment: Number of items processed
            current_item: Name of current item being processed
            failed: Whether the item failed processing
        """
        with self._lock:
            if failed:
                self.stats.failed_items += increment
            else:
                self.stats.processed_items += increment
            
            if current_item:
                self.stats.current_item = current_item
            
            # Update display if enough time has passed
            current_time = time.time()
            if current_time - self._last_update >= self._update_interval:
                self._update_display()
                self._last_update = current_time
                
                # Save checkpoint periodically
                self._save_checkpoint()
            
            # Call update callback if provided
            if self.update_callback:
                try:
                    self.update_callback(self.stats)
                except Exception as e:
                    logger.warning(f"Progress callback failed: {e}")
    
    def _update_display(self) -> None:
        """Update progress display."""
        if not self.enable_progress_bar:
            return
        
        try:
            # Simple text-based progress bar
            percentage = self.stats.completion_percentage
            bar_length = 40
            filled_length = int(bar_length * percentage / 100)
            bar = '█' * filled_length + '-' * (bar_length - filled_length)
            
            # Calculate metrics
            rate = self.stats.items_per_second
            eta = self.stats.eta_seconds
            
            # Format time
            def format_time(seconds: float) -> str:
                if seconds < 60:
                    return f"{seconds:.1f}s"
                elif seconds < 3600:
                    return f"{seconds/60:.1f}m"
                else:
                    return f"{seconds/3600:.1f}h"
            
            # Progress line
            progress_line = (
                f"\r{self.description}: |{bar}| "
                f"{self.stats.processed_items}/{self.stats.total_items} "
                f"({percentage:.1f}%) "
                f"Rate: {rate:.1f} items/s "
                f"ETA: {format_time(eta)}"
            )
            
            if self.stats.failed_items > 0:
                progress_line += f" Failed: {self.stats.failed_items}"
            
            print(progress_line, end='', flush=True)
            
        except Exception as e:
            logger.warning(f"Failed to update progress display: {e}")
    
    def finish(self) -> None:
        """Finish progress tracking."""
        with self._lock:
            if self.enable_progress_bar:
                print()  # New line after progress bar
            
            elapsed = self.stats.elapsed_time
            rate = self.stats.items_per_second
            
            logger.info(
                f"{self.description} completed: "
                f"{self.stats.processed_items} items processed, "
                f"{self.stats.failed_items} failed, "
                f"Rate: {rate:.1f} items/s, "
                f"Total time: {elapsed:.1f}s"
            )
            
            # Clean up checkpoint file
            if self.checkpoint_file and self.checkpoint_file.exists():
                try:
                    self.checkpoint_file.unlink()
                except Exception as e:
                    logger.warning(f"Failed to remove checkpoint file: {e}")
    
    def get_processed_items(self) -> int:
        """Get number of processed items (for resumability)."""
        return self.stats.processed_items
    
    def should_skip_item(self, item_index: int) -> bool:
        """Check if item should be skipped (already processed)."""
        return item_index < self.stats.processed_items


class SimpleProgressBar:
    """Simple progress bar for basic operations."""
    
    def __init__(self, total: int, description: str = "Progress"):
        self.total = total
        self.current = 0
        self.description = description
        self.start_time = time.time()
    
    def update(self, increment: int = 1):
        """Update progress by increment."""
        self.current += increment
        self._display()
    
    def _display(self):
        """Display current progress."""
        if self.total == 0:
            return
        
        percentage = (self.current / self.total) * 100
        bar_length = 40
        filled_length = int(bar_length * self.current / self.total)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        
        elapsed = time.time() - self.start_time
        rate = self.current / elapsed if elapsed > 0 else 0
        
        print(
            f"\r{self.description}: |{bar}| {self.current}/{self.total} "
            f"({percentage:.1f}%) {rate:.1f} items/s",
            end='',
            flush=True
        )
    
    def close(self):
        """Close progress bar."""
        print()  # New line