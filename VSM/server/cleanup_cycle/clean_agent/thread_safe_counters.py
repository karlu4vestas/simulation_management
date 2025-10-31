"""
Thread-safe counter classes for tracking cleanup progress.
"""
from collections import namedtuple
from threading import Lock

# Named tuple for deletion counter values
DeletionCounts = namedtuple('DeletionCounts', ['files_deleted', 'bytes_deleted'])


class ThreadSafeCounter:
    """Generic thread-safe counter with lock."""
    
    def __init__(self):
        """Initialize counter to 0."""
        self._counter = 0
        self._lock = Lock()
    
    def increment(self, value: int = 1):
        """
        Increment the counter by the given value.
        
        Args:
            value: Amount to increment (default: 1)
        """
        with self._lock:
            self._counter += value
    
    def value(self) -> int:
        """
        Get the current counter value.
        
        Returns:
            Current counter value
        """
        with self._lock:
            return self._counter
    
    def change_value(self, new_value: int):
        """
        Set the counter to a new value.
        
        Args:
            new_value: New counter value
        """
        with self._lock:
            self._counter = new_value


class ThreadSafeDeletionCounter:
    """Thread-safe counter for tracking files and bytes deleted."""
    
    def __init__(self):
        """Initialize files and bytes counters to 0."""
        self._files_deleted = 0
        self._bytes_deleted = 0
        self._lock = Lock()
    
    def add(self, files_deleted: int, bytes_deleted: int):
        """
        Add to the files and bytes counters.
        
        Args:
            files_deleted: Number of files deleted
            bytes_deleted: Number of bytes deleted
        """
        with self._lock:
            self._files_deleted += files_deleted
            self._bytes_deleted += bytes_deleted
    
    def values(self) -> DeletionCounts:
        """
        Get current counter values.
        
        Returns:
            DeletionCounts: Named tuple with files_deleted and bytes_deleted
        """
        with self._lock:
            return DeletionCounts(self._files_deleted, self._bytes_deleted)
