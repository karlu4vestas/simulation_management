"""
Configuration and parameters for cleanup operations.
"""
from collections import namedtuple
from enum import Enum
from queue import Queue
from threading import Event
from cleanup.clean_agent.thread_safe_counters import DeletionCounts, ThreadSafeCounter, ThreadSafeDeletionCounter
from datamodel.dtos import FileInfo

# Named tuple for returning cleanup measures
CleanMeasures = namedtuple('CleanMeasures', [
    'simulations_processed',
    'simulations_cleaned',
    'simulations_issue',
    'simulations_skipped',
    'files_deleted',
    'bytes_deleted',
    'error_count'
])

class CleanMode(Enum):
    ANALYSE = "analyse"  # Count files/bytes but don't delete
    DELETE = "delete"    # Actually delete files

class CleanParameters:
    # Configuration and runtime state for cleanup operations.
    # Holds queues, counters, and control events for multi-threaded cleanup.
    
    def __init__(self, clean_mode: CleanMode, deletion_queue_max_size: int):
        #Initialize cleanup parameters.        
        #Args:
        #    clean_mode: ANALYSE or DELETE mode
        #    deletion_queue_max_size: Maximum size of deletion queue
        
        # Configuration of cleanup mode
        self.clean_mode = clean_mode
        
        # Queues
        self.simulation_queue:Queue[FileInfo] = Queue()  # Simulation paths to process
        self.file_deletion_queue:Queue[str] = Queue(maxsize=deletion_queue_max_size)  # Files to delete (bounded)
        self.processed_simulations_result_queue:Queue[FileInfo] = Queue()  # Results from simulation workers
        self.error_queue:Queue[tuple[str, str]] = Queue()  # Errors during processing
        
        # Control
        self.stop_event = Event()
        
        # Counters
        self.simulations_processed = ThreadSafeCounter()
        self.simulations_cleaned = ThreadSafeCounter()
        self.simulations_issue = ThreadSafeCounter()
        self.simulations_skipped = ThreadSafeCounter()
        self.deletion_measures = ThreadSafeDeletionCounter()
        self.error_count = ThreadSafeCounter()
    
    def get_measures(self) -> CleanMeasures:
        # Get all cleanup measures as a named tuple.        
        # Returns:
        #     CleanMeasures: Named tuple containing all cleanup statistics:
        #         - simulations_processed: Total simulations processed
        #         - simulations_cleaned: Simulations successfully cleaned
        #         - simulations_issue: Simulations with issues
        #         - simulations_skipped: Simulations skipped
        #         - files_deleted: Total files deleted
        #         - bytes_deleted: Total bytes deleted
        #         - error_count: Total errors encountered
        # Thread-safety notes:
        #     Each individual counter read is thread-safe (protected by its own lock).
        #     However, the overall snapshot is NOT atomic across all counters - values may
        #     be read at slightly different points in time if worker threads are actively
        #     updating counters.
        #             
        #     This is acceptable because:
        #     1. During execution: Used by progress monitor for periodic updates where
        #        approximate values are sufficient to show that work is progressing.
        #        Perfect consistency is not required for progress reporting.
        #   
        #     2. Final report: Called after all worker threads have stopped (after join()),
        #        so no concurrent modifications occur and the snapshot is consistent.
        #    
        #     The non-atomic reads provide a good balance between simplicity and the
        #     actual requirements of the system.
        
        deletions:DeletionCounts = self.deletion_measures.values()        
        return CleanMeasures(
            simulations_processed=self.simulations_processed.value(),
            simulations_cleaned=self.simulations_cleaned.value(),
            simulations_issue=self.simulations_issue.value(),
            simulations_skipped=self.simulations_skipped.value(),
            files_deleted=deletions.files_deleted,
            bytes_deleted=deletions.bytes_deleted,
            error_count=self.error_count.value()
        )