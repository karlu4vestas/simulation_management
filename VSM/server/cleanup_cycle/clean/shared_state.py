"""Manages shared state accessible by different modules and threads."""

import threading
from queue import Queue
import config # Use relative import

# --- Shared Counters ---
success_count = 0
fail_count = 0
count_lock = threading.Lock()

# --- Shutdown Control ---
shutdown_requested = threading.Event()

# --- Logging Queues ---
success_log_queue = Queue(maxsize=config.LOG_QUEUE_MAX_SIZE)
failure_log_queue = Queue(maxsize=config.LOG_QUEUE_MAX_SIZE)

# --- Utility Functions for Shared State ---

def increment_success():
    """Thread-safely increments the success counter."""
    global success_count
    with count_lock:
        success_count += 1

def increment_failure():
    """Thread-safely increments the failure counter."""
    global fail_count
    with count_lock:
        fail_count += 1

def get_counts() -> tuple[int, int]:
    """Thread-safely returns the current success and failure counts."""
    with count_lock:
        return success_count, fail_count

def add_success_log(file_path: str):
    """Adds a success entry to the log queue."""
    success_log_queue.put(file_path)
    increment_success()

def add_failure_log(log_entry: tuple):
    """Adds a failure entry (tuple) to the log queue."""
    failure_log_queue.put(log_entry)
    increment_failure()

def signal_loggers_to_finish():
    """Adds sentinel values to log queues to signal writers to stop."""
    success_log_queue.put(config.LOG_QUEUE_SENTINEL)
    failure_log_queue.put(config.LOG_QUEUE_SENTINEL)