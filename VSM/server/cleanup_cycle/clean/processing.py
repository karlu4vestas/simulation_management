"""Handles file operations (noop, exist, delete) and worker task logic."""

import os
import time
from pathlib import Path
from datetime import datetime

import config
import shared_state

def _log_failure(file_path: str, error_msg: str):
    """Internal helper to format and queue failure logs."""
    timestamp = datetime.now().strftime(config.LOG_DATETIME_FORMAT)
    shared_state.add_failure_log((file_path, error_msg, timestamp))

def perform_operation(file_path_str: str, mode: str) -> bool:
    """
    Performs the requested operation (noop, exist, delete) on the file.
    Includes retry logic. Returns True on success, False on failure.
    Logs failures internally.
    """
    last_exception = None
    for attempt in range(config.RETRY_COUNT):
        try:
            file_path = Path(file_path_str)
            if mode == 'noop':
                # Simulate success without touching the file system
                return True # Success
            elif mode == 'exist':
                if file_path.exists():
                    return True # Success
                else:
                    last_exception = FileNotFoundError(f"File not found: {file_path_str}")
                    break # Don't retry if file genuinely doesn't exist

            elif mode == 'delete':
                # Decision: Treat non-existence as an error for 'delete' as per TODO
                if not file_path.exists():
                     last_exception = FileNotFoundError(f"File not found (cannot delete): {file_path_str}")
                     break # File not found is an error state for delete now

                os.remove(file_path)
                # Decision: Remove the double-check for existence after delete as per TODO (potential perf hit)
                return True # Assume success if os.remove didn't raise Exception

            else:
                # This case should be unreachable due to argparse choices validation
                last_exception = ValueError(f"Internal Error: Invalid mode '{mode}' encountered.")
                break # No point retrying an invalid mode

        except Exception as e:
            # Catch unexpected errors during the operation attempt
            last_exception = e

        # If we reach here, an error occurred and we might retry
        if attempt < config.RETRY_COUNT - 1:
            # Check shutdown flag again before sleeping
            if shared_state.shutdown_requested.is_set():
                raise InterruptedError("Shutdown requested during retry delay")
            time.sleep(config.RETRY_DELAY_SECONDS)


    # If loop finished without returning True (or broke out with error), it's a failure.
    _log_failure(file_path_str, str(last_exception) if last_exception else "Unknown error after retries")
    return False # Failure