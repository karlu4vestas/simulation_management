# smb_file_processor/logging_module.py
# -*- coding: utf-8 -*-
"""Handles log file generation and writing."""

import csv
import sys
import threading
from datetime import datetime
from pathlib import Path
from queue import Queue

import config
import shared_state


class LogManager:
    """Manages logging operations including thread management for log writers."""
    
    def __init__(self, processed_log_path: Path, failed_log_path: Path):
        """Initialize the LogManager with log file paths."""
        self.processed_log_path = processed_log_path
        self.failed_log_path = failed_log_path
        self.success_logger_thread = None
        self.failure_logger_thread = None
    
    @staticmethod
    def generate_log_filename(prefix: str, base_name: str) -> str:
        """Generates a log filename with mode prefix and timestamp."""
        now_str = datetime.now().strftime(config.LOG_DATETIME_FORMAT)
        return f"{prefix}-{base_name}-{now_str}.csv"
    
    def start_logging(self):
        """Start the logger threads."""
        # Start log writer threads
        self.success_logger_thread = threading.Thread(
            target=self._log_writer,
            args=(self.processed_log_path, config.PROCESSED_LOG_HEADER, shared_state.success_log_queue),
            name="SuccessLogWriter",
            daemon=True  # Daemons allow exit even if running, but we will join explicitly
        )
        self.failure_logger_thread = threading.Thread(
            target=self._log_writer,
            args=(self.failed_log_path, config.FAILED_LOG_HEADER, shared_state.failure_log_queue),
            name="FailureLogWriter",
            daemon=True
        )
        self.success_logger_thread.start()
        self.failure_logger_thread.start()
    
    def stop_logging(self):
        """Stop the logger threads and wait for them to finish."""
        # Signal log writers to finish
        shared_state.signal_loggers_to_finish()

        # Wait for log writers to finish processing their queues
        if self.success_logger_thread and self.success_logger_thread.is_alive():
            self.success_logger_thread.join(timeout=config.LOG_WRITER_JOIN_TIMEOUT)
        if self.failure_logger_thread and self.failure_logger_thread.is_alive():
            self.failure_logger_thread.join(timeout=config.LOG_WRITER_JOIN_TIMEOUT)

        # Check if loggers finished cleanly
        if self.success_logger_thread and self.success_logger_thread.is_alive():
            print("Warning: Success log writer thread did not exit cleanly.", file=sys.stderr)
        if self.failure_logger_thread and self.failure_logger_thread.is_alive():
            print("Warning: Failure log writer thread did not exit cleanly.", file=sys.stderr)
    
    def _log_writer(self, log_path: Path, header: list[str], log_queue: Queue):
        """Writes logs from a queue to a CSV file."""
        written_count = 0
        #print(f"Log writer starting for {log_path.name}...")
        try:
            with open(log_path, 'w', newline='', encoding=config.CSV_ENCODING, buffering=2**17) as csvfile:
                writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                writer.writerow(header)
                while True:
                    item = log_queue.get()
                    if item is config.LOG_QUEUE_SENTINEL:
                        log_queue.task_done()
                        break

                    # Ensure item is a list/tuple for writerow
                    try:
                        if isinstance(item, str): # Success log
                            writer.writerow([item])
                        elif isinstance(item, (list, tuple)): # Failure log
                            writer.writerow(item)
                        else:
                            print(f"Warning: Unknown item type in log queue for {log_path.name}: {type(item)}", file=sys.stderr)
                        written_count += 1
                    except csv.Error as csv_e:
                         print(f"Error: CSV writing error in {log_path.name} for item {item}: {csv_e}", file=sys.stderr)
                         # Optionally log this error to the other log file or stderr
                    except Exception as write_e:
                         print(f"Error: Unexpected writing error in {log_path.name} for item {item}: {write_e}", file=sys.stderr)

                    log_queue.task_done() # Mark task as done after processing

        except OSError as e:
            print(f"\nError: Critical OS error opening/writing to log file {log_path}: {e}", file=sys.stderr)
            shared_state.shutdown_requested.set() # Trigger shutdown
        except Exception as e:
            print(f"\nError: Critical unexpected error in log writer for {log_path}: {e}", file=sys.stderr)
            shared_state.shutdown_requested.set() # Trigger shutdown
        finally:
            # Ensure queue is marked done even if exceptions occurred before sentinel
            if item is not config.LOG_QUEUE_SENTINEL:
                log_queue.task_done()
            #print(f"Log writer for {log_path.name} finished. Wrote {written_count} entries.")
    

