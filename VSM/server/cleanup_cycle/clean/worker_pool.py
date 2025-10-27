import csv
import sys
import threading
from queue import Queue
from typing import List

import config
import shared_state
import processing
from datetime import datetime

def _log_failure(file_path: str, error_msg: str):
    """Internal helper to format and queue failure logs."""
    timestamp = datetime.now().strftime(config.LOG_DATETIME_FORMAT)
    shared_state.add_failure_log((file_path, error_msg, timestamp))

class WorkerPool:
    def __init__(self, num_workers: int, mode: str):
        self.job_queue = Queue(maxsize=config.JOB_QUEUE_SIZE)
        self.workers: List[threading.Thread] = []
        self.mode = mode
        self.num_workers = num_workers
        self.total_files_read = 0
        
    def _worker_loop(self):
        """Long running worker that processes jobs until shutdown"""
        while not shared_state.shutdown_requested.is_set():
            try:
                file_path = self.job_queue.get()
                if file_path is None:  # Sentinel value
                    break
                success = processing.perform_operation(file_path, self.mode)
                if success:
                    # Log success (which also increments counter via shared_state)
                    shared_state.add_success_log(file_path)
            except Exception as e:
                # just log all exceptions and move on to the next file
                _log_failure(file_path, f"Unhandled worker exception: {e}")

    def start(self):
        """Start the worker threads"""
        for i in range(self.num_workers):
            worker = threading.Thread( target=self._worker_loop, name=f"Worker-{i}", daemon=True )
            self.workers.append(worker)
            worker.start()
            
    def feed_jobs(self, csv_path: str):
        """Read CSV and feed jobs to workers"""
        try:
            with open(csv_path, 'r', encoding=config.CSV_ENCODING, newline='', buffering=2**16) as csvfile:
                reader = csv.reader(csvfile, quoting=csv.QUOTE_MINIMAL)
                
                for row in reader:
                    if shared_state.shutdown_requested.is_set():
                        break
                        
                    if row and row[0].strip():
                        self.job_queue.put(row[0].strip())
                        self.total_files_read += 1
                        
        except Exception as e:
            print(f"\nError: Failed to read input file: {e}", file=sys.stderr)
            shared_state.shutdown_requested.set()
            
    def shutdown(self):
        """Signal workers to stop and wait for completion"""
        # Send sentinel values to stop workers
        for _ in self.workers:
            self.job_queue.put(None)
            
        # Wait for workers to finish
        for worker in self.workers:
            worker.join(timeout=5.0)