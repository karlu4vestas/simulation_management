# smb_file_processor/main.py
# -*- coding: utf-8 -*-
"""
Main entry point for the SMB File Processor application.

Handles CLI arguments, orchestrates processing, logging, progress reporting,
and graceful shutdown.
"""

import csv
import sys
import signal
import threading
import time

import config
import shared_state
from logging_module import LogManager
import progress
from worker_pool import WorkerPool
from argument_parser import FileProcessorInput, get_input_from_args


worker_pool = None # Global variable to hold the worker pool instance

# --- Signal Handling ---
def signal_handler(signum, frame):
    """Handles Ctrl+C or other termination signals."""
    if shared_state.shutdown_requested.is_set():
        print("\nWarning: Multiple shutdown signals received. Forcing exit might be required.", file=sys.stderr)
        return
    print("\nInfo: Shutdown signal received. Requesting graceful shutdown...", file=sys.stdout)
    print("Info: No new tasks will be started. Waiting for active tasks to complete...", file=sys.stdout)
    print("Info: Progress may update less frequently during shutdown.", file=sys.stdout)
    shared_state.shutdown_requested.set()
    global worker_pool
    worker_pool.shutdown(wait_for_completion=True) # Wait for active tasks to finish

# --- Main Execution ---
def run(input:FileProcessorInput):

    print(f"--- Starting File Processor ---")
    print(f"Mode:           {input.mode}")
    print(f"Input CSV:      {input.input_file_path}")
    print(f"Output Dir:     {input.output_dir_path}")
    print(f"Worker Threads: {input.num_threads}")
    print(f"Processed Log:  {input.processed_log_path}")
    print(f"Failed Log:     {input.failed_log_path}")
    print(f"Retries:        {config.RETRY_COUNT} attempts per file")
    print(f"Retry Delay:    {config.RETRY_DELAY_SECONDS} seconds")
    print(f"Count Files:    {'Yes' if input.count_files else 'No'}")
    print("-" * 30, flush=True)

    total_files_estimate = None
    if input.count_files:
        try:
            print("Counting total files in input CSV (this might take a while)...", end='\r', file=sys.stdout, flush=True)
            count_start_time = time.monotonic()
            with open(input.input_file_path, 'r', encoding=config.CSV_ENCODING, newline='', buffering=2**18) as infile:
                 # Use generator expression for memory efficiency
                 total_files_estimate = sum(1 for row in csv.reader(infile, quoting=csv.QUOTE_MINIMAL, quotechar='"') if row and row[0].strip())
            count_duration = time.monotonic() - count_start_time
            print(f"Count complete: Found {total_files_estimate} files to process in {count_duration:.2f}s.")
        except Exception as e:
            print(f"Warning: Could not count total files: {e}. Progress percentage will be unavailable.", file=sys.stderr)
            total_files_estimate = None # Reset on error

    # --- Initialize Shared State (Reset counters just in case) ---
    shared_state.success_count = 0
    shared_state.fail_count    = 0
    shared_state.shutdown_requested.clear() # Ensure flag is initially False

    start_time = time.monotonic() #start timer here so that we get the correct number of files pr seconds faster. ie including the read of the input file result in a wrong average speed 

    # Start log writer threads
    logging = LogManager(input.processed_log_path, input.failed_log_path)
    logging.start_logging()

    # --- Setup Signal Handling ---
    signal.signal(signal.SIGINT, signal_handler)  # Handle Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler) # Handle termination signal (e.g., task manager)

    # --- Main Processing Logic ---
    total_files_read = 0

    # Start progress reporter thread AFTER potential file count
    progress_thread = threading.Thread(
        target=progress.progress_reporter,
        args=(start_time, total_files_estimate),
        name="ProgressReporter",
        daemon=True
    )
    progress_thread.start()

     # Initialize and start worker pool
    global worker_pool 
    worker_pool = WorkerPool(input.num_threads, input.mode)
    worker_pool.start()
    try:
        # Feed jobs from CSV file
        worker_pool.feed_jobs(input.input_file_path)
        
    except Exception as e:
        print(f"\nError: Unhandled exception during processing: {e}", file=sys.stderr)
        shared_state.shutdown_requested.set()
    finally:
        # --- Finalization ---

        # Ensure shutdown flag is set if we exited loop unexpectedly
        worker_pool.shutdown()
        shared_state.shutdown_requested.set()
        # Use actual count from job feeding
        total_files_read = worker_pool.total_files_read

        # Stop the progress reporter thread (it checks the flag, but signal it)
        # Wait for progress reporter first, as it relies on counts
        if progress_thread and progress_thread.is_alive():
            #print("Info: Waiting for progress reporter to finish...", flush=True)
            progress_thread.join(timeout=config.PROGRESS_INTERVAL_SECONDS * 2)
            if progress_thread.is_alive():
                 print("Warning: Progress thread did not exit cleanly.", file=sys.stderr)

        # Signal log writers to finish
        shared_state.signal_loggers_to_finish()

        logging.stop_logging()

        end_time = time.monotonic()
        total_time = end_time - start_time
        final_success_count, final_fail_count = shared_state.get_counts()

        print("-" * 30, flush=True)
        print(f"Total files read from CSV: {total_files_read}")
        print(f"Successfully processed:    {final_success_count}")
        print(f"Failed operations:         {final_fail_count}")
        print(f"Total execution time:      {total_time:.2f} seconds")
        print(f"Results logged to:")
        print("-" * 30, flush=True)

        # Exit with appropriate code
        sys.exit(0 if final_fail_count == 0 else 1)

if __name__ == "__main__":
    # The program can be run with different modes=(noop, exist, delete), input file, threads, and count_files (number of files to preocess): 
    # python main.py --mode=delete --input="C:/Users/karlu/Downloads/vts_clean_data/cleanup_test/cleanup_input.csv"                                                                                                                     
    # python main.py --mode=delete --input="C:/Users/karlu/Downloads/vts_clean_data/cleanup_test/cleanup_input.csv" --threads=2 --count_files=Yes                                                                                                                      

    # Add check for Python version if strictly needed
    if sys.version_info < (3, 13):
        print("Warning: This script is specified for Python 3.13+. Behavior on older versions may differ.", file=sys.stderr)

    input = get_input_from_args()
    run(input)

"""
def mainrun():
    from pathlib import Path
    mode = "delete" # "exist"
    mode_prefix = mode
    input_file_path     = Path( "C:/Users/karlu/Downloads/vts_clean_data/test/cleanup_input.csv" ).resolve()
    output_dir_path     = input_file_path.parent
    processed_log_path  = output_dir_path / LogManager.generate_log_filename(mode, config.PROCESSED_LOG_BASENAME)
    failed_log_path     = output_dir_path / LogManager.generate_log_filename(mode_prefix, config.FAILED_LOG_BASENAME)
    num_threads         = 1
    count_files         = True

    input=FileProcessorInput(mode=mode,
                             input_file_path=input_file_path, output_dir_path=output_dir_path,
                             processed_log_path=processed_log_path, failed_log_path=failed_log_path,
                             num_threads=num_threads,
                             count_files=count_files)    
    run(input)

mainrun()
""" 