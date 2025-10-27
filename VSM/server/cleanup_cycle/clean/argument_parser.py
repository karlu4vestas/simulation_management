# -*- coding: utf-8 -*-
"""
Argument parsing and input configuration for the SMB File Processor application.
"""

import argparse
import os
import sys
from pathlib import Path

import config
import logging_module


class FileProcessorInput:
    """Configuration class for file processor input parameters."""
    
    def __init__(self, mode: str, 
                 input_file_path: str, output_dir_path: str,
                 processed_log_path: str, failed_log_path: str, 
                 num_threads: int, 
                 count_files: bool
                 ):
        self.mode = mode
        self.input_file_path = input_file_path
        self.output_dir_path = output_dir_path
        self.num_threads = num_threads
        self.processed_log_path = processed_log_path
        self.failed_log_path = failed_log_path
        self.count_files = count_files


def get_input_from_args():
    """Parses arguments and returns the main application configuration."""
    parser = argparse.ArgumentParser(
        description="Windows CLI tool to process files listed in a CSV from SMB storage.",
        epilog=f"Example: python -m smb_file_processor.main --mode=delete --input=files.csv --threads={config.DEFAULT_THREADS}"
    )
    parser.add_argument(
        '--mode',
        required=True,
        choices=['noop', 'exist', 'delete'],
        help="Operation mode: 'noop' (dry run), 'exist' (check existence), 'delete' (remove files)."
    )
    parser.add_argument(
        '--input',
        required=True,
        type=str,
        help="Path to the input UTF-8 encoded CSV file (one column, double-quoted paths)."
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=config.DEFAULT_THREADS,
        help=f"Number of worker threads (default: {config.DEFAULT_THREADS})."
    )
    parser.add_argument(
        '--count_files',
        choices=['Yes', 'No', 'yes', 'no', 'YES', 'NO'],
        default='No',
        help="Count total files in the input CSV beforehand for accurate progress percentage (can be slow for large files)."
    )
    args = parser.parse_args()

    # --- Input Validation ---
    input_file_path = Path(args.input)
    if not input_file_path.is_file():
        print(f"Error: Input file not found or is not a file: {input_file_path}", file=sys.stderr)
        sys.exit(1)

    # Determine output directory based on input file's directory
    output_dir_path = input_file_path.parent
    try:
        # Test writability more robustly
        test_file = output_dir_path / f"~test_write_{os.getpid()}.tmp"
        with open(test_file, 'w') as f:
            f.write('test')
        test_file.unlink()
    except Exception as e:
        print(f"Error: Output directory '{output_dir_path}' is not writable or accessible: {e}", file=sys.stderr)
        sys.exit(1)

    num_threads = args.threads
    if num_threads <= 0:
        print("Error: Number of threads must be positive.", file=sys.stderr)
        sys.exit(1)

    mode = args.mode
    # Use mode itself as prefix for dry-run/exist logs, 'delete' for actual deletion log
    mode_prefix = mode  # Simplified based on requirement note

    count_files = True if not args.count_files is None and len(args.count_files) > 0 and args.count_files.lower() == "yes" else False

    # --- Setup Logging ---
    processed_log_path = output_dir_path / logging_module.LogManager.generate_log_filename(mode_prefix, config.PROCESSED_LOG_BASENAME)
    failed_log_path = output_dir_path / logging_module.LogManager.generate_log_filename(mode_prefix, config.FAILED_LOG_BASENAME)

    input_config = FileProcessorInput(
        mode=mode,
        input_file_path=input_file_path, 
        output_dir_path=output_dir_path,
        processed_log_path=processed_log_path, 
        failed_log_path=failed_log_path,
        num_threads=num_threads,
        count_files=count_files
    )
    return input_config
