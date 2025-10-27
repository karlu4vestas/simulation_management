# smb_file_processor/config.py
# -*- coding: utf-8 -*-
"""Configuration constants for the SMB File Processor application."""

# Threading and Processing
DEFAULT_THREADS = 256
RETRY_COUNT = 3
RETRY_DELAY_SECONDS = 0.5

# Progress Reporting
PROGRESS_INTERVAL_SECONDS = 5

# Logging and CSV
LOG_DATETIME_FORMAT = "%Y-%m-%d_%H-%M"
CSV_ENCODING = 'utf-8-sig' #'utf-8' utf-8-sig should handle both files encoded af utf-8-sig and utf-8
LOG_QUEUE_MAX_SIZE = 10000  # Buffer size for log queue
LOG_WRITER_JOIN_TIMEOUT = 30 # Seconds to wait for log writers to finish

# File paths (Base names for logs)
PROCESSED_LOG_BASENAME = "processed_files"
FAILED_LOG_BASENAME = "failed_files"

# Log Headers
PROCESSED_LOG_HEADER = ['files']
FAILED_LOG_HEADER = ['files', 'error', 'datetime']

# Log Queue Sentinel
LOG_QUEUE_SENTINEL = None

# Job Queue Configuration
JOB_QUEUE_SIZE = 16*1024  # Control memory usage by limiting pending jobs