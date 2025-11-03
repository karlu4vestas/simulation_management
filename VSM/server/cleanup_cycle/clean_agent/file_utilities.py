import numpy as np

def get_size_stat_from_file_entries(file_entries: dict):
    # Compute size and timestamp statistics from a dictionary of file entries.
    
    # Args:
    #     file_entries: Dictionary where values are lists of DirEntry objects (files)
    
    # Returns:
    #     Tuple of (bytes, count_files, max_date, min_date, file_entries):
    #     - bytes_: Total size in bytes across all files
    #     - count_files: Total number of files
    #     - max_date: Most recent modification timestamp (or None)
    #     - min_date: Oldest modification timestamp (or None)
    #     - file_entries: The input dictionary (passed through)
    
    # Note:
    #     - Timestamps after Year 2038 problem threshold (2^31 - 1) are excluded from date calculations
    #     - Uses numpy for efficient computation of sums, min, and max
    
    max_time = float(2**31 - 1)  # last valid timestamp https://en.wikipedia.org/wiki/Year_2038_problem

    bytes_, count_files, max_date, min_date, file_entries = 0, 0, None, None, file_entries
    
    files_counts = [len(entries) for entries in file_entries.values()]
    count_files = sum(files_counts)
    files_stat = [file.stat() for entries in file_entries.values() for file in entries]
    count_files = len(files_stat)
    if count_files > 0:
        file_bytes = np.fromiter((state.st_size for state in files_stat), dtype=np.int64)
        bytes_ = np.sum(file_bytes)

        file_timestamp = [state.st_mtime for state in files_stat if state.st_mtime < max_time]  # have to create a list with valid timestamps first
        if len(file_timestamp) > 0:
            file_timestamp = np.fromiter(file_timestamp, dtype=np.float64)
            max_date = np.max(file_timestamp)
            min_date = np.min(file_timestamp)

    return bytes_, count_files, max_date, min_date, file_entries
