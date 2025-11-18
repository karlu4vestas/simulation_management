# Progress reporter base class and default implementation for cleanup operations.
# Follows the same pattern as ProgressReporter/ProgressWriter in the scan module.
from abc import ABC, abstractmethod
import os
import sys
import time
import math
from cleanup.scan import RobustIO
from cleanup.clean_agent.clean_parameters import CleanMeasures


class CleanProgressReporter(ABC):
    # Abstract base class for reporting cleanup progress.
    # Similar to ProgressReporter in the scan module.
    
    seconds_between_update: int = 5
    @abstractmethod
    def update(self, measures: CleanMeasures, deletion_queue_size: int, active_threads: int):
        pass

class CleanProgressWriter(CleanProgressReporter):
    # Default implementation for cleanup progress reporting.
    # Similar to ProgressWriter in the scan module.    
    # Writes progress to both console (realtime) and log file (periodic).
    # Can be subclassed to customize output behavior.
    
    @staticmethod
    def as_date_time(timestamp):
        #Convert timestamp to formatted date-time string.
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    
    def __init__(self, seconds_between_update: int, seconds_between_filelog: int):
        # Initialize progress writer.        
        # Args:
        #     seconds_between_update: Interval for console updates
        #     seconds_between_filelog: Interval for file log updates

        self.seconds_between_update = seconds_between_update
        self.seconds_between_filelog = seconds_between_filelog
        self.start_time = time.time()
        self.next_filelog_time = time.time()
        self.nb_last_processed_sims = 0
        self.logfile_handle = None
    
    def open(self, output_path: str):
        # Open log file for writing.
        # Args:
        #     output_path: Directory for log file
        
        if not RobustIO.IO.exist_path(output_path):
            RobustIO.IO.create_folder(output_path)
        
        self.logfile = os.path.join(output_path, "clean_progress_log.csv")
        RobustIO.IO.delete_file(self.logfile)
        self.logfile_handle = open(self.logfile, "a")
        self.logfile_handle.write("time;duration (min);processed;cleaned;issue;skipped;"
                                  "current sims/s;mean sims/s;deletion queue;active threads\n")
    
    def close(self):
        # Close log file
        if self.logfile_handle:
            self.logfile_handle.close()

    def update(self, measures: CleanMeasures, deletion_queue_size: int, active_threads: int):
        # Update progress - both console and file log.
        # Args:
        #     measures: CleanMeasures with all cleanup statistics
        #     deletion_queue_size: Current size of deletion queue
        #     active_threads: Number of active threads
        
        current_time = time.time()
        diff_time = current_time - self.next_filelog_time
        
        # Write to file log if interval has elapsed
        if self.logfile_handle and diff_time >= self.seconds_between_filelog:
            self.next_filelog_time += self.seconds_between_filelog
            
            run_time_sec = current_time - self.start_time
            mean_sims_second = math.trunc(measures.simulations_processed / run_time_sec + 0.5) if run_time_sec > 0 else 0
            
            current_sims_second = math.trunc((measures.simulations_processed - self.nb_last_processed_sims) / diff_time + 0.5) if diff_time > 0 else 0
            self.nb_last_processed_sims = measures.simulations_processed
            
            run_time_min = math.trunc(run_time_sec / 60 + 0.5)
            
            self.write_filelog(measures, deletion_queue_size, active_threads,
                              run_time_min, current_sims_second, mean_sims_second)
        
        # Always write to console
        run_time_sec = time.time() - self.start_time
        mean_sims_second = math.trunc(measures.simulations_processed / run_time_sec + 0.5) if run_time_sec > 0 else 0
        self.write_realtime_progress(measures, mean_sims_second, deletion_queue_size,
                                     active_threads)

    def write_realtime_progress(self, measures: CleanMeasures, mean_sims_second: int, deletion_queue_size: int, active_threads: int):
        # Write real-time progress to stdout.
        # This method can be overridden in subclasses to customize progress output.        
        # Args:
        #     measures: CleanMeasures with all cleanup statistics
        #     mean_sims_second: Average simulations per second
        #     deletion_queue_size: Current size of deletion queue
        #     active_threads: Number of active threads

        sys.stdout.write(
            f"\rProcessed: {measures.simulations_processed}; Cleaned: {measures.simulations_cleaned}; "
            f"Issue: {measures.simulations_issue}; Skipped: {measures.simulations_skipped}; "
            f"Rate: {mean_sims_second} sims/s; Queue: {deletion_queue_size}; "
            f"Threads: {active_threads}      "
        )
        sys.stdout.flush()

    def write_filelog(self, measures: CleanMeasures, deletion_queue_size: int, active_threads: int, run_time_min: int, current_sims_second: int, mean_sims_second: int):
        # Write progress to the log file.
        # This method can be overridden in subclasses to customize log output.        
        # Args:
        #     measures: CleanMeasures with all cleanup statistics
        #     deletion_queue_size: Current size of deletion queue
        #     active_threads: Number of active threads
        #     run_time_min: Runtime in minutes
        #     current_sims_second: Current simulations per second
        #     mean_sims_second: Mean simulations per second

        current_time = time.time()
        log_line = (
            f"{CleanProgressWriter.as_date_time(current_time)};"
            f"{run_time_min};"
            f"{measures.simulations_processed};"
            f"{measures.simulations_cleaned};"
            f"{measures.simulations_issue};"
            f"{measures.simulations_skipped};"
            f"{current_sims_second};"
            f"{mean_sims_second};"
            f"{deletion_queue_size};"
            f"{active_threads}\n"
        )
        self.logfile_handle.write(log_line)