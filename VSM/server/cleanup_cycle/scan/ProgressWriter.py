from abc import abstractmethod
import os
import sys
import time
import math
import RobustIO
from progress_reporter import ProgressReporter

class ProgressWriter(ProgressReporter):
    @staticmethod
    def as_date_time(timestamp):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

    def __init__(self, seconds_between_update:int, seconds_between_filelog:int):
        self.seconds_between_update = seconds_between_update
        self.seconds_between_filelog  = seconds_between_filelog
        self.start_time = time.time()
        self.next_time  = time.time()
        self.nb_last_processed_dirs = 0

    def open (self, output_archive:str):    
        if not RobustIO.IO.exist_path(output_archive):
            RobustIO.IO.create_folder(output_archive)

        self.logfile = os.path.join(output_archive,"progress_log.csv")
        RobustIO.IO.delete_file(self.logfile)
        self.logfile_handle = open(self.logfile,"a")
        self.logfile_handle.write("time;duration (min);enqueued;processed;current dirs/s;mean dirs/s;active threads\n")
        
    def close (self):
        #current_time = time.time()

        #write the final line to the log and close the file
        #self.update_log()
        self.logfile_handle.close()

        """#show progress on screen    
        seconds   = current_time - self.start_time 
        #remember to extract the stop signal
        processed_folders = self.param.nb_processed_folders.value 
        scan_rate = math.trunc( processed_folders/seconds + 0.5)       
        sys.stdout.write(f"\r{ProgressWriter.as_date_time(time.time())} Folders processed: {processed_folders} in { math.trunc(seconds + 0.5) } seconds "\
                         f"at a rate of {scan_rate} folders pr second          \n")
        sys.stdout.flush()    
        """
    def update (self, nb_processed_folders:int, io_queue_qsize:int, active_threads:int):

        current_time = time.time()
        diff_time = current_time - self.next_time 
        if diff_time >= self.seconds_between_filelog:
            self.next_time += self.seconds_between_filelog 

            processed_folder = nb_processed_folders
            run_time_sec     = current_time - self.start_time
            mean_dirs_second = math.trunc( processed_folder / run_time_sec + 0.5 )

            dirs_pr_second   = math.trunc( (processed_folder - self.nb_last_processed_dirs)/diff_time + 0.5 )
            self.nb_last_processed_dirs = processed_folder

            run_time_min     = math.trunc(run_time_sec/60+0.5)

            self.write_filelog(processed_folder, io_queue_qsize, active_threads, run_time_min, dirs_pr_second, mean_dirs_second)

        #show progress on screen    
        run_time_sec     = time.time() - self.start_time
        mean_dirs_second = math.trunc( nb_processed_folders / run_time_sec + 0.5 )
        self.write_realtime_progress (nb_processed_folders, mean_dirs_second, io_queue_qsize, active_threads)

    #@abstractmethod
    def write_realtime_progress (self, nb_processed_folders:int, mean_dirs_second:int, io_queue_qsize:int, active_threads:int):    
        #Write real-time progress to stdout.
        #This method can be overridden in subclasses to customize progress output.
        
        sys.stdout.write(f"\rFolders processed; pr second, queue_size, threads: {nb_processed_folders}; {mean_dirs_second}; {io_queue_qsize}; {active_threads}      ")
        sys.stdout.flush()

    #@abstractmethod
    def write_filelog (self, nb_processed_folders:int, io_queue_qsize:int, active_threads:int, run_time_min:int, dirs_pr_second:int, mean_dirs_second:int):
        #Write progress to the log file.
        #This method can be overridden in subclasses to customize log output.
        current_time = time.time()
        l = f"{ProgressWriter.as_date_time(current_time)};{run_time_min};{io_queue_qsize};{nb_processed_folders};"\
            f"{dirs_pr_second};{mean_dirs_second};{active_threads}\n"
        self.logfile_handle.write(l)
