import os
import sys
import time
import math
import RobustIO
from scanner import ScanParameters
from threading import active_count

#@TODO covert to log to a stream that we provide"
class ProgressWriter:
    @staticmethod
    def as_date_time(timestamp):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))

    def __init__(self, sleeptime, param:ScanParameters):
        self.sleeptime  = sleeptime
        self.start_time = time.time()
        self.next_time  = time.time()
        self.param = param
        self.nb_last_processed_dirs = self.param.nb_processed_folders.value
        
        self.logfile = os.path.join(param.output_archive,"log.csv")
        RobustIO.IO.delete_file(self.logfile)
        self.logfile_handle  = open(self.logfile,"a")
        self.logfile_handle.write("time;duration (min);enqueued;processed;current dirs/s;mean dirs/s\n")
        
    def close (self):
        current_time = time.time()

        #write the final line to the log and close the file
        self.update_log()
        self.logfile_handle.close()

        #show progress on screen    
        seconds   = current_time - self.start_time 
        #remember to extract the stop signal
        processed_folders = self.param.nb_processed_folders.value 
        scan_rate = math.trunc( processed_folders/seconds + 0.5)       
        sys.stdout.write(f"\r{ProgressWriter.as_date_time(time.time())} Folders processed: {processed_folders} in { math.trunc(seconds + 0.5) } seconds "\
                         f"at a rate of {scan_rate} folders pr second          \n")
        sys.stdout.flush()    


    def update (self):
        self.update_log()
        
        #show progress on screen    
        processed_folder = self.param.nb_processed_folders.value
        run_time_sec     = time.time() - self.start_time
        mean_dirs_second = math.trunc( processed_folder / run_time_sec + 0.5 )
        sys.stdout.write(f"\rFolders processed; pr second, threads: {processed_folder}; {mean_dirs_second}; {active_count()}      ")
        sys.stdout.flush()    
        
          
    def update_log (self):
            
        current_time = time.time()
        diff_time = current_time - self.next_time 
        if diff_time >= self.sleeptime:
            self.next_time += self.sleeptime 

            run_time_sec    = current_time - self.start_time
            run_time_min    = math.trunc(run_time_sec/60+0.5)

            processed_folder = self.param.nb_processed_folders.value
            dirs_pr_second   = math.trunc( (processed_folder - self.nb_last_processed_dirs)/diff_time + 0.5 )
            mean_dirs_second = math.trunc( processed_folder / run_time_sec + 0.5 )
            self.nb_last_processed_dirs = processed_folder

            l = f"{ProgressWriter.as_date_time(current_time)};{run_time_min};{self.param.io_queue.qsize()};{processed_folder};"\
                f"{dirs_pr_second};{mean_dirs_second}\n"
            self.logfile_handle.write(l)
      