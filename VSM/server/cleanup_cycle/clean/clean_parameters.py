import os
import re
import time
import queue
import datetime
#from concurrent.futures import ThreadPoolExecutor
from threading import Event
from clean_writer_tasks import FileDeletionMethod, ThreadSafeCounter, ThreadSafeDeletionCounter


class clean_parameters:
    params=None

    def __init__(self, base_folder:str, file_deletion_method:FileDeletionMethod = FileDeletionMethod.Analyse, cleaner_threads = 6, deletion_threads:int=2048):
        #shall we analyse or du the real deletions
        self.file_deletion_method = file_deletion_method

        #with htc
        self.folder_exclusions    = r"(htc)"  
        self.re_folder_exclusions = re.compile( self.folder_exclusions, re.IGNORECASE )

        self.min_date = datetime.datetime(2016, 1, 1, 00, 00)
        self.max_date = datetime.datetime(2024, 4, 1, 00, 00)

        self.vts_standard_folders   = set( [name.casefold() for name in ["INPUTS","DETWIND","EIG","INT","LOG",   "OUT","PARTS","PROG","STA"] ] )
        
        #old hawc2 simulation can have "log" instead of "logfiles", "htc" instead of "htcfiles"
        #if there is a "master" folder then its files should be considered as input too
        self.hawc2_standard_folders = set( [name.casefold() for name in ["control", "htcfiles","inc","inputs",                "int","Logfiles",                   "STA"] ] ) 
         
           
        #Queues
        str_now = time.strftime("%Y-%m-%d %H-%M-%S-", time.gmtime())        

        self.progress_path  = os.path.join(base_folder, str_now+"progress.csv" )
        self.progress_file_handle  = None 

        self.file_error_path  = os.path.join(base_folder, str_now+"file_msg.csv" )
        self.file_error_queue = queue.Queue() 
        
        self.folder_error_path  = os.path.join(base_folder, str_now+"path_msg.csv" )
        self.folder_error_queue = queue.Queue()

        self.deletion_log_path  = os.path.join(base_folder, str_now+"deletion_log.csv" )
        self.deletion_log_queue = queue.Queue()

        #The queue size is set to hold all files to be cleaned for a big simulation with 32.000 timeseries, there can be up to 8 extension to clean. 
        self.file_deletion_queue = queue.Queue( 10^6 ) 

        #all folders to be analysed
        self.folder_scan_queue = queue.Queue()
        self.folder_scanner_threads = None

        #all analysed folders
        self.scan_log_path  = os.path.join(base_folder, str_now+"scan_log.csv" )
        self.scan_log_queue = queue.Queue()

        self.stop_event = Event()

        #counter
        self.file_errors         = ThreadSafeCounter()
        self.folder_errors       = ThreadSafeCounter()
        self.folder_count        = ThreadSafeCounter()
        self.folder_count_total  = ThreadSafeCounter()
        
        self.deletionMeasures    = ThreadSafeDeletionCounter() 
        self.folders_processed   = ThreadSafeCounter()
        self.sim_processed       = ThreadSafeCounter()
        self.sim_cleaned         = ThreadSafeCounter()
        self.sim_already_cleaned = ThreadSafeCounter()
        self.sim_irreproducible  = ThreadSafeCounter()
        self.sim_ignored         = ThreadSafeCounter()
        self.folder_root_dict = {}
        self.start_time = time.time() 
        clean_parameters.params = self           
    
    def set_min_max_date(self, min_date, max_date):
        self.min_date = min_date
        self.max_date = max_date

    def close_logStatus(self):
        raise NotImplementedError

    def start_worker(self, no_folder_scanner_threads, no_deletions_threads):
        raise NotImplementedError

    def stop_workers(self):
        raise NotImplementedError
