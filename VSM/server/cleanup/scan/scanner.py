import os
import sys 
import time
import numpy as np
from datetime import datetime
from queue import Queue
from threading import Event, Thread
from multiprocessing.sharedctypes import Value
#from concurrent.futures import ThreadPoolExecutor
from cleanup.scan import RobustIO
from cleanup.scan.progress_reporter import ProgressReporter
#from file_owner import FileOwner

def as_date_time(time): return datetime.fromtimestamp(time).strftime('%Y-%m-%d_%H-%M-%S')

class ScanIO:
    def __init__( self, folder:str, scanning_output:Queue[str], error_queue:Queue[str] ):
        self.folder:str               = folder          #the folder to scan
        self.output_queue: Queue[str] = scanning_output #where to place the output from the scan
        self.error_queue: Queue[str]  = error_queue     #where to place the error from the scanning

class ScanPathConfig:
    #aggregate 
    # scan_path: which is the in rootfolder to be scanned
    # output_root: the main folder wher output for all rootfolder must be oplace under the name of the rootfolder
    # scan_output_folder: output_root + name of scan_path
    
    # scanio: containt the folder to be scanned recurisvely and its output queue
    # error_queue: the queue where the error logs should be placed
    # output_queue: the queue where the scan results should be placed

    scan_path:str = None
    output_root:str = None
    scan_output_folder:str = None
    scan_output_file:str = None
    scan_output_errorlog_file:str = None

    scanio:ScanIO=None
    output_queue:Queue[str]=Queue()
    error_queue:Queue[str]=Queue()

    def __init__(self, scan_path:str, output_root:str):
        self.scan_path = scan_path
        self.output_root = output_root

        # folder where both the scan results and the error log will be stored
        scan_folder_name:str        = os.path.basename(scan_path)
        self.scan_output_folder:str = os.path.join( output_root, scan_folder_name)
        if not RobustIO.IO.exist_path(self.scan_output_folder) :
            RobustIO.IO.create_folder(self.scan_output_folder)
            if not RobustIO.IO.exist_path(self.scan_output_folder) :
                raise FileNotFoundError(f"Failed to create output folder: {self.scan_output_folder}")

        self.scan_output_file          = os.path.join(self.scan_output_folder, as_date_time(time.time())+"_scan_results.csv")
        self.scan_output_errorlog_file = os.path.join(self.scan_output_folder, as_date_time(time.time())+"_scan_errors.csv")

        self.scanio = ScanIO(scan_path, self.output_queue, self.error_queue)

    def start_error_thread(self):
        self.error_writer_thread = Thread(target=ScanPathConfig.error_file_writer_task, args=(self.scan_output_errorlog_file, self.error_queue, ), daemon=True) 
        self.error_writer_thread.start()

    def start_scan_writer_threads(self):
        self.output_writer_thread = Thread(target=ScanPathConfig.file_writer_task, args=(self.scan_output_file, self.output_queue, ), daemon=True) 
        self.output_writer_thread.start()
    
    @staticmethod
    def stop( scanpath_configs:list["ScanPathConfig"] ):
        #send signal to file_writers that scan of all folders are done
        for spc in scanpath_configs:
            spc.output_queue.put(None)
            spc.error_queue.put(None)

        for spc in scanpath_configs:
            spc.output_queue.join()
            spc.error_queue.join()

        for spc in scanpath_configs:
            spc.output_writer_thread.join()
            spc.error_writer_thread.join()

    @staticmethod
    def file_writer_task(filepath:str, queue:Queue[str]):
        with open(filepath, 'w', encoding="utf-8", buffering= 2**18 ) as file:
            file.writelines( f"\"folder\";\"min_modified\";\"max_modified\";\"min_accessed\";\"max_accessed\";\"files\"\n" )        
            while True:
                line = queue.get()
                if line is None:
                    file.flush()
                    queue.task_done()
                    break
                else:
                    file.write(line)
                    queue.task_done()

    @staticmethod
    def error_file_writer_task(filepath:str, queue:Queue[str]):
        with open(filepath, 'w', encoding="utf-8", buffering= 2**18 ) as file:
            #file.writelines( f"\"folder\";\"exception(ns)\"\n" )                
            file.writelines( f"\"folder\";\"sys.info[0]\";\"sys.info[1]\";\"sys.info[2]\";\"lineno\"\n" )                
            while True:
                line = queue.get()
                if line is None:
                    file.flush()
                    queue.task_done()
                    break
                else:
                    file.write(line)
                    queue.task_done()



class ScanParameters:
    def __init__(self):
        self.scanpath_config:list[ScanPathConfig] = [] 
        self.io_queue: Queue[ScanIO] = Queue() 
        self.nbScanners:int = 0                  # number of threads used to scan input folders
        self.scan_is_done_event:Event = Event()  # used by the scanner to signal to the main loop that scanning is done
        self.scan_abort_event:Event = Event()    # can be used to signal that the scanning must abort
        self.nb_processed_folders: Value = Value('i', 0)        #number of threads to use to get file owners. 0 means no owner retrieval


class Scanner:

    @staticmethod
    def start( param:ScanParameters ):
        #start error and output writer threads for each scanpathconfig
        # collect all scanio for that they can be processed by a shared set of scanner threads
        for spc in param.scanpath_config:
            spc.start_error_thread()
            spc.start_scan_writer_threads()
            param.io_queue.put( spc.scanio )

        #Create a fixed set of thread to scan the all folders starting with the topfolders 
        #owner_pools = [None if param.owner_threads == 0 else ThreadPoolExecutor(param.owner_threads) for i in range(param.nbScanners) ]
        threads = [Thread(target=Scanner.getDirs_task, 
                          args=(param.io_queue, param.scan_abort_event, param.nb_processed_folders, 
                                #owner_pools[i],
                                  ), 
                          daemon=True ) 
                    for i in range(param.nbScanners)]
        for thread in threads:
            thread.start()

        # Wait for all scanner threads to finish processing the queue.
        # NOTE: If scan_abort_event is set, scanner threads will stop processing without
        # calling task_done(), causing this to hang. However, Scanner.stop() checks the
        # abort event and skips this join() in that case. The daemon threads will be
        # automatically terminated when the parent thread/process exits.
        param.io_queue.join()

        #no stop the output queues and writers  
        Scanner.stop( param )
        

    @staticmethod
    def stop( param:ScanParameters ):
        # wait until all input folders are done
        # is the scanning aborted the we must not wait because the scanner has stopped processing the input queue
        if not param.scan_abort_event.is_set():
            param.io_queue.join()

        # wait for the output to finish
        ScanPathConfig.stop( param.scanpath_config )

        # signal to the main thread that all scanning and output processing is done
        param.scan_is_done_event.set()


    @staticmethod
    def timestamp_statistics( extract_timestamp_function ):
        # function to handle calculation of min, max and string format for both valid and invalid timestamps
        # the timestamps are provide as a function to extract timestamps 
              
        #create valid array of timestamps before we take min and max
        max_time = float(2**31 - 1) # last validt timestamp https://en.wikipedia.org/wiki/Year_2038_problem
        min_date, max_date, str_date_array = "", "", [] 
        try:
            timestamps       = extract_timestamp_function()
            valid_timestamps = [t for t in timestamps if t < max_time] 
            valid_timestamps = np.array(valid_timestamps, dtype=np.float64 ) 
            min_date         = np.min(valid_timestamps) if len(valid_timestamps)> 0 else None
            max_date         = np.max(valid_timestamps) if len(valid_timestamps)> 0 else None
                            
            #get all data as strings
            if not min_date is None:
                min_date = time.strftime("%Y-%m-%d", time.localtime(min_date))
                max_date = time.strftime("%Y-%m-%d", time.localtime(max_date))
            else:
                min_date = max_date = ""               
                            
            str_date_array   = [time.strftime("%Y-%m-%d", time.localtime( t if t < max_time else max_time ) ) for t in timestamps]
        except :
            min_date, max_date, str_date_array = "", "", [] 

        return min_date, max_date, str_date_array


    # get next the files in the directory and enqueue its subfolders 
    @staticmethod
    def getDirs_task( io_queue:Queue[ScanIO], scan_abort_event:Event, nb_dirs_processed:Value, max_failure:int=3):
        #def getDirs_task( io_queue:Queue[ScanIO], stop_event:Event, nb_dirs_processed:int, owner_threadpool:ThreadPoolExecutor, max_failure:int=3):

        while True and not scan_abort_event.is_set():                
            sio  = io_queue.get()
            folder = sio.folder
            error_queue = sio.error_queue
            failures, succes = 0, False
            
            # ignore paths that have disapperared between the moment they were enqueue and now
            if not RobustIO.IO.exist_path(folder): 
                failures = max_failure
                error_message = f"{folder};deleted between time of detection and scanning\n"
                error_queue.put(error_message)
            
            while failures < max_failure and not succes:
                try:
                    with os.scandir(folder) as ite:    
                        entries: list[os.DirEntry] = [ entry for entry in ite ]
                        
                    files: list[os.DirEntry]          = [ entry for entry in entries if entry.is_file(follow_symlinks=False) ]
                    file_states: list[os.stat_result] = [ f.stat(follow_symlinks=False) for f in files ]

                    if len(files) > 0:

                        min_modified, max_modified, str_modify_dates   = Scanner.timestamp_statistics( lambda : [state.st_mtime     for state in file_states]     )
                        min_accessed, max_accessed, str_accessed_dates = Scanner.timestamp_statistics( lambda : [state.st_atime     for state in file_states]     )
                        str_file_names = [f.name for f in files]
                        str_file_bytes = [str(s.st_size) for s in file_states]
                        str_owners     =  [""] * len(files) #str_owners     =  [""] * len(files) if owner_threadpool is None else FileOwner.getOwners( files, owner_threadpool) 

                        if not(len(str_modify_dates)==len(str_accessed_dates) and 
                               len(str_modify_dates)==len(str_file_names)     and len(str_modify_dates)==len(str_file_bytes) ):
                            msg           = f"length issue: {len(str_modify_dates)}, {len(str_accessed_dates)}, {len(str_file_names)}, {len(str_file_bytes)}, {len(str_owners)}"
                            error_message = f"\"{folder}\";\"{msg}\";\"\";\"\";\"\"\n"
                            error_queue.put(error_message)
                        else:
                            #save data to th following positions
                            #0: str_file
                            #1: str_modify
                            #2: str_access
                            #3: str_create
                            #4: str_byte
                            #5: str_owner
                            files = [ f"{str_file}\x00{str_modify}\x00{str_access}\x00{str_byte}\x00{str_owner}" 
                                      for str_file, str_modify, str_access, str_byte, str_owner in zip(str_file_names,str_modify_dates,str_accessed_dates,str_file_bytes, str_owners) ]
                            files = "\x00\x00".join(files) 
                            #print( files )

                        if folder[0:8]=="\\\\?\\UNC\\": folder = "\\\\"+folder[8:]   
                        sio.output_queue.put(f"\"{folder}\";\"{min_modified}\";\"{max_modified}\";\"{min_accessed}\";\"{max_accessed}\";\"{files}\"\n")
                    else:
                        # get the following attributes for the folder and store them in the same ways as the files 
                        folder_stats = [os.stat(folder)]
                        min_modified, max_modified, str_modify_dates   = Scanner.timestamp_statistics( lambda : [state.st_mtime     for state in folder_stats]     )
                        min_accessed, max_accessed, str_accessed_dates = Scanner.timestamp_statistics( lambda : [state.st_atime     for state in folder_stats]     )
                        sio.output_queue.put(f"\"{folder}\";\"{min_modified}\";\"{max_modified}\";\"{min_accessed}\";\"{max_accessed}\";\"\"\n")

                    #add new directories AFTER handling this directories files so that the queue of directories to scan increases more slowly than if we added it before
                    dirs = [ entry for entry in entries if entry.is_dir(follow_symlinks=False) ]
                    for entry in dirs:
                        io_queue.put( ScanIO( entry.path, sio.output_queue, sio.error_queue ) )

                    nb_dirs_processed.value +=1
                    succes = True
                except Exception as e:
                    failures = failures + 1
                    if failures < max_failure:
                        time.sleep(1)
                    else:
                        if sys.exc_info() is None:
                            error_message = f"\"{folder}\";\"{str(e)}\";\"\";\"\";\"\"\n"
                        else:    
                            exc_type, exc_value, exc_traceback = sys.exc_info()              
                            error_message = f"\"{folder}\";\"{exc_type}\";\"{exc_value}\";\"{exc_traceback.tb_frame.f_code.co_name}\";\"{exc_traceback.tb_lineno}\"\n"
                        error_queue.put(error_message)

            io_queue.task_done()