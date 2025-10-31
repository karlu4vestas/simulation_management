import time
import sys
import time
from threading import Thread
from clean_parameters import clean_parameters
from clean_writer_tasks import file_error_writer_task, folder_error_writer_task, scan_log_task, FileDeletionMethod, delete_files, deletion_log_task
from clean_folder_scanner import folder_scanner
from cleaner import clean_parameters_start_stop
"""
Start and stop of the thread that control the scan of folders and clean of simulaitons
"""

#format number with thousand separator and alignment
def ft_al_6(number):  return '{:>6,}'.format(number).replace(",",".")
def ft_al_9(number):  return '{:>9,}'.format(number).replace(",",".")
def ft_al_12(number): return '{:>12,}'.format(number).replace(",",".")
def ft_al_15(number): return '{:>15,}'.format(number).replace(",",".")


def monitor(params):
    while not params.stop_event.is_set():
        sys.stdout.write(f"\r{params.logStatus()}")
        sys.stdout.flush()    
        time.sleep(5)


def clean_simulations(simulation_folders:list[str], params:clean_parameters_start_stop):
    nb_simulations = len(simulation_folders)
    params.folder_count_total.change_value(nb_simulations)

    #concerning the total number of thread notice that beside the below no_folder_scanner_threads and no_deletions_threads
    # we will pr no_folder_scanner_threads also create:  simulation_threadpool = ThreadPoolExecutor( max_workers = 10  )
    #The total number of threads is then: no_folder_scanner_threads + no_deletions_threads + simulation_threadpool_max_workers*no_folder_scanner_threads
    no_folder_scanner_threads, no_deletions_threads = 0, 1
    if params.file_deletion_method == FileDeletionMethod.Delete:
        no_folder_scanner_threads=32
        no_deletions_threads=1024
    else:     
        no_folder_scanner_threads=128-32  
        no_deletions_threads=2
        #no_folder_scanner_threads=0
        #no_deletions_threads=1

    #initialize the folder queue with the known simulations
    for p in simulation_folders:
        params.folder_scan_queue.put(p) 

    #initialize all multithreading to analyze or delete simulations according to params.file_deletion_method
    start = time.time()
    print(f"\nNumber of simulations to be processed: {len(simulation_folders)}" )
    print(f"no_folder_scanner_threads, no_deletions_threads: {no_folder_scanner_threads}, {no_deletions_threads}")
    try:
        monitor_thread = Thread( target=monitor, args=(params, ), daemon=True )
        monitor_thread.start()
        job = Thread(target=params.start_workers, args=(no_folder_scanner_threads, no_deletions_threads,), daemon=True )
        job.start()

        #wait for all simulations to have been taken off the queue
        while params.folder_scan_queue.unfinished_tasks > 0 and not params.stop_event.is_set() :
            time.sleep(2)

        if params.folder_scan_queue.unfinished_tasks == 0:
            print(f"\nAll simulations have been taken off the queue: {params.folder_scan_queue.unfinished_tasks}")
            params.stop_event.set()

    except KeyboardInterrupt:
        print(" interupt scanning")
        params.stop_event.set()

    #start = time.time()
    params.stop_workers()    
    monitor_thread.join()
    #print(f"\ntime to stop: {time.time() - start}")

    return params.sim_processed.value(), params.sim_cleaned.value(), params.sim_ignored.value(), params.sim_already_cleaned.value(), params.sim_irreproducible.value()


class clean_parameters_start_stop(clean_parameters):
    def __init__(self, base_folder:str, file_deletion_method:FileDeletionMethod = FileDeletionMethod.Analyse, cleaner_threads = 6, deletion_threads:int=2048):
        super().__init__( base_folder, file_deletion_method, cleaner_threads, deletion_threads)
        self.progress_file_handle = open(self.progress_path,"a")
          
    def close_logStatus(self):
        self.progress_file_handle.flush()
        self.progress_file_handle.close()

    def logStatus(self):
        files_deleted, bytes_deleted = self.deletionMeasures.values()
        status = f"Duration: {ft_al_9(int(time.time() - self.start_time))} Detected folders :{ft_al_12(self.folder_count_total.value())} Processed folders:{ft_al_12(self.folders_processed.value())} "\
                +f"Simulations:{ft_al_12(self.sim_processed.value())} cleaned:{ft_al_6(self.sim_cleaned.value())} nothing to delete:{ft_al_6(self.sim_already_cleaned.value())} ignored:{ft_al_6(self.sim_ignored.value())} "
        if self.file_deletion_method == FileDeletionMethod.Delete:                        
                status = status+f"Files, bytes, file errors: {ft_al_9(files_deleted)},  {ft_al_15(bytes_deleted)},  {self.file_errors.value()}"
        self.progress_file_handle.write(status+"\n")
        return status

    def start_workers(self, no_folder_scanner_threads, no_deletions_threads):
        
        self.file_error_thread = Thread(target=file_error_writer_task, 
                                          args=(self.file_error_path, 
                                                self.file_error_queue, 
                                                self.file_errors,), 
                                                daemon=True) 
        self.file_error_thread.start()

        self.folder_error_thread = Thread(target=folder_error_writer_task, 
                                            args=(self.folder_error_path, 
                                                  self.folder_error_queue,
                                                  self.folder_errors ), 
                                                  daemon=True) 
        self.folder_error_thread.start()

        self.scan_log_thread = Thread(target=scan_log_task, 
                                        args=(self.scan_log_path, 
                                              self.scan_log_queue,
                                              self.folder_count ), 
                                              daemon=True) 
        self.scan_log_thread.start()


        self.deletion_log_thread = Thread(target=deletion_log_task, 
                                            args=(  self.deletion_log_path, 
                                                    self.deletion_log_queue ), 
                                                    daemon=True) 
        self.deletion_log_thread.start()


        def create_deletion_thread():
            return Thread(target=delete_files, 
                          args=(self.file_deletion_queue, self.file_error_queue, self.deletion_log_queue, self.deletionMeasures, self.file_deletion_method ), daemon=True) 

        self.file_deletion_threads = [create_deletion_thread() for i in range(no_deletions_threads)] 
        for t in self.file_deletion_threads:
            t.start()


        if no_folder_scanner_threads > 0:
            self.folder_scanner_threads = [Thread(target=folder_scanner, args=(self.folder_scan_queue, self.folder_error_queue, self, 1, ), daemon=True)  
                                           for i in range(no_folder_scanner_threads)] 
            for t in self.folder_scanner_threads:
                t.start()
        else:
            #insert stop signal here because without threading the folder_scanner does not return before its done    
            folder_scanner( self.folder_scan_queue, self.folder_error_queue, self)
        


    #stop logger Queue and threads
    def stop_workers(self):
        
        #insert the terminating signal "None" for the folder_scanner_threads now that all simulations has been taken
        if not self.folder_scanner_threads is None:  
            #insert termination signals and wait for the scanner threads to consume them
            for t in self.folder_scanner_threads:
                self.folder_scan_queue.put(None)
            for t in self.folder_scanner_threads:
                t.join()
 

        for t in self.file_deletion_threads:
            self.file_deletion_queue.put(None)
        for t in self.file_deletion_threads:
            t.join()

        #at this point the folder_scanner_threads will no longer produce messags so signal that all loggers must stop
        self.scan_log_queue.put(None)
        self.file_error_queue.put(None)
        self.folder_error_queue.put(None)
        self.deletion_log_queue.put(None)

        self.scan_log_thread.join()
        self.file_error_thread.join()
        self.folder_error_thread.join()
        self.deletion_log_thread.join()