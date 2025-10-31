import os
import sys
import time
from enum import Enum
from threading import Lock


# thread safe counter class
class ThreadSafeCounter():
    # constructor
    def __init__(self):
        # initialize counter
        self._counter = 0
        # initialize lock
        self._lock = Lock()
 
    # increment the counter
    def increment(self, increment=1):
        with self._lock:
            self._counter += increment
 
    # get the counter value
    def value(self):
        with self._lock:
            return self._counter

    # get the counter value
    def change_value(self, new_value):
        with self._lock:
            self._counter = new_value

# thread safe counter class
class ThreadSafeDeletionCounter():
    # constructor
    def __init__(self):
        # initialize counter
        self._files_deleted = 0
        self._bytes_deleted = 0
        # initialize lock
        self._lock = Lock()
 
    # increment the values
    def add(self, files_deleted, bytes_deleted):
        with self._lock:
            self._files_deleted += files_deleted
            self._bytes_deleted += bytes_deleted
 
    # get the values
    def values(self):
        with self._lock:
            return self._files_deleted, self._bytes_deleted



# class syntax
class FileDeletionMethod(Enum):
    Analyse = "analyse"
    Delete  = "delete"
    def __str__(self):
        return self.name
    



#from enum import StrEnum
class FolderLogInfo(Enum):
    p0                          = "p0_local_folder"
    p1                          = "p1_local_folder"
    p2                          = "p2_local_folder"
    p3                          = "p3_local_folder"
    p4                          = "p4_local_folder"
    p5                          = "p5_local_folder"
    p6                          = "p6_local_folder"
    folder                      = "folder"
    root_folder                 = "root_folder"

    folder_files                = "folder_files"
    folder_bytes                = "folder_bytes"
    folder_max_date             = "folder_max_date"
    folder_type                 = "folder_type"
    standard_folders            = "standard_folders"
    direct_child_folders        = "direct_child_folders"
    #vts_output_files            = "vts_output_file_count"
    #vts_output_bytes            = "vts_output_bytes"
    prepper                     = "prepper"
    min_set_output_time         = "vts_output_oldest"
    max_INPUTS_time             = "vts_inputs_most_recent"
    setname_count               = "setname_count"
    int_file_count              = "int_file_count"
    extfnd_file_count           = "extfnd_file_count"
    eig_file_count              = "eig_file_count"
    out_file_count              = "out_file_count"
    log_file_count              = "log_file_count"
    sta_file_count              = "sta_file_count"
    
    htc_file_count              = "htc_count"
    htcfiles_file_count         = "htcfiles_count"
    wind_file_count             = "wind_file_count"
    res_file_count              = "res_file_count"
    logfiles_file_count         = "logfiles_file_count"   
    
    #cleanup2backup_count        = "str_cleanup2backup_count"
    exclusion_status            = "exclusion_status"  #is the simulation not excluded
    exclusion_info              = "exclusion"  #is the simulation not excluded
    reproducibility             = "reproducibility"
    setnames_validity           = "setnames"
    cleaning_status             = "cleanable_simulation"
    cleanable_files             = "cleanable_files"
    cleanable_bytes             = "cleanable_bytes"
    #uncleanable_details         = "uncleanable_details (folder:file-count:bytes)"
    reproducibility_violations  = "reproducibility_violations"

    def __str__(self):
        return self.name

class LogInfoElement:

    def __init__(self, pass_criteria): self.can_clean = pass_criteria
    #def can_clean(self): return self.can_clean
    #def msg(self):       return self.element[1]
    #def __str__(self):   return f"{self.element[0]};{self.element[1]}"


class SimulationStatus:
    #intiate the columns dict so all entries has a value. 
    # #In this way we do not need to check for the presence of value when making the output
    default_columns_dict=None
    def __init__(self, folder, ignore_splits=3):
        if SimulationStatus.default_columns_dict is None : 
            SimulationStatus.default_columns_dict = { str(m):"" for m in FolderLogInfo.__members__}

        self.columns = dict(SimulationStatus.default_columns_dict)
        self.can_clean_criteria = {}

        self.add(FolderLogInfo.folder, None, folder)
        folder_segments = folder.split("\\")[ignore_splits:]
        if len(folder_segments) > 0: self.add(FolderLogInfo.p0, None, folder_segments[0])
        if len(folder_segments) > 1: self.add(FolderLogInfo.p1, None, folder_segments[1])
        if len(folder_segments) > 2: self.add(FolderLogInfo.p2, None, folder_segments[2])
        if len(folder_segments) > 3: self.add(FolderLogInfo.p3, None, folder_segments[3])
        if len(folder_segments) > 4: self.add(FolderLogInfo.p4, None, folder_segments[4])
        if len(folder_segments) > 5: self.add(FolderLogInfo.p5, None, folder_segments[5])
        if len(folder_segments) > 6: self.add(FolderLogInfo.p6, None, folder_segments[6])


    def add(self, info_type:FolderLogInfo, can_clean:bool, log_msg):
         if can_clean is None:             
            self.columns[str(info_type)] = log_msg 
         else:
            self.columns[str(info_type)] = log_msg 
            self.can_clean_criteria[str(info_type)] = LogInfoElement(can_clean) 

    def can_clean(self):
        can_clean_ = [ s.can_clean for s in self.can_clean_criteria.values() ] if len(self.can_clean_criteria)>0 else [False]
        return all(can_clean_) 
    #def __str__(self):
    #    return ";".join( [ "\""+m+"\"" for m in columns] )+"\n"
    

@staticmethod
def scan_log_task(filepath, queue, folder_count):
        
    with open(filepath, 'a', encoding="utf-8", buffering= 2**19 ) as file:
        #columns = FolderLogInfo.__members__
        columns = [ str(m) for m in FolderLogInfo.__members__]
        header = ";".join( [ "\""+m+"\"" for m in columns] )+"\n"
        #print(f"header:{header}")
        file.writelines( header )

        while True:
            sim_status = queue.get()
            if sim_status is None:
                file.flush()
                queue.task_done()
                break
            else:

                #for c in columns:
                #    print(c)
                #    print(sim_status.columns[c] ) 
                                      
                line = ";".join( [ f"\""+(sim_status.columns[c] ) +"\"" for c in columns] )+"\n"

                file.write(line)
                folder_count.increment()
                queue.task_done()

def deletion_log_task(filepath, queue):
        
    with open(filepath, 'a', encoding="utf-8", buffering= 2**19 ) as file:
        file.writelines( "deleted_files\n" ) #header

        while True:
            file_path = queue.get()
            if file_path is None:
                file.flush()
                queue.task_done()
                break
            else:          
                log = "\""+file_path+"\"\n"    
                file.write(log)
                queue.task_done()


def delete_files( file_queue, error_queue, log_queue, deletionMeasures, file_deletion_method, max_failure=1):
    while True :                
        file_entry = file_queue.get()
        failures, succes = 0, False
            
        if file_entry is None:
            file_queue.task_done()
            break

        while failures < max_failure and not succes:
            try:
                files_deleted, bytes_deleted = 1, file_entry.stat().st_size
                if file_deletion_method == FileDeletionMethod.Delete:
                    os.remove(file_entry.path)
                log_queue.put(file_entry.path)

                deletionMeasures.add( files_deleted, bytes_deleted )
                succes = True
            except Exception as e: 
                failures = failures + 1
                if failures < max_failure:
                    time.sleep(0.5)
                else:
                    if sys.exc_info() is None:
                        error_message = f"\"{file_entry.path}\";\"{str(e)}\";\"\";\"\";\"\"\n"
                    else:    
                        exc_type, exc_value, exc_traceback = sys.exc_info()              
                        error_message = f"\"{file_entry.path}\";\"{exc_type}\";\"{exc_value}\";\"{exc_traceback.tb_frame.f_code.co_name}\";\"{exc_traceback.tb_lineno}\"\n"
                    error_queue.put(error_message)

        file_queue.task_done()


@staticmethod
def file_error_writer_task(filepath, queue, file_errors):
        
    with open(filepath, 'a', encoding="utf-8", buffering= 2**18 ) as file:
        file.writelines( f"\"error\";\"file\"\n" )                
        while True:
            line = queue.get()
            if line is None:
                file.flush()
                queue.task_done()
                break
            else:
                file.write(line)
                file_errors.increment()
                queue.task_done()

@staticmethod
def folder_error_writer_task(filepath, queue, folder_errors):
        
    with open(filepath, 'a', encoding="utf-8", buffering= 2**18 ) as file:
        file.writelines( f"\"error\";\"path\"\n" )                
        while True:
            line = queue.get()
            if line is None:
                file.flush()
                queue.task_done()
                break
            else:
                file.write(line)
                folder_errors.increment()
                queue.task_done()

