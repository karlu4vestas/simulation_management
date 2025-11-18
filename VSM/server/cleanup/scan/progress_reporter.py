from abc import ABC, abstractmethod
class ProgressReporter(ABC):
    seconds_between_update:int = 60
    
    @abstractmethod
    def update (self, nb_processed_folders:int, io_queue_qsize:int, active_threads:int):
        #this function is to called by the scanning process to report progress
        pass
