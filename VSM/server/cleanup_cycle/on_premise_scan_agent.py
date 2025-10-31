import os
import tempfile
import csv
from datetime import date, datetime
from cleanup_cycle.cleanup_dtos import ActionType 
from cleanup_cycle.cleanup_scheduler import AgentInterfaceMethods
from cleanup_cycle.internal_agents import AgentTemplate
from cleanup_cycle.scan.scan import do_scan, ScanResult 
from datamodel.dtos import ExternalRetentionTypes, FileInfo, FolderTypeEnum
from cleanup_cycle.scan.ProgressWriter import ProgressWriter, ProgressReporter
from cleanup_cycle.scan.folder_tree import FolderTree, FolderTreeNode
#from server.cleanup_cycle.scan.progress_reporter import ProgressReporter

# The purpose of this class is to resuse the ProgressWriter to report progress to the task
class AgentScanProgressWriter(ProgressWriter):
    def __init__(self, agentScanRootFolder: "AgentScanVTSRootFolder", seconds_between_update:int, seconds_between_filelog:int):
        super().__init__(seconds_between_update, seconds_between_filelog)
        self.agentScanRootFolder = agentScanRootFolder

    def write_realtime_progress(self, nb_processed_folders:int, mean_dirs_second:int, io_queue_qsize:int, active_threads:int):    
        #report real-time progress to the task.
        msg: str = f"\rFolders processed; pr second, queue_size, threads: {nb_processed_folders}; {mean_dirs_second}; {io_queue_qsize}; {active_threads}"
        self.task = AgentInterfaceMethods.task_progress(self.agentScanRootFolder.task.id, msg)

    def open(self, output_path: str):
        super().open(output_path)
        
    def close(self):
        super().close()

class AgentScanVTSRootFolder(AgentTemplate):
    temporary_result_folder: str | None
    vts_name_set:set[str]  = set( [name.casefold() for name in ["INPUTS","DETWIND","EIG","INT","LOG", "OUT","PARTS","PROG","STA"] ] )    

    def __init__(self):
        super().__init__("AgentScanRootfolder", [ActionType.SCAN_ROOTFOLDER.value])

        #get a temporary result folder as default location for scan results
        self.temporary_result_folder: str = os.getenv('TEMPORARY_SCAN_RESULTS', tempfile.gettempdir())  # where should the file and folder meta data be placed
        if len(self.temporary_result_folder) == 0 or not os.path.exists(self.temporary_result_folder):
            self.error_message = f"TEMPORARY_SCAN_RESULTS environment variable is not set or the path does not exist: {self.temporary_result_folder}"
            self.temporary_result_folder = None
        
        self.nb_scan_thread: int = int(os.getenv('SCAN_THREADS', 256))  # number of scanning threads

    def execute_task(self):
        if self.temporary_result_folder is None:
            return

        root_folder_name: str  = os.path.basename(self.task.path)
        date_time_str: str     = date.today().strftime("%Y%m%d:%H%M%S")
        metadata_file: str     = os.path.join(self.temporary_result_folder, date_time_str+"_"+root_folder_name+"_metadata.csv")

        scan_result:ScanResult = self.scan_metadata(self.task.path, metadata_file, self.nb_scan_thread)
        if scan_result.nb_scanned_folders == 0:
            self.error_message = f"Failed to scan metadata for {self.task.path}. Zero folders processed: {scan_result.message}"
            return

        extract_simulations: list[FileInfo]
        n_hierarchical_simulations: int
        extract_simulations, n_hierarchical_simulations = self.extract_simulations(scan_result.scan_output_files[0])
        
        AgentInterfaceMethods.task_progress(self.task.id, f"Identified {len(extract_simulations)} simulations and ignored {n_hierarchical_simulations} hierarchical simulations")
        if len(extract_simulations) == 0:
            return False, "No simulations were found during the scan."

        #@TODO would have been more useful to return the number of simulations that were inserted/updated. 
        # This must however come from AgentInterfaceMethods.task_insert_or_update_simulations_in_db
        result: dict[str, str] = AgentInterfaceMethods.task_insert_or_update_simulations_in_db(self.task.id, extract_simulations)

    def scan_metadata(self, path: str, meta_file_path: str, nb_scan_thread:int) -> ScanResult:
        scan_path      = os.path.normpath(path)
        output_archive = os.path.normpath(meta_file_path)

        progress_reporter:ProgressReporter = AgentScanProgressWriter( self, seconds_between_update=10, seconds_between_filelog=60)
        progress_reporter.open(output_archive)
        
        scan_io_result:ScanResult = do_scan( scan_path, output_archive, nb_scan_thread, scan_subdirs=False, progress_reporter=progress_reporter)
        
        progress_reporter.close()
        return scan_io_result

    #the current scan for simulations does not evaluate whether the simulation was cleaned or has issues. 
    def extract_simulations(self, scan_result_file: str) -> tuple[list[FileInfo], int]:

        # Notice that the modified date from the load_all_paths is the max date of alle subfolders and files.
        # The metadata scan includes the entire foldertree this is why we can convert a node' path to a modified date below.
        folder_modified_date_dict: dict[str, date] = self.load_all_paths(scan_result_file)
        if len(folder_modified_date_dict) == 0:
            return [], 0

        #Define lables for identifying the vts simulation in the tree structure and where we have issues with hierarchical vts-simulations
        vts_label:str              = "vts_simulations"
        vts_hierarchical_label:str = "vts_hierarchical"
        has_vts_children_label:str = "has_vts_children"
        #small_vts_name_set:set[str] = set( [name.casefold() for name in ["EIG","INT"] ] )
        
        prefix: str = ""
        tree:FolderTree = FolderTree(folder_modified_date_dict.keys(), prefix=prefix, path_separator="\\")
        
        tree.mark_vts_simulations(vts_label, AgentScanVTSRootFolder.vts_name_set, vts_hierarchical_label, has_vts_children_label)
        simulations_with_sub_simulations:tuple[FolderTreeNode,...]    = tree.findall(lambda node: node.get_attr(vts_hierarchical_label,False))

        simulations_without_sub_simulations:tuple[FolderTreeNode,...] = tree.findall(lambda node: len(node.get_attr(vts_label,"")) > 0 and not node.get_attr(vts_hierarchical_label,False))
        simulations_without_sub_simulations_modified_date:list[date]  = [ folder_modified_date_dict[prefix+n.get_attr(vts_label)] for n in simulations_without_sub_simulations] 
        tree = None

        #the current scan for simulations does not evaluate whether the simulation was cleaned or has issues. 
        simulations_without_sub_simulations:list[FileInfo] = [ FileInfo( filepath = prefix+n.get_attr(vts_label),
                                                                         modified_date = modified_date,
                                                                         nodetype = FolderTypeEnum.VTS_SIMULATION,
                                                                         external_retention = ExternalRetentionTypes.UNDEFINED.value
                                                                        )
                                                                for n, modified_date in zip(simulations_without_sub_simulations, simulations_without_sub_simulations_modified_date)]
        

        return simulations_without_sub_simulations, len(simulations_with_sub_simulations)    
    
    def load_all_paths(self, scan_output_file: str) -> dict[str, date]:
        # The scan output file is a csv file with a header like:"folder";"min_modified";"max_modified";"min_accessed";"max_accessed";"files"
        # Load folder and max_modified from the CSV as fast as possible
        if not os.path.exists(scan_output_file):
            self.error_message = f"Scan output file does not exist: {scan_output_file}"
            return {}

        folder_modified_data: dict[str, datetime] = {}
        try:
            # Use larger buffer for better I/O performance with large files
            with open(scan_output_file, 'r', buffering=8192*1024, newline='', encoding='utf-8') as f:
                # Use csv.reader with semicolon delimiter for efficient parsing
                reader = csv.reader(f, delimiter=';', quoting=csv.QUOTE_ALL)
                
                # Read header row and verify columns
                header = next(reader, None)
                if header is None:
                    self.error_message = f"Scan output file is empty: {scan_output_file}"
                    return {}
                
                # Find column indices
                try:
                    folder_idx = header.index("folder")
                    max_modified_idx = header.index("max_modified")
                except ValueError as e:
                    self.error_message = f"Required columns not found in {scan_output_file}. Expected 'folder' and 'max_modified'. Header: {header}"
                    return {}
                
                # Read only the folder and max_modified columns from each row
                # Parse datetime from string (adjust format as needed based on actual CSV format)
                folder_modified_data = {row[folder_idx]: date.fromisoformat(row[max_modified_idx]) 
                                        for row in reader if row and len(row) > max(folder_idx, max_modified_idx)}              
        except Exception as e:
            self.error_message = f"Failed to load folder data from {scan_output_file}: {str(e)}"
            return {}
        
        return folder_modified_data