import os
import io
import re
import sys
import time
from datetime import datetime
from typing import Optional, NamedTuple
from queue import Queue

# Import from clean_agent folder
from .simulation_file_registry import SimulationFileRegistry
from .file_utilities import get_size_stat_from_file_entries, FileStatistics
from .clean_folder_type import clean_folder_type, BaseSimulation
from .clean_all_pr_ext import clean_all_pr_ext
from .clean_all_but_one_pr_ext import clean_all_but_one_pr_ext

# Import DTOs
from datamodel.dtos import FileInfo, FolderTypeEnum
from datamodel.retentions import ExternalRetentionTypes


class SimulationEvalResult(NamedTuple):
    """Result from evaluating a simulation for cleanup."""
    file_info: FileInfo
    all_cleaners_files: list
    cleaner_file_count: int
    cleaner_bytes: int


class Simulation(BaseSimulation):
    # Simplified simulation class for identifying cleanable files in VTS simulations.
    
    # This class:
    # - Uses SimulationFileRegistry for efficient file/folder scanning
    # - Identifies cleanable files based on .set file load cases
    # - Detects simulation modifications via timestamp comparison
    # - Returns FileInfo with external_retention status
    
    cleaners: list[clean_folder_type] = [
        clean_all_pr_ext(["INT"], [".int", ".tff"]),
        clean_all_pr_ext(["EXTFND/dat"], [".sim"]), #@TODO the line should probably be clean_all_pr_ext(["EXTFND/dat"], [".sim"])?
        clean_all_pr_ext(["EXTFND"], [".sim"]),   
        clean_all_but_one_pr_ext(["EIG"], [".eig", ".mtx"]),
        clean_all_but_one_pr_ext(["OUT"], [".out"]),
        clean_all_but_one_pr_ext(["STA"], [".sta"]),
        clean_all_but_one_pr_ext(["LOG"], [".log"]),
    ]
    vts_standard_folders = set([name.casefold() for name in ["INPUTS", "DETWIND", "EIG", "INT", "LOG", "OUT", "PARTS", "PROG", "STA"]])

    def __init__(self, root_path: str, old_modified_date: datetime, error_queue: Queue[str], file_registry: SimulationFileRegistry):
        # Initialize Simulation instance.
        
        # Args:
        #     root_path: Base path of the simulation directory
        #     old_modified_date: Previous maximum modification timestamp
        #     error_queue: Queue for CSV-formatted error messages
        #     threadpool: Optional threadpool for parallel scanning

        super().__init__(root_path)
        self.root_path = root_path
        self.old_modified_date:datetime = old_modified_date
        self.error_queue = error_queue
        
        # Initialize file registry for scanning
        self.registry = file_registry
        
        # Cached data
        self.set_names: list[str] = None
        self.set_extension_files: list[os.DirEntry] = None
        self.set_output_files = None
        self.standard_dirs: dict[str, os.DirEntry] = None
        self.has_htc_folder_: bool = None

    def getSetFiles(self) -> list[os.DirEntry]:
        # Get .set files from INPUTS folder.        
        # Returns:
        #     List of DirEntry objects for .set files
        if self.set_extension_files is None:
            _, file_entries = self.registry.get_entries("inputs")
            if file_entries is not None:
                self.set_extension_files = [e for e in file_entries if ".set" == e.name[-4:].lower()]
            else:
                self.set_extension_files = []
        
        return self.set_extension_files

    def getSetNames(self) -> list[str]:
        # Extract setnames from .set file.        
        # Handles line continuation (>> followed by newline) and skips first 6 header lines.
        # Returns first token from each data line as setname in lowercase.
        
        max_failures, failures = 2, 0
        success = False
        
        while failures < max_failures and success is False:
            try:
                if self.set_names is None:
                    set_files = self.getSetFiles()
                    self.set_names: list[str] = []

                    if len(set_files) == 1:
                        with io.open(set_files[0].path, "r", encoding="utf-8", errors='ignore', buffering=2**19) as file:
                            # Handle line continuation: >> followed by newline
                            set_names_splitted_lines = re.sub("(>>\\s*\\n)", " ", file.read()).lower().splitlines()

                        # Skip first 6 header lines, then extract first token from each line
                        # Filter to only include loadcase names longer than 0 and that are alphanumeric
                        if len(set_names_splitted_lines) > 6:
                            set_names_splitted_lines = [s.split(None, 1) for s in set_names_splitted_lines[6:]]
                            self.set_names = [s[0] for s in set_names_splitted_lines if len(s) > 0 and s[0].isalnum()]

                success = True
            except Exception as e:
                failures = failures + 1
                if failures < max_failures:
                    time.sleep(1)
                else:
                    error_message = f'"getSetNames:{str(e)}";"{self.root_path}"\n'
                    self.error_queue.put(error_message)

        return self.set_names if self.set_names is not None else []

    def hasValidSetNames(self) -> bool:
        # Check if simulation has valid setnames.        
        # Returns:
        #     True if at least one setname exists
        setnames = self.getSetNames()
        return len(setnames) > 0

    def get_set_output_files_for_cleaners(self) -> dict[str, list[os.DirEntry]]:
        # Get all files matching cleaner extensions that also match setnames.
        # Returns:
        #     Dictionary mapping cleaner key to list of matching DirEntry objects
        if self.set_output_files is None:
            self.set_output_files: dict[str, list[os.DirEntry]] = {}
            set_names: set[str] = set(self.getSetNames())
            
            for cleaner in Simulation.cleaners:
                _, file_entries = self.registry.get_entries(os.path.join(*cleaner.local_folder_names))
                if file_entries is not None:
                    # Match files by extension and setname (filename without extension)
                    self.set_output_files[cleaner.key] = [
                        f for f in file_entries 
                        if f.name.lower().endswith(cleaner.extensions) 
                        and (f.name.lower().rsplit(".", 1)[0] in set_names)
                    ]
                else:
                    self.set_output_files[cleaner.key] = []

        return self.set_output_files

    def get_cleaner_files(self) -> dict[str, list[os.DirEntry]]:
        # Get files to be cleaned for each cleaner.        
        # Returns:
        #     Dictionary mapping cleaner key to list of cleanable files
        cleaner_files_from_setnames: dict[str, list[os.DirEntry]] = {}

        for cleaner in Simulation.cleaners:
            cleaner_files_from_setnames[cleaner.key] = cleaner.retrieve_file_list(self, self.root_path)

        return cleaner_files_from_setnames

    def get_standard_folders(self) -> dict[str, os.DirEntry]:
        if self.standard_dirs is None:
            try:
                immediate_folders: dict[str, os.DirEntry] = self.registry.get_immediate_folders_in_root()
                self.standard_dirs = {name.lower(): direntry for name, direntry in immediate_folders.items() if name in self.vts_standard_folders}
            except:
                self.standard_dirs = None
           
        return self.standard_dirs

    def has_htc_folder(self) -> bool:
        if self.has_htc_folder_ is None:
            matches:list[bool] = [True for name, direntry in self.registry.get_immediate_folders_in_root().items() if "htc" in name.lower()]
            self.has_htc_folder_ = any(matches)
        return self.has_htc_folder_

    def eval(self) -> SimulationEvalResult:
        # Evaluate simulation for cleanup.
        #  - If modification date FROM the scan is missing then : Returns MISSING retention with empty file list. Should not happen or only very rare.
        #  - If modification date changed: Returns UNDEFINED retention with empty file list.
        #  - If modification date unchanged: Identifies all cleanable files.
        # Returns:
        #     SimulationEvalResult with:
        #     - file_info: FileInfo object with retention status
        #     - all_cleaners_files: List of DirEntry objects to clean (empty if modified)
        #     - cleaner_file_count: Number of cleanable files (0 if modified)
        #     - cleaner_bytes: Total bytes of cleanable files (0 if modified)

        all_cleaners_files = []
        cleaner_file_count = 0
        cleaner_bytes = 0
        
        try:
            # Get current maximum modification date
            filestats:FileStatistics = self.registry.get_simulation_statistics()

            # if the modification date is missing or this is not a VTS folder or is the root has a htcfolder then
            # return immediate with a FileInfo where all but the filepath is set to None
            if filestats.max_date is None or len(self.get_standard_folders()) != len(self.vts_standard_folders) or self.has_htc_folder():
                file_info = FileInfo(
                    filepath=self.root_path,
                    modified_date=None,
                    nodetype=None,
                    external_retention=None
                )
                return SimulationEvalResult(file_info, [], 0, 0)
            elif filestats.max_date != self.old_modified_date:
                # the simulation was modified since last scan so skip cleanup, set retention to UNDEFINED
                file_info = FileInfo(
                    filepath=self.root_path,
                    modified_date=filestats.max_date,
                    nodetype=FolderTypeEnum.SIMULATION,
                    external_retention=ExternalRetentionTypes.NUMERIC
                )
                return SimulationEvalResult(file_info, [], 0, 0)

            #at this point filestats.max_date == self.old_modified_date so we can proceed with the cleanup

            # No modification detected - proceed with cleanup identification
            all_cleaners_files_dict: dict[str, list[os.DirEntry]] = self.get_cleaner_files()
            all_cleaners_files = [f for files in all_cleaners_files_dict.values() for f in files]
            
            # Calculate size of cleanable files
            #cleaner_bytes, cleaner_file_count, _, _, _ = get_size_stat_from_file_entries(all_cleaners_files_dict)

            # Determine external retention status
            if not self.hasValidSetNames():
                external_retention = ExternalRetentionTypes.ISSUE
            elif len(all_cleaners_files) == 0:
                external_retention = ExternalRetentionTypes.CLEAN
            else:
                external_retention = ExternalRetentionTypes.CLEAN

            file_info = FileInfo(
                filepath=self.root_path,
                modified_date=filestats.max_date,
                nodetype=FolderTypeEnum.SIMULATION,
                external_retention=external_retention
            )

        except Exception as e:
            # Log error to queue
            if sys.exc_info() is None:
                error_message = f'"{self.root_path}";"{str(e)}";"";"";""\n'
            else:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                error_message = f'"{self.root_path}";"{str(exc_type)}";"{str(exc_value)}";"{str(exc_traceback.tb_frame.f_code.co_name)}";"{str(exc_traceback.tb_lineno)}"\n'
            self.error_queue.put(error_message)
            
            # Return safe defaults on error
            file_info = FileInfo(
                filepath=self.root_path,
                modified_date=self.old_modified_date,
                nodetype=FolderTypeEnum.SIMULATION,
                external_retention=ExternalRetentionTypes.ISSUE
            )
            all_cleaners_files = []
            cleaner_file_count = 0
            cleaner_bytes = 0

        return SimulationEvalResult(file_info, all_cleaners_files, cleaner_file_count, cleaner_bytes)
    
