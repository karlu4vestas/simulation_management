# SimulationFileRegistry - A class for scanning and caching file/folder entries in a simulation directory tree.
#
# This class provides efficient lookup of files and folders within a simulation by:
# - Scanning the entire directory tree on initialization
# - Caching all entries in memory for fast lookups
# - Supporting both threaded and single-threaded scanning
# - Using local paths (relative to simulation root) as keys

import os
import sys
import time
from queue import Queue
from typing import Optional, Callable

from cleanup_cycle.clean_agent.file_utilities import FileStatistics, get_size_stat_from_file_entries


class SimulationFileRegistry:
    # Registry for efficiently accessing files and folders within a simulation directory tree.
    # 
    # The registry scans the entire simulation folder structure on initialization and caches
    # all directory and file entries, allowing fast lookups by local path (relative to the
    # simulation root).
    # 
    # Attributes:
    #     base_path (str): The absolute path to the simulation root directory
    #     all_dir_entries (dict): Dictionary mapping local paths to lists of directory DirEntry objects
    #     all_file_entries (dict): Dictionary mapping local paths to lists of file DirEntry objects
    
    def __init__(self, base_path: str, error_queue: Queue[str], 
                 threadpool: Optional[object] = None):
        # Initialize the registry and scan the simulation directory tree.
        # 
        # Args:
        #     base_path: Absolute path to the simulation root directory
        #     error_queue: Queue for error reporting. Errors are written as CSV lines
        #                 with format: "path";"error_type";"error_value";"function";"line_number"
        #     threadpool: Optional threadpool with a map() method for parallel scanning.
        #                If None, scanning runs in the current thread sequentially.
        self.base_path = base_path
        self._error_queue = error_queue
        self._threadpool = threadpool
        self.all_dir_entries: dict[str, list[os.DirEntry]] = {}
        self.all_file_entries: dict[str, list[os.DirEntry]] = {}
        self.direct_child_folders_in_base_path: dict[str, list[os.DirEntry]] = None
        
        # Scan on initialization
        self._scan_all()
    
    def _scan_all(self) -> None:
        # Scan the entire simulation directory tree and cache all entries.
        # 
        # This method performs a breadth-first traversal of the directory tree,
        # processing directories level by level. It uses the threadpool if available
        # for parallel scanning, otherwise scans sequentially.
        set_base_dirs = set()
        dir_queue = []
        
        def scan(path: str) -> tuple[Optional[list[os.DirEntry]], str]:
            # Scan a single directory and cache its entries.
            # Args:
            #     path: Absolute path to the directory to scan
            # Returns:
            #     Tuple of (entries_list, path) for compatibility with threadpool.map()
            entries_list = None
            max_failure, failures, success = 2, 0, False
            
            while failures < max_failure and not success:
                try:
                    entries_list: list[os.DirEntry] = []
                    with os.scandir(path) as entries:
                        entries_list = [e for e in entries]
                    
                    # Calculate local path relative to simulation root
                    # Replace backslashes with forward slashes for consistency
                    local_path = path[len(self.base_path):].strip("\\/").replace("\\", "/").lower()
                    
                    # Separate and cache files and directories
                    self.all_file_entries[local_path] = [
                        e for e in entries_list if e.is_file(follow_symlinks=False)
                    ]
                    self.all_dir_entries[local_path] = [
                        e for e in entries_list if e.is_dir(follow_symlinks=False)
                    ]
                    
                    # Add subdirectories to queue (avoid duplicates)
                    new_subdirs = [
                        e.path for e in self.all_dir_entries[local_path] 
                        if e.path.lower() not in set_base_dirs
                    ]
                    set_base_dirs.update([d.lower() for d in new_subdirs])
                    dir_queue.extend(new_subdirs)
                    
                    success = True
                    
                except Exception as e:
                    failures += 1
                    if failures < max_failure:
                        time.sleep(0.5)
                    else:
                        # Log error if error_queue is available
                        if self._error_queue is not None:
                            if sys.exc_info()[0] is None:
                                error_message = f'"{path}";"{str(e)}";"";"";""\\n'
                            else:
                                exc_type, exc_value, exc_traceback = sys.exc_info()
                                error_message = (
                                    f'"{path}";"{exc_type}";"{exc_value}";'
                                    f'"{exc_traceback.tb_frame.f_code.co_name}";'
                                    f'"{exc_traceback.tb_lineno}"\\n'
                                )
                            self._error_queue.put(error_message)
            
            return entries_list, path
        
        # Initialize with base directory and its immediate children
        base_dirs = []
        try:
            # First, scan the base directory itself
            with os.scandir(self.base_path) as entries:
                base_entries = [e for e in entries]
                
                # Store the base directory entries
                self.all_file_entries[""] = [e for e in base_entries if e.is_file(follow_symlinks=False)]
                self.all_dir_entries[""] = [e for e in base_entries if e.is_dir(follow_symlinks=False)]
                
                # Add subdirectories to scan queue (use actual path, not lowercased)
                base_dirs.extend([e.path for e in base_entries if e.is_dir(follow_symlinks=False)])
        except Exception:
            # If we can't scan the base directory, we're done
            return
        
        set_base_dirs.update([self.base_path.lower()])
        set_base_dirs.update([d.lower() for d in base_dirs])  # Store lowercased for comparison
        dir_queue.extend(base_dirs)  # But queue the actual paths
        
        # Process directories level by level
        while len(dir_queue):
            paths = dir_queue.copy()
            dir_queue.clear()
            
            if self._threadpool is not None:
                # Use threadpool for parallel scanning
                for _, _ in self._threadpool.map(scan, paths):
                    continue
            else:
                # Sequential scanning in current thread
                for path in paths:
                    scan(path)
    
    def get_entries(self, local_path: str = "") -> tuple[list[os.DirEntry], list[os.DirEntry]]:
        # Get directory and file entries for a specific local path.
        
        # Args:
        #     local_path: Path relative to simulation root (e.g., "INPUTS", "INT", "PROG").
        #                Use empty string "" for the simulation root itself.
        #                Path separators can be "/" or "\\".
        #                Case-insensitive.
        
        # Returns:
        #     Tuple of (dir_entries, file_entries) where each is a list of os.DirEntry objects.
        #     Returns empty lists if the path doesn't exist in the registry.
        
        # Examples:
        #     dirs, files = registry.get_entries("INPUTS")
        #     dirs, files = registry.get_entries("INT")
        #     dirs, files = registry.get_entries("")  # Root directory
        #     dirs, files = registry.get_entries("EXTFND/dat")
        
        # Normalize path: replace backslashes with forward slashes, convert to lowercase
        local_path = local_path.replace("\\", "/").lower()
        return (
            self.all_dir_entries.get(local_path, []),
            self.all_file_entries.get(local_path, [])
        )
    
    def get_all_entries(self) -> tuple[dict[str, list[os.DirEntry]], dict[str, list[os.DirEntry]]]:
        # Get all cached directory and file entries.
        
        # Returns:
        #     Tuple of (all_dir_entries, all_file_entries) where each is a dictionary
        #     mapping local paths (str) to lists of os.DirEntry objects.
        
        # Examples:
        #     all_dirs, all_files = registry.get_all_entries()
            
        #     # Get all paths that were scanned
        #     all_paths = all_files.keys()
            
        #     # Iterate through all files in the simulation
        #     for local_path, files in all_files.items():
        #         for file_entry in files:
        #             print(file_entry.path, file_entry.stat().st_size)
        return self.all_dir_entries, self.all_file_entries

    def get_immediate_folders_in_root(self) -> dict[str, os.DirEntry]:
        # Returns: the immediate child folders in the simulation root where the 
        # dict keys are the lowercase name of the subfolder and the value is the DirEntry object.
        # Example:
        #   {
        #       "inputs": <DirEntry 'INPUTS'>,
        #       "int": <DirEntry 'INT'>,
        #       "prog": <DirEntry 'PROG'>,
        #       "outputs": <DirEntry 'OUTPUTS'>
        #   }
        if self.direct_child_folders_in_base_path is None:
            self.direct_child_folders_in_base_path: dict[str, os.DirEntry] = {}
            # Get the direct child directories from the root path ("")
            root_dirs = self.all_dir_entries.get("", [])
            for dir_entry in root_dirs:
                # Map lowercase folder name to the DirEntry object
                self.direct_child_folders_in_base_path[dir_entry.name.lower()] = dir_entry

        return self.direct_child_folders_in_base_path
    
    def get_simulation_statistics(self) -> FileStatistics:
        # Calculate total size and modification dates of all files in simulation.
        
        # Returns:
        #     SizeStats: Named tuple containing:
        #     - bytes: Total size in bytes across all files
        #     - count_files: Total number of files
        #     - max_date: Most recent modification timestamp (or None)
        #     - min_date: Oldest modification timestamp (or None)
        #     - file_entries: The input dictionary (passed through)
        _, all_files = self.get_all_entries()
        return get_size_stat_from_file_entries(all_files)
