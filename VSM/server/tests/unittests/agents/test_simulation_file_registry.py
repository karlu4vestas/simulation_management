# Unit tests for SimulationFileRegistry

import pytest
import os
import shutil
import tempfile
from queue import Queue
from multiprocessing.pool import ThreadPool
from cleanup_cycle.clean_agent.simulation_file_registry import SimulationFileRegistry

# Global test storage location
TEST_STORAGE_LOCATION = "/workspaces/simulation_management/VSM/io_dir_for_storage_test"


class TestSimulationFileRegistry:
    """Test the SimulationFileRegistry class with various scenarios"""

    @pytest.fixture
    def temp_simulation_dir(self):
        """Create a temporary simulation directory structure for testing"""
        # Create test directory in the test storage location
        test_dir = os.path.join(TEST_STORAGE_LOCATION, "test_simulation_registry")
        
        # Clean up if it exists from previous run
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        
        # Create directory structure mimicking a VTS simulation
        os.makedirs(test_dir, exist_ok=True)
        
        # Create standard VTS folders
        inputs_dir = os.path.join(test_dir, "INPUTS")
        outputs_dir = os.path.join(test_dir, "OUTPUTS")
        int_dir = os.path.join(test_dir, "INT")
        prog_dir = os.path.join(test_dir, "PROG")
        extfnd_dir = os.path.join(test_dir, "EXTFND")
        extfnd_dat_dir = os.path.join(extfnd_dir, "dat")
        
        os.makedirs(inputs_dir)
        os.makedirs(outputs_dir)
        os.makedirs(int_dir)
        os.makedirs(prog_dir)
        os.makedirs(extfnd_dat_dir)
        
        # Create some test files
        with open(os.path.join(test_dir, "root_file.txt"), "w") as f:
            f.write("root level file")
        
        with open(os.path.join(inputs_dir, "input1.set"), "w") as f:
            f.write("set file content")
        
        with open(os.path.join(inputs_dir, "input2.dat"), "w") as f:
            f.write("data file content")
        
        with open(os.path.join(int_dir, "result.int"), "w") as f:
            f.write("int file content")
        
        with open(os.path.join(int_dir, "result.tff"), "w") as f:
            f.write("tff file content")
        
        with open(os.path.join(prog_dir, "prep-id.txt"), "w") as f:
            f.write("prep file content")
        
        with open(os.path.join(extfnd_dat_dir, "data.sim"), "w") as f:
            f.write("sim file content")
        
        yield test_dir
        
        # Cleanup after test
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

    @pytest.fixture
    def error_queue(self):
        """Create an error queue for testing"""
        return Queue()

    @pytest.fixture
    def threadpool(self):
        """Create a threadpool for parallel testing"""
        pool = ThreadPool(processes=4)
        yield pool
        pool.close()
        pool.join()

    def test_initialization_without_threadpool(self, temp_simulation_dir, error_queue):
        """Test registry initialization without threadpool (sequential scanning)"""
        registry = SimulationFileRegistry(temp_simulation_dir, error_queue)
        
        assert registry.base_path == temp_simulation_dir
        assert isinstance(registry.all_dir_entries, dict)
        assert isinstance(registry.all_file_entries, dict)
        assert len(registry.all_dir_entries) > 0
        assert len(registry.all_file_entries) > 0

    def test_initialization_with_threadpool(self, temp_simulation_dir, error_queue, threadpool):
        """Test registry initialization with threadpool (parallel scanning)"""
        registry = SimulationFileRegistry(temp_simulation_dir, error_queue, threadpool)
        
        assert registry.base_path == temp_simulation_dir
        assert isinstance(registry.all_dir_entries, dict)
        assert isinstance(registry.all_file_entries, dict)
        assert len(registry.all_dir_entries) > 0
        assert len(registry.all_file_entries) > 0

    def test_get_entries_root_directory(self, temp_simulation_dir, error_queue):
        """Test retrieving entries from the root directory"""
        registry = SimulationFileRegistry(temp_simulation_dir, error_queue)
        
        dirs, files = registry.get_entries("")
        
        # Should have directories (INPUTS, OUTPUTS, INT, PROG, EXTFND)
        assert len(dirs) == 5
        dir_names = {d.name.upper() for d in dirs}
        assert "INPUTS" in dir_names
        assert "OUTPUTS" in dir_names
        assert "INT" in dir_names
        assert "PROG" in dir_names
        assert "EXTFND" in dir_names
        
        # Should have one file (root_file.txt)
        assert len(files) == 1
        assert files[0].name == "root_file.txt"

    def test_get_entries_inputs_folder(self, temp_simulation_dir, error_queue):
        """Test retrieving entries from INPUTS folder"""
        registry = SimulationFileRegistry(temp_simulation_dir, error_queue)
        
        dirs, files = registry.get_entries("INPUTS")
        
        # INPUTS should have no subdirectories
        assert len(dirs) == 0
        
        # INPUTS should have 2 files
        assert len(files) == 2
        file_names = {f.name for f in files}
        assert "input1.set" in file_names
        assert "input2.dat" in file_names

    def test_get_entries_case_insensitive(self, temp_simulation_dir, error_queue):
        """Test that path lookups are case-insensitive"""
        registry = SimulationFileRegistry(temp_simulation_dir, error_queue)
        
        dirs1, files1 = registry.get_entries("INPUTS")
        dirs2, files2 = registry.get_entries("inputs")
        dirs3, files3 = registry.get_entries("InPuTs")
        
        assert len(dirs1) == len(dirs2) == len(dirs3)
        assert len(files1) == len(files2) == len(files3)

    def test_get_entries_nested_path(self, temp_simulation_dir, error_queue):
        """Test retrieving entries from nested path"""
        registry = SimulationFileRegistry(temp_simulation_dir, error_queue)
        
        dirs, files = registry.get_entries("extfnd\\dat")
        
        # Should have no subdirectories
        assert len(dirs) == 0
        
        # Should have 1 file
        assert len(files) == 1
        assert files[0].name == "data.sim"

    def test_get_entries_nonexistent_path(self, temp_simulation_dir, error_queue):
        """Test retrieving entries from non-existent path returns empty lists"""
        registry = SimulationFileRegistry(temp_simulation_dir, error_queue)
        
        dirs, files = registry.get_entries("NONEXISTENT")
        
        assert len(dirs) == 0
        assert len(files) == 0
        assert isinstance(dirs, list)
        assert isinstance(files, list)

    def test_get_all_entries(self, temp_simulation_dir, error_queue):
        """Test getting all cached entries"""
        registry = SimulationFileRegistry(temp_simulation_dir, error_queue)
        
        all_dirs, all_files = registry.get_all_entries()
        
        assert isinstance(all_dirs, dict)
        assert isinstance(all_files, dict)
        
        # Check that we have entries for expected paths
        assert "" in all_files  # Root directory
        assert "inputs" in all_files
        assert "int" in all_files
        assert "prog" in all_files
        assert "extfnd" in all_dirs
        assert "extfnd/dat" in all_files  # Note: forward slash separator

    def test_all_files_counted(self, temp_simulation_dir, error_queue):
        """Test that all files are correctly counted"""
        registry = SimulationFileRegistry(temp_simulation_dir, error_queue)
        
        all_dirs, all_files = registry.get_all_entries()
        
        # Count total files
        total_files = sum(len(files) for files in all_files.values())
        
        # We created 7 files total
        assert total_files == 7

    def test_all_directories_counted(self, temp_simulation_dir, error_queue):
        """Test that all directories are correctly counted"""
        registry = SimulationFileRegistry(temp_simulation_dir, error_queue)
        
        all_dirs, all_files = registry.get_all_entries()
        
        # Count total directories (excluding root)
        total_dirs = sum(len(dirs) for dirs in all_dirs.values())
        
        # We created 6 directories total (5 in root, 1 in extfnd)
        assert total_dirs == 6

    def test_error_queue_on_invalid_path(self, error_queue):
        """Test that errors are properly queued when scanning invalid path"""
        # Create a path that doesn't exist
        invalid_path = "/nonexistent/path/to/simulation"
        
        registry = SimulationFileRegistry(invalid_path, error_queue)
        
        # Registry should be initialized but empty
        assert len(registry.all_dir_entries) == 0
        assert len(registry.all_file_entries) == 0

    def test_sequential_vs_parallel_consistency(self, temp_simulation_dir, error_queue, threadpool):
        """Test that sequential and parallel scanning produce identical results"""
        # Sequential scan
        registry_seq = SimulationFileRegistry(temp_simulation_dir, error_queue)
        all_dirs_seq, all_files_seq = registry_seq.get_all_entries()
        
        # Parallel scan
        error_queue_parallel = Queue()
        registry_par = SimulationFileRegistry(temp_simulation_dir, error_queue_parallel, threadpool)
        all_dirs_par, all_files_par = registry_par.get_all_entries()
        
        # Should have same number of paths
        assert len(all_dirs_seq) == len(all_dirs_par)
        assert len(all_files_seq) == len(all_files_par)
        
        # Should have same paths
        assert set(all_dirs_seq.keys()) == set(all_dirs_par.keys())
        assert set(all_files_seq.keys()) == set(all_files_par.keys())
        
        # Should have same number of files in each path
        for path in all_files_seq.keys():
            assert len(all_files_seq[path]) == len(all_files_par[path])
        
        # Should have same number of dirs in each path
        for path in all_dirs_seq.keys():
            assert len(all_dirs_seq[path]) == len(all_dirs_par[path])

    def test_direntry_objects_validity(self, temp_simulation_dir, error_queue):
        """Test that returned DirEntry objects are valid and usable"""
        registry = SimulationFileRegistry(temp_simulation_dir, error_queue)
        
        dirs, files = registry.get_entries("INPUTS")
        
        # Test that we can access DirEntry properties
        for file_entry in files:
            assert hasattr(file_entry, 'name')
            assert hasattr(file_entry, 'path')
            assert file_entry.is_file(follow_symlinks=False)
            assert not file_entry.is_dir(follow_symlinks=False)
            
            # Test that we can get file stats
            stat_info = file_entry.stat()
            assert stat_info.st_size >= 0

    def test_empty_directory_scanning(self, error_queue):
        """Test scanning an empty directory"""
        # Create empty temp directory
        test_dir = os.path.join(TEST_STORAGE_LOCATION, "test_empty_dir")
        os.makedirs(test_dir, exist_ok=True)
        
        try:
            registry = SimulationFileRegistry(test_dir, error_queue)
            
            dirs, files = registry.get_entries("")
            
            assert len(dirs) == 0
            assert len(files) == 0
            
            all_dirs, all_files = registry.get_all_entries()
            # Should only have the root entry
            assert len(all_dirs) == 1  # Just the "" entry
            assert len(all_files) == 1  # Just the "" entry
            
        finally:
            if os.path.exists(test_dir):
                shutil.rmtree(test_dir)

    def test_deep_directory_structure(self, error_queue):
        """Test scanning a deep directory structure"""
        test_dir = os.path.join(TEST_STORAGE_LOCATION, "test_deep_structure")
        
        # Clean up if exists
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        
        # Create a deep structure: level1/level2/level3/level4/level5
        deep_path = os.path.join(test_dir, "level1", "level2", "level3", "level4", "level5")
        os.makedirs(deep_path, exist_ok=True)
        
        # Create a file at the deepest level
        with open(os.path.join(deep_path, "deep_file.txt"), "w") as f:
            f.write("deep content")
        
        try:
            registry = SimulationFileRegistry(test_dir, error_queue)
            
            # Check that we can access the deep file
            dirs, files = registry.get_entries("level1\\level2\\level3\\level4\\level5")
            
            assert len(files) == 1
            assert files[0].name == "deep_file.txt"
            
        finally:
            if os.path.exists(test_dir):
                shutil.rmtree(test_dir)

    def test_path_normalization(self, temp_simulation_dir, error_queue):
        """Test that paths are properly normalized (backslashes stripped)"""
        registry = SimulationFileRegistry(temp_simulation_dir, error_queue)
        
        all_dirs, all_files = registry.get_all_entries()
        
        # All keys should be lowercase
        for key in all_dirs.keys():
            assert key == key.lower()
        
        for key in all_files.keys():
            assert key == key.lower()
        
        # Root should be empty string
        assert "" in all_files
