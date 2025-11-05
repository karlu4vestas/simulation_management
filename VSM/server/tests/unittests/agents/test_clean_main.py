# Unit tests for clean_main cleanup functionality.

import pytest
import os
import shutil
from datetime import date
from cleanup_cycle.clean_agent.clean_main import clean_main
from datamodel.dtos import FileInfo, FolderTypeEnum, ExternalRetentionTypes
from cleanup_cycle.clean_agent.clean_parameters import CleanMode
from cleanup_cycle.clean_agent.clean_progress_reporter import CleanProgressWriter
from tests import test_storage

# Global test storage location
TEST_STORAGE_LOCATION = test_storage.LOCATION

class TestCleanMain:
    """Test the clean_main function with various scenarios"""

    @pytest.fixture
    def temp_output_dir(self):
        """Create a test directory for test output in io_dir_for_storage_test"""
        test_dir = os.path.join(TEST_STORAGE_LOCATION, "test_clean_main_output")
        os.makedirs(test_dir, exist_ok=True)
        yield test_dir
        # Cleanup after test
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

    @pytest.fixture
    def sample_simulations(self):
        """Create sample simulation data for testing"""
        return [
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, "project1/simulation_001"),
                modified_date=date(2024, 1, 15),
                nodetype=FolderTypeEnum.VTS_SIMULATION,
                external_retention=ExternalRetentionTypes.UNDEFINED
            ),
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, "project1/simulation_002"),
                modified_date=date(2024, 2, 20),
                nodetype=FolderTypeEnum.VTS_SIMULATION,
                external_retention=ExternalRetentionTypes.UNDEFINED
            ),
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, "project2/simulation_003"),
                modified_date=date(2024, 3, 10),
                nodetype=FolderTypeEnum.VTS_SIMULATION,
                external_retention=ExternalRetentionTypes.UNDEFINED
            ),
        ]

    @pytest.fixture
    def progress_reporter(self, temp_output_dir):
        """Create and open a progress reporter for testing"""
        reporter = CleanProgressWriter(
            seconds_between_update=5,
            seconds_between_filelog=60
        )
        reporter.open(temp_output_dir)
        yield reporter
        reporter.close()

    def test_clean_main_analyse_mode(self, sample_simulations, progress_reporter, temp_output_dir):
        """Test clean_main in ANALYSE mode"""
        result = clean_main(
            simulations=sample_simulations,
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.ANALYSE,
            num_sim_workers=2,
            num_deletion_workers=1,
            deletion_queue_max_size=1000
        )
        
        # Verify result structure
        assert result is not None
        assert hasattr(result, 'results')
        assert hasattr(result, 'measures')
        
        # Verify measures
        assert result.measures.simulations_processed == 3
        assert result.measures.simulations_cleaned == 0
        assert result.measures.simulations_issue == 0
        assert result.measures.simulations_skipped == 3
        assert result.measures.files_deleted == 0
        assert result.measures.bytes_deleted == 0
        assert result.measures.error_count == 0
        
        # Verify results list
        assert len(result.results) == 3
        for sim_result in result.results:
            assert hasattr(sim_result, 'filepath')
            assert hasattr(sim_result, 'modified_date')
            assert hasattr(sim_result, 'nodetype')
            assert hasattr(sim_result, 'external_retention')
            assert sim_result.nodetype == None # because the simulation folder created in the test is empty
            assert sim_result.external_retention == None # because the simulation folder created in the test is empty

    def test_clean_main_empty_simulations(self, progress_reporter, temp_output_dir):
        """Test clean_main with empty simulation list"""
        result = clean_main(
            simulations=[],
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.ANALYSE,
            num_sim_workers=2,
            num_deletion_workers=1
        )
        
        assert result.measures.simulations_processed == 0
        assert len(result.results) == 0

    def test_clean_main_single_simulation(self, progress_reporter, temp_output_dir):
        """Test clean_main with a single simulation"""
        single_sim = [
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, "single/simulation"),
                modified_date=date(2024, 5, 1),
                nodetype=FolderTypeEnum.VTS_SIMULATION,
                external_retention=ExternalRetentionTypes.UNDEFINED
            )
        ]
        
        result = clean_main(
            simulations=single_sim,
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.ANALYSE,
            num_sim_workers=1,
            num_deletion_workers=1
        )
        
        assert result.measures.simulations_processed == 1
        assert len(result.results) == 1
        assert result.results[0].filepath == os.path.join(TEST_STORAGE_LOCATION, "single/simulation")

    def test_clean_main_multiple_workers(self, sample_simulations, progress_reporter, temp_output_dir):
        """Test clean_main with multiple workers"""
        result = clean_main(
            simulations=sample_simulations,
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.ANALYSE,
            num_sim_workers=4,
            num_deletion_workers=2,
            deletion_queue_max_size=10000
        )
        
        # Should process all simulations regardless of worker count
        assert result.measures.simulations_processed == 3
        assert len(result.results) == 3

    def test_clean_main_output_files_created(self, sample_simulations, progress_reporter, temp_output_dir):
        """Test that clean_main creates expected output files"""
        result = clean_main(
            simulations=sample_simulations,
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.ANALYSE,
            num_sim_workers=2,
            num_deletion_workers=1
        )
        
        # Check that error log file was created
        error_log = os.path.join(temp_output_dir, "clean_errors.csv")
        assert os.path.exists(error_log)
        
        # Progress log should also exist if progress reporter created it
        progress_log = os.path.join(temp_output_dir, "clean_progress_log.csv")
        # Note: Progress log may not exist if no updates were written during the short test

    def test_clean_main_result_contains_all_simulations(self, sample_simulations, progress_reporter, temp_output_dir):
        """Test that results contain all input simulations"""
        result = clean_main(
            simulations=sample_simulations,
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.ANALYSE,
            num_sim_workers=2,
            num_deletion_workers=1
        )
        
        # All input simulations should be in results
        input_paths = {sim.filepath for sim in sample_simulations}
        result_paths = {sim.filepath for sim in result.results}
        
        assert input_paths == result_paths

    def test_clean_main_delete_mode(self, sample_simulations, progress_reporter, temp_output_dir):
        """Test clean_main in DELETE mode (no actual files exist in stub)"""
        result = clean_main(
            simulations=sample_simulations,
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.DELETE,
            num_sim_workers=2,
            num_deletion_workers=1
        )
        
        # Even in DELETE mode, stub simulations won't find actual files
        assert result.measures.simulations_processed == 3
        assert result.measures.files_deleted == 0
        assert result.measures.bytes_deleted == 0
