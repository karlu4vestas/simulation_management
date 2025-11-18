"""
Unit tests for clean_main with database usage patterns.

These tests demonstrate and verify how to use clean_main with data
from database queries in various formats.
"""
import pytest
import os
import shutil
from datetime import date
from dataclasses import dataclass
from cleanup.clean_agent.clean_main import CleanupResult, clean_main
from datamodel.dtos import FileInfo, FolderTypeEnum
from datamodel.retentions import ExternalRetentionTypes
from cleanup.clean_agent.clean_parameters import CleanMode
from cleanup.clean_agent.clean_progress_reporter import CleanProgressWriter
from tests import test_storage


# Global test storage location
TEST_STORAGE_LOCATION = test_storage.LOCATION


class TestCleanMainDatabaseUsage:
    """Test clean_main with various database result formats"""

    @pytest.fixture
    def temp_output_dir(self):
        """Create a test directory for test output in io_dir_for_storage_test"""
        test_dir = os.path.join(TEST_STORAGE_LOCATION, "test_clean_main_db_output")
        os.makedirs(test_dir, exist_ok=True)
        yield test_dir
        # Cleanup after test
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

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

    def test_clean_main_with_tuple_database_results(self, progress_reporter, temp_output_dir):
        """Test converting database tuples to FileInfo"""
        # Simulate database query results as list of tuples
        db_results = [
            (os.path.join(TEST_STORAGE_LOCATION, "vts/project_A/sim_001"), date(2024, 1, 15)),
            (os.path.join(TEST_STORAGE_LOCATION, "vts/project_A/sim_002"), date(2024, 2, 20)),
            (os.path.join(TEST_STORAGE_LOCATION, "vts/project_B/sim_003"), date(2024, 3, 10)),
        ]
        
        # Convert to FileInfo list (as would be done in production)
        simulations = [ FileInfo( filepath=row[0],
                                  modified_date=row[1],
                                  nodetype=FolderTypeEnum.SIMULATION,
                                 external_retention=ExternalRetentionTypes.NUMERIC )
                        for row in db_results ]
        
        # Call clean_main
        result = clean_main(
            simulations=simulations,
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.ANALYSE,
            num_sim_workers=2,
            num_deletion_workers=1
        )
        
        # Verify all simulations were processed
        assert result.measures.simulations_processed == 3
        assert len(result.results) == 3
        
        # Verify paths match
        result_paths = {r.filepath for r in result.results}
        expected_paths = {row[0] for row in db_results}
        assert result_paths == expected_paths

    def test_clean_main_with_dict_database_results(self, progress_reporter, temp_output_dir):
        """Test converting database dict results to FileInfo"""
        # Simulate database query results as list of dicts
        db_results_dict = [
            {"path": os.path.join(TEST_STORAGE_LOCATION, "server/sim1"), "modified_date": date(2024, 1, 15), "owner": "user1"},
            {"path": os.path.join(TEST_STORAGE_LOCATION, "server/sim2"), "modified_date": date(2024, 2, 20), "owner": "user2"},
        ]
        
        # Convert to FileInfo list
        simulations = [
            FileInfo(
                filepath=row["path"],
                modified_date=row["modified_date"],
                nodetype=FolderTypeEnum.SIMULATION,
                external_retention=ExternalRetentionTypes.NUMERIC
            )
            for row in db_results_dict
        ]
        
        result = clean_main(
            simulations=simulations,
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.ANALYSE,
            num_sim_workers=2,
            num_deletion_workers=1
        )
        
        assert result.measures.simulations_processed == 2
        assert len(result.results) == 2

    def test_clean_main_with_dataclass_database_results(self, progress_reporter, temp_output_dir):
        """Test converting database dataclass results to FileInfo"""
        
        @dataclass
        class SimulationFromDB:
            id: int
            path: str
            modified_date: date
            owner: str
            status: str
        
        # Simulate database results as dataclasses
        db_simulations = [
            SimulationFromDB(1, os.path.join(TEST_STORAGE_LOCATION, "server/sim1"), date(2024, 1, 15), "user1", "ready"),
            SimulationFromDB(2, os.path.join(TEST_STORAGE_LOCATION, "server/sim2"), date(2024, 2, 20), "user2", "ready"),
            SimulationFromDB(3, os.path.join(TEST_STORAGE_LOCATION, "server/sim3"), date(2024, 3, 25), "user3", "ready"),
        ]
        
        # Convert to FileInfo
        simulations = [
            FileInfo(
                filepath=sim.path,
                modified_date=sim.modified_date,
                nodetype=FolderTypeEnum.SIMULATION,
                external_retention=ExternalRetentionTypes.NUMERIC
            )
            for sim in db_simulations
        ]
        
        result = clean_main(
            simulations=simulations,
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.ANALYSE,
            num_sim_workers=4,
            num_deletion_workers=2
        )
        
        assert result.measures.simulations_processed == 3
        assert len(result.results) == 3

    def test_clean_main_result_processing(self, progress_reporter, temp_output_dir):
        """Test processing results for database updates"""
        simulations = [
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, "test/sim1"),
                modified_date=date(2024, 1, 1),
                nodetype=FolderTypeEnum.SIMULATION,
                external_retention=ExternalRetentionTypes.NUMERIC
            ),
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, "test/sim2"),
                modified_date=date(2024, 2, 1),
                nodetype=FolderTypeEnum.SIMULATION,
                external_retention=ExternalRetentionTypes.NUMERIC
            ),
        ]
        
        result = clean_main(
            simulations=simulations,
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.ANALYSE,
            num_sim_workers=2,
            num_deletion_workers=1
        )
        
        # Process results (as would be done for database updates)
        cleaned_count = 0
        issue_count = 0
        skipped_count = 0
        
        for sim_result in result.results:
            if sim_result.external_retention == ExternalRetentionTypes.CLEAN:
                cleaned_count += 1
            elif sim_result.external_retention == ExternalRetentionTypes.ISSUE:
                issue_count += 1
            else:
                skipped_count += 1
        
        # Verify counts match measures
        assert cleaned_count == result.measures.simulations_cleaned
        assert issue_count == result.measures.simulations_issue
        assert skipped_count == result.measures.simulations_skipped

    def test_clean_main_large_batch(self, progress_reporter, temp_output_dir):
        """Test clean_main with larger batch of simulations"""
        # Create 20 simulations
        simulations = [
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, f"test/project{i//5}/sim_{i:03d}"),
                modified_date=date(2024, (i % 12) + 1, 1),
                nodetype=FolderTypeEnum.SIMULATION,
                external_retention=ExternalRetentionTypes.NUMERIC
            )
            for i in range(20)
        ]
        
        result = clean_main(
            simulations=simulations,
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.ANALYSE,
            num_sim_workers=4,
            num_deletion_workers=2,
            deletion_queue_max_size=10000
        )
        
        # All simulations should be processed
        assert result.measures.simulations_processed == 20
        assert len(result.results) == 20

    def test_clean_main_minimal_config(self, temp_output_dir):
        """Test clean_main with minimal configuration"""
        simulations = [
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, "test/sim"),
                modified_date=date(2024, 1, 1),
                nodetype=FolderTypeEnum.SIMULATION,
                external_retention=ExternalRetentionTypes.NUMERIC
            )
        ]
        
        # Create minimal progress reporter
        reporter = CleanProgressWriter(
            seconds_between_update=5,
            seconds_between_filelog=60
        )
        reporter.open(temp_output_dir)
        
        try:
            result = clean_main(
                simulations=simulations,
                progress_reporter=reporter,
                output_path=temp_output_dir,
                clean_mode=CleanMode.ANALYSE
            )
            
            assert result.measures.simulations_processed == 1
        finally:
            reporter.close()

    def test_fileinfo_attributes_preserved(self, progress_reporter, temp_output_dir):
        # if we try to clean an empty folder  with no file then we must expect that the folder is not recognised as a simulation.
        # As such it attributes will be NONE except for the path to the folder 
        test_date = date(2024, 6, 15)
        test_path = os.path.join(TEST_STORAGE_LOCATION, "test/specific/path")
        simulations = [
            FileInfo(
                filepath=test_path,
                modified_date=test_date,
                nodetype=FolderTypeEnum.SIMULATION,
                external_retention=ExternalRetentionTypes.NUMERIC
            )
        ]
        
        result = clean_main(
            simulations=simulations,
            progress_reporter=progress_reporter,
            output_path=temp_output_dir,
            clean_mode=CleanMode.ANALYSE,
            num_sim_workers=1,
            num_deletion_workers=1
        )
        
        # Verify all attributes are preserved
        assert len(result.results) == 1
        result_sim: CleanupResult = result.results[0]
        
        assert result_sim.filepath == test_path
        assert result_sim.modified_date == None
        assert result_sim.nodetype == None
        assert result_sim.external_retention == None
