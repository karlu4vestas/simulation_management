"""
Integration test for cleanup cycle with real VTS simulations.

This test:
1. Generates real VTS simulations using main_GenerateSimulation
2. Scans them to get FileInfo with proper modified dates using SimulationFileRegistry
3. Runs clean_main to perform cleanup
4. Validates results using validate_cleanup from main_validate_cleanup
"""

from __future__ import annotations
from typing import TYPE_CHECKING
import pytest
import os
import shutil
import sys
from datetime import datetime, timedelta
from queue import Queue

from tests.generate_vts_simulations.GenerateTimeseries import SimulationType

# Add the generate_vts_simulations directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../unittests/generate_vts_simulations'))

from cleanup_cycle.clean_agent.file_utilities import FileStatistics
from tests.generate_vts_simulations.main_GenerateSimulation import GeneratedSimulationsResult, SimulationTestSpecification, generate_simulations, initialize_generate_simulations
from tests.generate_vts_simulations.main_validate_cleanup import validate_cleanup

from cleanup_cycle.clean_agent.clean_main import CleanupResult, clean_main
from cleanup_cycle.clean_agent.clean_parameters import CleanMode
from cleanup_cycle.clean_agent.clean_progress_reporter import CleanProgressWriter
from cleanup_cycle.clean_agent.simulation_file_registry import SimulationFileRegistry
if TYPE_CHECKING:
    from datamodel.retentions import ExternalRetentionTypes, RetentionTypeDTO, Retention
    from datamodel.dtos import FileInfo, FolderTypeEnum

from tests import test_storage

TEST_STORAGE_LOCATION = test_storage.LOCATION

@pytest.mark.integration
class TestCleanupWithOnDiskSimulations:
    # Integration test using VTS simulations generated on-disk. 
    # The content of the .set files are realistic but still generated
    # all othre files have fake content

    @pytest.fixture
    def test_base_path(self):
        # Create and manage test directory for simulations
        test_dir = os.path.join(TEST_STORAGE_LOCATION, "cleanup_integration_test")
        
        # Clean up from previous runs
        if os.path.exists(test_dir):
            print(f"\nCleaning up previous test directory: {test_dir}")
            shutil.rmtree(test_dir)
        
        # Create fresh directory
        os.makedirs(test_dir, exist_ok=True)
        yield test_dir
        
        # Leave for inspection - don't cleanup after test
        print(f"\nTest artifacts left for inspection at: {test_dir}")

    @pytest.fixture
    def path_to_loadcases_configuration_file(self):
        # Get path to json file for loadcase generation
        script_dir = os.path.join(os.path.dirname(__file__), '../generate_vts_simulations')
        return os.path.join(script_dir, "loadcases", "loadcases_ranges.json")

    @pytest.fixture
    def output_dir(self, test_base_path):
        # Create output directory for cleanup logs
        output_path = os.path.join(test_base_path, "cleanup_logs")
        os.makedirs(output_path, exist_ok=True)
        return output_path

    def scan_simulation_and_get_fileinfo(self, simulation_path: str) -> tuple[FileInfo, SimulationFileRegistry]:
        from datamodel.dtos import FileInfo, FolderTypeEnum
        from datamodel.retentions import ExternalRetentionTypes
        # Build the FileInfo requred for cleanup
        # The cleanup algorithm will ignore cleanup if the simulations modified_date has changed, 
        # because changing the simulation is a legitimate way for the user to signal that i need to work on this simulation 
        #
        # The modified date would originally come from scanning the storage for simulation.
        # To "emulate" this this function uses the SimulationFileRegistry to get the correct modified_date for FileInfo

        # Args:    simulation_path: Path to the simulation directory
        # Returns: FileInfo object with correct modified_date from SimulationFileRegistry scan
        error_queue = Queue()
        file_registry: SimulationFileRegistry = SimulationFileRegistry(simulation_path, error_queue)
        filestats: FileStatistics = file_registry.get_simulation_statistics()
        
        # filestats.max_date is now a datetime object (not a timestamp)
        modified_date: datetime = filestats.max_date
        
        # Create FileInfo with the scanned modified_date
        file_info = FileInfo(
            filepath=simulation_path,
            modified_date=modified_date,
            nodetype=FolderTypeEnum.SIMULATION,
            external_retention=ExternalRetentionTypes.NUMERIC
        )
        
        return file_info, file_registry

    def test_cleanup_analyse_mode_for_ondisk_simulations(self, test_base_path: str, path_to_loadcases_configuration_file: str, output_dir: str):
        # Test that cleanup in ANALYSE mode preserves all files 
        
        # Step 1: Generate real VTS simulations
        print("\n=== Generating simulations for ANALYSE mode test ===")
        simulation_folders: SimulationTestSpecification = [(os.path.join(test_base_path, "loadrelease", "analyse_mode_LOADS"), SimulationType.VTS)]
        gen_result: GeneratedSimulationsResult = generate_simulations(test_base_path, simulation_folders, path_to_loadcases_configuration_file)
        
        # Step 2: Scan simulations and count files before cleanup
        print("\n=== Step 2: Scanning simulations ===")
        simulations: list[FileInfo] = []
        file_counts_before: dict[str, int] = {}

        for sim_path in gen_result.simulation_paths:
            # get the FileInfo with the modified date and file count before cleanup using SimulationFileRegistry
            file_info, file_registry = self.scan_simulation_and_get_fileinfo(sim_path)
            simulations.append(file_info)
            _, all_files = file_registry.get_all_entries()
            file_counts_before[sim_path] = len(all_files)
            print(f"Before: Simulation {sim_path}: {file_counts_before[sim_path]} files")
        
        total_files_before = sum(file_counts_before.values())
        print(f"Before: Total files: {total_files_before}")
        
        # Step 3: Run cleanup in ANALYSE mode
        print("\n=== Step 3: Running cleanup in ANALYSE mode ===")
        progress_reporter = CleanProgressWriter(seconds_between_update=5, seconds_between_filelog=60)
        progress_reporter.open(output_dir)
        
        try:
            result: CleanupResult = clean_main(
                simulations=simulations,
                progress_reporter=progress_reporter,
                output_path=output_dir,
                clean_mode=CleanMode.ANALYSE,
                num_sim_workers=2,
                num_deletion_workers=1
            )  
            print(f"\nAnalyse completed:\n{result}")
            
            # Verify analysis ran
            assert result.measures.simulations_processed == len(simulations)
        finally:
            progress_reporter.close()
        
        # Step 4: Verify no files were deleted in ANALYSE mode
        print("\n=== Step 4: Verifying no files were deleted ===")
        file_counts_after: dict[str, int] = {}
        
        for sim_path in gen_result.simulation_paths:
            file_info_after, file_registry = self.scan_simulation_and_get_fileinfo(sim_path)
            simulations.append(file_info_after)
            _, all_files = file_registry.get_all_entries()
            file_counts_after[sim_path] = len(all_files)
            print(f"After: Simulation {sim_path}: {file_counts_after[sim_path]} files")

            # Assert file count unchanged
            assert file_counts_before[sim_path] == file_counts_after[sim_path], \
                f"ANALYSE mode should not delete files in {sim_path}. Before: {file_counts_before[sim_path]}, After: {file_counts_after[sim_path]}"
        
        total_files_after = sum(file_counts_after.values())
        print(f"Total files after cleanup: {total_files_after}")
        
        assert total_files_before == total_files_after, f"ANALYSE mode should not delete any files. Before: {total_files_before}, After: {total_files_after}"
        
        print(f"\n=== ANALYSE mode test completed successfully ===")


    def test_cleanup_cycle_for_htc_simulation_ondisk(self, test_base_path: str, path_to_loadcases_configuration_file: str, output_dir: str):
        # Show that when a htc folder is present immediately under the simulation then the simulation will not be modified but the cleanup
        # is is defensive because we should not import such simulation sot begin with 
        
        # Steps:
        # 1. Generate VTS simulation with a HTC folder on-disk using main_GenerateSimulation
        # 2. Scan them to create FileInfo objects with correct modified dates
        # 3. Run clean_main in DELETE mode
        # 4. Validate cleanup that the cleanup did not delete any files
        
        # Step 1: Generate real VTS simulations
        print("\n=== Step 1: Generating simulations ===")
        simulation_folders: SimulationTestSpecification = [(os.path.join(test_base_path, "loadrelease", "HTC_LOADS"), SimulationType.HTC)]
        gen_result: GeneratedSimulationsResult = generate_simulations(test_base_path, simulation_folders, path_to_loadcases_configuration_file)

        # Verify generation completed
        assert os.path.exists(gen_result.validation_csv_file), "validation.csv not created"
        assert os.path.exists(gen_result.simulations_csv_file), "simulations.csv not created"
        assert len(gen_result.simulation_paths) == len(simulation_folders), f"Expected {len(simulation_folders)} simulations, got {len(gen_result.simulation_paths)}"

        # Step 2: Scan all generated simulations to create FileInfo objects with proper modified dates
        print("\n=== Step 2: Scanning simulations to create FileInfo objects ===")
        simulations: list[FileInfo] = []
        file_counts_before: dict[str, int] = {}
        for sim_path in gen_result.simulation_paths:
            file_info, file_registry = self.scan_simulation_and_get_fileinfo(sim_path)
            file_counts_before[sim_path] = len(file_registry.get_all_entries()[1])
            simulations.append(file_info)
        assert len(simulations) == len(gen_result.simulation_paths), "Not all simulations scanned successfully"
        
        # Step 3: Run cleanup in DELETE mode
        print("\n=== Step 3: Running cleanup ===")
        progress_reporter = CleanProgressWriter(seconds_between_update=5, seconds_between_filelog=60)
        progress_reporter.open(output_dir)
        
        try:
            result: CleanupResult = clean_main(
                simulations=simulations,
                progress_reporter=progress_reporter,
                output_path=output_dir,
                clean_mode=CleanMode.DELETE,
                num_sim_workers=2,
                num_deletion_workers=1,
                deletion_queue_max_size=10000
            )
            print(f"Cleanup completed:\n{result}")
            
            # Verify cleanup run
            assert result.measures.simulations_processed == len(simulations), \
                f"Expected {len(simulations)} processed, got {result.measures.simulations_processed}"

            # Verify that the count of simulations processed matches the simulations processed categories
            assert (result.measures.simulations_cleaned + result.measures.simulations_issue + result.measures.simulations_skipped) \
                    == result.measures.simulations_processed, "Sum of cleaned/issue/skipped should equal processed"
            
        finally:
            progress_reporter.close()
        
        # Step 4: Validate cleanup results
        print("\n=== Step 4: Validating cleanup ===")
        print("\n=== Step 2: Scanning for all files ===")
        file_counts_after: dict[str, int] = {}

        error_queue = Queue()
        for sim_path in gen_result.simulation_paths:
            file_registry: SimulationFileRegistry = SimulationFileRegistry(sim_path, error_queue)
            file_counts_after[sim_path] = len(file_registry.get_all_entries()[1])
            assert file_counts_before[sim_path] == file_counts_after[sim_path], \
                f"HTC simulation cleanup should not delete files in {sim_path}. Before: {file_counts_before[sim_path]}, After: {file_counts_after[sim_path]}"

        print(f"The htc simulation is unchanged. Artifacts preserved at: {test_base_path}")

    def test_cleanup_cycle_ondisk_simulations(self, test_base_path: str, path_to_loadcases_configuration_file: str, output_dir: str):
        # Full integration test of cleanup cycle with ondisk simulations.
        
        # Steps:
        # 1. Generate VTS simulations on-disk using main_GenerateSimulation
        # 2. Scan them to create FileInfo objects with correct modified dates
        # 3. Run clean_main in DELETE mode
        # 4. Validate cleanup using validation.csv
        
        # Step 1: Generate real VTS simulations
        print("\n=== Step 1: Generating simulations ===")
        simulation_folders: SimulationTestSpecification = [(os.path.join(test_base_path, "loadrelease", "delete_mode_LOADS"), SimulationType.VTS)]
        gen_result: GeneratedSimulationsResult = generate_simulations(test_base_path, simulation_folders, path_to_loadcases_configuration_file)

        # Verify generation completed
        assert os.path.exists(gen_result.validation_csv_file), "validation.csv not created"
        assert os.path.exists(gen_result.simulations_csv_file), "simulations.csv not created"
        assert len(gen_result.simulation_paths) == len(simulation_folders), f"Expected {len(simulation_folders)} simulations, got {len(gen_result.simulation_paths)}"

        # Step 2: Scan all generated simulations to create FileInfo objects with proper modified dates
        print("\n=== Step 2: Scanning simulations to create FileInfo objects ===")
        simulations: list[FileInfo] = []
        for sim_path in gen_result.simulation_paths:
            file_info, file_registry = self.scan_simulation_and_get_fileinfo(sim_path)
            simulations.append(file_info)
            print(f" Simulation {sim_path} has modified date: {file_info.modified_date}")
        assert len(simulations) == len(gen_result.simulation_paths), "Not all simulations scanned successfully"
        
        # Step 3: Run cleanup in DELETE mode
        print("\n=== Step 3: Running cleanup ===")
        progress_reporter = CleanProgressWriter(seconds_between_update=5, seconds_between_filelog=60)
        progress_reporter.open(output_dir)
        
        try:
            result: CleanupResult = clean_main(
                simulations=simulations,
                progress_reporter=progress_reporter,
                output_path=output_dir,
                clean_mode=CleanMode.DELETE,
                num_sim_workers=2,
                num_deletion_workers=1,
                deletion_queue_max_size=10000
            )
            print(f"Cleanup completed:\n{result}")
            
            # Verify cleanup run
            assert result.measures.simulations_processed == len(simulations), \
                f"Expected {len(simulations)} processed, got {result.measures.simulations_processed}"

            # Verify that the count of simulations processed matches the simulations processed categories
            assert (result.measures.simulations_cleaned + result.measures.simulations_issue + result.measures.simulations_skipped) \
                    == result.measures.simulations_processed, "Sum of cleaned/issue/skipped should equal processed"
            
        finally:
            progress_reporter.close()
        
        # Step 4: Validate cleanup results
        print("\n=== Step 4: Validating cleanup ===")
        filepath_to_validation_results, failed_filepaths = validate_cleanup(gen_result.validation_csv_file)
        
        print(f"\nValidation results saved to: {filepath_to_validation_results}")
        
        if len(failed_filepaths) > 0:
            print(f"\nFound {len(failed_filepaths)} validation failures:")
            for path in failed_filepaths:
                print(f"  Path: {path}")
        else:
            print("\nAll validations passed! Cleanup matched expected results.")
        
        # The test passes even if there are validation failures, 
        # as we're just testing the integration.
        # For strict validation, uncomment:
        assert len(failed_filepaths) == 0, f"Cleanup validation failed for {len(failed_filepaths)} paths:\n" + "\n".join(failed_filepaths)
        
        print(f"\n=== Test completed successfully ===")
        print(f"Artifacts preserved at: {test_base_path}")
