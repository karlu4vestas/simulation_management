"""
Test the scan agent without requiring a database.

Test Steps:
1. Create simulation data as in "test_scheduler_and_agents_with_full_cleanup_round"
2. Run a scan using the on_premise_scan_agent:
   - Use ProgressWriter to avoid CleanupTaskManager.task_progress (which requires DB)
   - Call execute_task() directly (not run() which would try to reserve/complete tasks via DB)
   - Override task_insert_or_update_simulations_in_db() to capture results locally
3. Verify the scan identified the simulation "leafs" correctly
"""

import os
import shutil
import pytest
from unittest.mock import patch

from cleanup.agent_on_premise_scan import AgentScanVTSRootFolder
from cleanup.scheduler_dtos import ActionType, CleanupTaskDTO
from cleanup.agent_task_manager import AgentTaskManager
from datamodel.dtos import FileInfo
from tests.integration.testdata_for_import import RootFolderWithMemoryFolders
from tests.integration import test_scheduler_and_agents
from tests import test_storage


TEST_STORAGE_LOCATION = test_storage.LOCATION


class MockAgentScanVTSRootFolder(AgentScanVTSRootFolder):
    """Test-specific scan agent that captures results locally instead of inserting to DB."""
    
    def __init__(self):
        super().__init__()
        self.extracted_simulations: list[FileInfo] = []
        self.n_hierarchical_simulations: int = 0
        self.progress_messages: list[str] = []
    
    def insert_or_update_simulations_in_db(self, task_id: int, extracted_simulations: list[FileInfo]) -> dict[str, str]:
        """Override to capture simulations locally instead of inserting to DB."""
        self.extracted_simulations = extracted_simulations
        return {"status": "success", "count": str(len(extracted_simulations))}


@pytest.mark.unit
class TestOnPremiseScanAgent:
    
    def test_scan_agent_without_db(self, cleanup_scenario_data):
        """Test the scan agent can identify VTS simulations without requiring a database."""
        
        # Step 1: Prepare the storage with simulations to be scanned
        rootfolder_data: RootFolderWithMemoryFolders = cleanup_scenario_data["first_rootfolder"]
        
        # Setup folder for the test
        io_dir_for_storage_test: str = os.path.join( os.path.normpath(TEST_STORAGE_LOCATION), "test_unit_scan_agent")
        
        # Clean before test (so results can be inspected in case of failure)
        if os.path.isdir(io_dir_for_storage_test):
            shutil.rmtree(io_dir_for_storage_test)
        
        # Generate simulations on disk
        gen_sim_results = test_scheduler_and_agents.TestSchedulerAndAgents.generate_simulations_folder_and_files(
            io_dir_for_storage_test, 
            rootfolder_data
        )
        
        # Set environment variables for the scan agent
        os.environ['SCAN_TEMP_FOLDER'] = os.path.join(io_dir_for_storage_test, "temp_for_scanning")
        os.environ['SCAN_THREADS'] = str(1)
        
        # Step 2: Run the scan agent
        agent = MockAgentScanVTSRootFolder()
        
        # Create a mock task (normally created by scheduler and reserved by agent)
        mock_task = CleanupTaskDTO(
            id=1,
            calendar_id=1,
            rootfolder_id=1,
            path=rootfolder_data.rootfolder.path,
            task_offset=0,
            action_type=ActionType.SCAN_ROOTFOLDER.value,
            storage_id="local",
            status="reserved"
        )
        
        # Set the task on the agent (bypass reserve_task())
        agent.task = mock_task
        
        # Mock CleanupTaskManager.task_progress to avoid DB calls
        def mock_task_progress(task_id: int, message: str):
            agent.progress_messages.append(message)
            return mock_task
        
        with patch.object(AgentTaskManager, 'task_progress', side_effect=mock_task_progress):
            # Execute the task directly (bypass run() which would try to reserve/complete via DB)
            agent.execute_task()
        
        # Step 3: Verify the scan results
        # Check that no errors occurred
        assert agent.error_message is None, f"Scan failed with error: {agent.error_message}"
        
        # Check that simulations were extracted
        assert len(agent.extracted_simulations) > 0, "No simulations were extracted"
        
        # Expected number of leaf folders (VTS simulations) from the test data
        expected_leaf_count = len([folder for folder in rootfolder_data.folders if folder.is_leaf])
        
        # Verify we found the expected number of simulations
        assert len(agent.extracted_simulations) == expected_leaf_count, \
            f"Expected {expected_leaf_count} simulations, but found {len(agent.extracted_simulations)}"
        
        # Verify all extracted simulations are FileInfo objects with proper paths
        for sim in agent.extracted_simulations:
            assert isinstance(sim, FileInfo), f"Expected FileInfo object, got {type(sim)}"
            assert sim.filepath, "Simulation filepath is empty"
            assert os.path.isabs(sim.filepath), f"Simulation path is not absolute: {sim.filepath}"
        
        print(f"\n✓ Successfully scanned {len(agent.extracted_simulations)} simulations")
        print(f"✓ Scan metadata saved to: {os.environ['SCAN_TEMP_FOLDER']}")
