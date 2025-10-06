import pytest
import random
from server.app.web_api import normalize_path
from server.datamodel.dtos import FolderNodeDTO, RootFolderDTO
from .base_integration_test import BaseIntegrationTest
from .testdata_for_import import InMemoryFolderNode, flatten_folder_structure, flatten_multiple_folder_structures


@pytest.mark.integration
@pytest.mark.cleanup_workflow
@pytest.mark.slow
class TestCleanupWorkflows(BaseIntegrationTest):
    """Integration tests for complete cleanup workflows"""
    
    def test_full_cleanup_lifecycle(self, integration_session, cleanup_scenario_data):
        """Test complete cleanup workflow from start to finish"""
        session = integration_session

        # Step 0: Set up a new database and verify that it is empty apart from VTS metadata
        self.setup_new_db_with_vts_metadata(integration_session)

        # Step 1: generate all the simulations we need for the test
        number_of_rootfolders=2
        rootfolder_tuples: list[tuple[RootFolderDTO, InMemoryFolderNode]] = self.generate_simulations(number_of_rootfolders)
        assert len(rootfolder_tuples) > 0
        
        #Split the simulations in three parts:
        # first part with same rootfolder and all its folders
        # second and third part with random split of the remaining folders and rootfolders. part 2 and 3 have the same size +-1

        # part one: reserver the first tuple of (rootfolder, InMemoryFolderNode) and flatten it for later updates
        first_rootfolder_tuple_list = flatten_folder_structure(rootfolder_tuples[0])
        del rootfolder_tuples[0]

        # part two and three: make a random split of folders by 
        # step: create a list of tuple[RootFolderDTO, InMemoryFolderNode] from all folders by iterating top down or breath first through the folder trees
        items: list[tuple[RootFolderDTO, InMemoryFolderNode]] = flatten_multiple_folder_structures(rootfolder_tuples)
        assert len(items) > 0       
        # step: shuffling the list
        random.shuffle(items)
        # step: splitting that random list in half
        mid_index = len(items) // 2
        second_random_rootfolder_tuples: list[tuple[RootFolderDTO, InMemoryFolderNode]] = items[:mid_index]
        third_random_rootfolder_tuples:  list[tuple[RootFolderDTO, InMemoryFolderNode]] = items[mid_index:]

        # Step 2: Insert simulations
        #insert part one
        part_one_leaves:list[tuple[RootFolderDTO, InMemoryFolderNode]] = [(rootfolder,folder) for rootfolder, folder in first_rootfolder_tuple_list if folder.is_leaf]
        first_rootfolder_tuple_list_from_db:list[tuple[RootFolderDTO, FolderNodeDTO]] = self.insert_simulations(session, part_one_leaves)
        # verify that all first_rootfolder_tuple_list are found in first_rootfolder_tuple_list_from_db
        assert len(first_rootfolder_tuple_list) == len(first_rootfolder_tuple_list_from_db)
        #tjek that alle the paths are the same. The inpur at output can be in random order so start by sorting both lists by path
        first_rootfolder_tuple_list.sort(key=lambda x: x[1].path)
        first_rootfolder_tuple_list_from_db.sort(key=lambda x: x[1].path)
        for (input_rootfolder, input_folder), (db_rootfolder, db_folder) in zip(first_rootfolder_tuple_list, first_rootfolder_tuple_list_from_db):
            assert normalize_path(input_rootfolder.path) == normalize_path(db_rootfolder.path)

        # Step 3: Start cleanup round
        cleanup_round = self.start_cleanup_round(session, duration_weeks=4)
        assert cleanup_round.status == "active"
        
        # Step 4: Update retention during cleanup
        retention_changes = {"retention_id": 1, "new_days": 60}
        self.update_retention_during_cleanup(session, retention_changes)
        
        # Step 5: Update simulations during cleanup
        sim_updates = {"sim_old_inactive": {"status": "archived"}}
        self.update_simulations_during_cleanup(session, sim_updates)
        
        # Step 6: Execute cleanup
        results = self.execute_cleanup(session, cleanup_round.id)
        
        # Step 7: Verify final state
        expected = {"cleaned_count": 1, "retained_count": 1}
        self.verify_cleanup_results(session, expected)
        
        assert results["cleaned"] >= expected["cleaned_count"]
    
    def test_retention_policy_changes_during_cleanup(self, integration_session, cleanup_scenario_data):
        """Test how retention policy changes affect ongoing cleanup"""
        session = integration_session
        
        # Setup initial state
        simulations = self.insert_simulations(session, cleanup_scenario_data["simulations"])
        cleanup_round = self.start_cleanup_round(session)
        
        # Change retention policy mid-cleanup
        original_retention = cleanup_scenario_data["retention_policies"][0]
        self.update_retention_during_cleanup(session, {
            "retention_id": original_retention["id"],
            "new_days": original_retention["days"] * 2  # Double retention
        })
        
        # Execute and verify policy change took effect
        results = self.execute_cleanup(session, cleanup_round.id)
        
        # Should retain more items due to extended retention
        expected = {"cleaned_count": 0, "retained_count": 2}
        self.verify_cleanup_results(session, expected)

    def test_simulation_updates_during_cleanup(self, integration_session, cleanup_scenario_data):
        """Test simulation modifications during active cleanup round"""
        session = integration_session
        
        simulations = self.insert_simulations(session, cleanup_scenario_data["simulations"])
        cleanup_round = self.start_cleanup_round(session)
        
        # Update simulation status during cleanup
        self.update_simulations_during_cleanup(session, {
            "sim_old_inactive": {"status": "active"}  # Reactivate old sim
        })
        
        results = self.execute_cleanup(session, cleanup_round.id)
        
        # Reactivated simulation should be retained
        expected = {"cleaned_count": 0, "retained_count": 2}
        self.verify_cleanup_results(session, expected)
