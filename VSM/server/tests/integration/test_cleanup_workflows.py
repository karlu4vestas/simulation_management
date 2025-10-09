from datetime import date
import pytest
from datamodel.dtos import CleanupConfiguration, FolderNodeDTO, FolderTypeEnum, RootFolderDTO
from app.web_api import insert_rootfolder, normalize_path, read_cleanupfrequency_by_domain_id, read_cycle_time_by_domain_id, read_retentiontypes_by_domain_id, read_simulation_domains, read_folder_types_pr_domain_id, get_cleanup_configuration_by_rootfolder_id
from .base_integration_test import BaseIntegrationTest
from .testdata_for_import import InMemoryFolderNode, flatten_folder_structure

@pytest.mark.integration
@pytest.mark.cleanup_workflow
@pytest.mark.slow
class TestCleanupWorkflows(BaseIntegrationTest):
    """Integration tests for complete cleanup workflows"""

    def test_initialization_with_import_of_simulations(self, integration_session, cleanup_scenario_data):
        """Test intialization of the workflow"""

        # Step 0: Set up a new database and verify that it is empty apart from VTS metadata
        self.setup_new_db_with_vts_metadata(integration_session)
        simulation_domains: list = read_simulation_domains()
        assert len(simulation_domains) > 0
        simulation_domain_id = simulation_domains[0].id
        assert simulation_domain_id is not None and simulation_domain_id > 0
        #verify that the domain has the necessary configuration data
        assert len(read_retentiontypes_by_domain_id(simulation_domain_id)) > 0
        assert len(read_folder_types_pr_domain_id(simulation_domain_id)) > 0
        assert len(read_cleanupfrequency_by_domain_id(simulation_domain_id)) > 0
        assert len(read_cycle_time_by_domain_id(simulation_domain_id)) > 0


        #get the data

        # part one: if the first rootfolders tuple of (rootfolder, InMemoryFolderNode)
        first_rootfolder_tuple: tuple[RootFolderDTO, InMemoryFolderNode] = cleanup_scenario_data["first_rootfolder_tuple"]
        assert len(first_rootfolder_tuple) > 0
        # register the rootfolder in the db
        first_rootfolder: RootFolderDTO = first_rootfolder_tuple[0]
        first_rootfolder.simulationdomain_id = simulation_domain_id
        first_rootfolder = insert_rootfolder(first_rootfolder)
        assert first_rootfolder.id is not None and first_rootfolder.id > 0
        # create a new tuple with the rootfolder from the db
        first_rootfolder_tuple: tuple[RootFolderDTO, InMemoryFolderNode] = (first_rootfolder, first_rootfolder_tuple[1])

        # flatten the tree and keep only the leaves
        first_rootfolder_tuple_list = flatten_folder_structure(first_rootfolder_tuple)
        from app.web_api import FileInfo
        part_one_leaves:list[tuple[RootFolderDTO, FileInfo]] = [(rootfolder,FileInfo(
            filepath=folder.path,
            modified_date=date.today(),
            nodetype=FolderTypeEnum.VTS_SIMULATION,
            retention_id=None,
        )) for rootfolder, folder in first_rootfolder_tuple_list if folder.is_leaf]

        # Step 2: Insert simulations
        #insert the leaves. the return will be all folders in the db
        #the return will be all folders in the db not only the inserted ones
        first_rootfolder_tuple_list_from_db:list[tuple[RootFolderDTO, FolderNodeDTO]] = self.insert_simulations(integration_session, part_one_leaves)
        print(f"first_rootfolder_tuple_list_from_db has {len(first_rootfolder_tuple_list_from_db)} entries")

        # to verify that the foldertree has been save correctly to the db we can control that all first_rootfolder_tuple_list contains the same paths as first_rootfolder_tuple_list_from_db
        """
        assert len(first_rootfolder_tuple_list) == len(first_rootfolder_tuple_list_from_db)
        #tjek that alle the paths are the same. The input at output can be in random order so start by sorting both lists by path
        first_rootfolder_tuple_list.sort(key=lambda x: x[1].path)
        first_rootfolder_tuple_list_from_db.sort(key=lambda x: x[1].path)
        for (input_rootfolder, input_folder), (db_rootfolder, db_folder) in zip(first_rootfolder_tuple_list, first_rootfolder_tuple_list_from_db):
            assert normalize_path(input_rootfolder.path) == normalize_path(db_rootfolder.path)

        # part two and three: are random splits of the remaining rootfolder, folders
        #second_random_rootfolder_tuples: list[tuple[RootFolderDTO, InMemoryFolderNode]] = cleanup_scenario_data["second_random_rootfolder_tuples"]
        #third_random_rootfolder_tuples:  list[tuple[RootFolderDTO, InMemoryFolderNode]] = cleanup_scenario_data["third_random_rootfolder_tuples"]

        # Step 2: Insert simulations

        #assert that  
        # the defualt cleanup configuration is set correctly so that it can not be used to start a cleanup round
        # no simulation is marked for cleanup before we set a cleanup configuration that can start a cleanup round
        part_one_rootfolder: RootFolderDTO = part_one_leaves[0]
        cleanup_configuration: CleanupConfiguration = get_cleanup_configuration_by_rootfolder_id(part_one_rootfolder.id)
        assert cleanup_configuration.can_start_cleanup() is False

        marked_for_cleanup: list[tuple[RootFolderDTO, FolderNodeDTO]] = self.get_marked_for_cleanup(integration_session, part_one_rootfolder)
        assert len(marked_for_cleanup) == 0

        """

        """
        #set the cleanup configuration before starting a cleanup round on the root folder
        cleanup_configuration: CleanupConfiguration
        updated_cleanup_configuration: CleanupConfiguration = self.update_cleanup_configuration(self, session, part_one_rootfolder, cleanup_configuration)
        assert cleanup_configuration==updated_cleanup_configuration
        assert updated_cleanup_configuration.can_start_cleanup()

        # Step 3: Start cleanup round and validate that we get the expected number of marked simulation for cleanup
        # @TODO calculate the simulations that should be marked for cleanup using the modified date and the "updated_cleanup_configuration"
        marked_for_cleanup_in_first_rootfolder = [(rootfolder, folder) for rootfolder, folder in first_rootfolder_tuple_list_from_db if folder.nodetype_id ==  "inactive"]
        marked_for_cleanup: list[tuple[RootFolderDTO, FolderNodeDTO]] = self.start_cleanup_round(session)
        assert len(marked_for_cleanup) ==  len(marked_for_cleanup_in_first_rootfolder)

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
        """
    """
    def test_retention_policy_changes_during_cleanup(self, integration_session, cleanup_scenario_data):
        # Test how retention policy changes affect ongoing cleanup
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
        # Test simulation modifications during active cleanup round
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
"""