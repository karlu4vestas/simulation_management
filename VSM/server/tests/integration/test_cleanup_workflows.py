from datetime import date
from typing import NamedTuple
import pytest
from datamodel.dtos import CleanupConfiguration, FolderNodeDTO, FolderTypeDTO, FolderTypeEnum, RootFolderDTO
from app.web_api import normalize_path, read_cleanupfrequency_by_domain_id, read_cycle_time_by_domain_id, read_folder_type_dict_pr_domain_id, read_retentiontypes_by_domain_id, read_simulation_domains, read_folder_types_pr_domain_id, get_cleanup_configuration_by_rootfolder_id
from db.db_api import insert_rootfolder
from .base_integration_test import BaseIntegrationTest, RootFolderWithFolderNodeDTOList
from .testdata_for_import import InMemoryFolderNode, RootFolderWithMemoryFolderTree, RootFolderWithMemoryFolders, flatten_folder_structure

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


        # validate that the folder hierarchy is inserted correctly

        # start by getting the input and the output data for the db simulation for each scenarios before procedding to validation
        # The input data is in cleanup_scenario_data fixture in conftest.py
        # The output data is the return from insert_simulations
        # The input data is three parts:
        # part one with one root folder and a list of all its subfolders in random order
        # part two and three with a random split of the second rootfolders list of folders
        class DataIOSet(NamedTuple):
            key:    str  # input to retrieve the scenario data from cleanup_scenario_data in conftest.py
            input:  RootFolderWithMemoryFolders #input rootfolder
            output: RootFolderWithFolderNodeDTOList #output rootfolder and folders from db after insertion of simulations

        dataio_sets: list[DataIOSet] = []
        for key in ["first_rootfolder", "second_rootfolder_part_one", "second_rootfolder_part_two"]:
            #insert add the simulation domain to the rootfolder and inser the rootfolder in the db
            input: RootFolderWithMemoryFolders = cleanup_scenario_data[key]
            # set the simulation domain, save the rootfolder to the db and then create first_rootfolder_tuple with the rootfolder from the db
            input.rootfolder.simulationdomain_id = simulation_domain_id
            input.rootfolder = insert_rootfolder(input.rootfolder)
            #verify that the rootfolder was saved correctly
            assert input.rootfolder is not None
            assert input.rootfolder.id is not None and input.rootfolder.id > 0
            assert input.rootfolder.path == input.rootfolder.path

            # Insert simulations (the leaves). The return will be all folders in the db
            output: RootFolderWithFolderNodeDTOList = self.insert_simulations(integration_session, input)
            str_summary = f"key:{key}, input rootfolder ids: {input.rootfolder.id}, output rootfolder ids: {output.rootfolder.id}"
            print(str_summary)
            dataio_sets.append(DataIOSet(key=key,input=input,output=output))

        str_dataio_sets_ids = [f"{data_set.key}, {data_set.input.rootfolder.id} == {data_set.output.rootfolder.id}   {data_set.input.rootfolder.path} == {data_set.output.rootfolder.path}" for data_set in dataio_sets]
        print(f"Verification of rootfolder ids between input and output: {str_dataio_sets_ids}")

        # Step 2 validate the output data
        leaf_node_type:int = read_folder_type_dict_pr_domain_id(simulation_domain_id)[FolderTypeEnum.VTS_SIMULATION].id
        for data_set in dataio_sets:
            assert data_set.input.rootfolder.id == data_set.output.rootfolder.id  # should never fail because we just got back what we input to insert_simulations

            # extract the the list of leaves in input and output so that it is easy to validate them
            input_simulation_folders  = [ folder for folder in data_set.input.folders  if folder.is_leaf ]
            output_simulation_folders = [ folder for folder in data_set.output.folders if folder.nodetype_id == leaf_node_type ]

            match data_set.key:
                case "first_rootfolder":
                    #part one: consist of the same rootfolder that is not shared with other scenarios

                    #check that the all leaves (the simulations) were inserted
                    assert len(input_simulation_folders) == len(output_simulation_folders)

                    # even if "insert_simulations" only insert the leaves the whole foldertree must have be generated from the leaves in the db
                    assert len(data_set.input.folders) == len(data_set.output.folders)

                    # Check that all the paths are the same. Sort both lists by path because they were inserted in random order
                    data_set.input.folders.sort(key=lambda x: x.path)
                    data_set.output.folders.sort(key=lambda x: x.path)
                    for input_folder, db_folder in zip(data_set.input.folders, data_set.output.folders):
                        assert normalize_path(input_folder.path) == normalize_path(db_folder.path)
                
                case "second_rootfolder_part_one":
                    # The part two and part 3 are about insertion into the same rootfolders: 
                    # part 2 was inserted before part 3 

                    # the number of input and output leafs must be the same in part 2
                    assert len(input_simulation_folders) == len(output_simulation_folders)


                    # Number of input folders must be less than or equal to the number of output folders
                    # because each insert leaf can generate a whole branch of multiple folders
                    assert len(data_set.input.folders) <= len(data_set.output.folders)

                    # Verify all input folders exist in output
                    input_paths = {normalize_path(f.path) for f in data_set.input.folders}
                    output_paths = {normalize_path(f.path) for f in data_set.output.folders}
                    assert input_paths.issubset(output_paths)
                
                case "second_rootfolder_part_two":
                    # The part 2 and part 3 are about insertion into the same rootfolders: 
                    # Part 2 was inserted before part 3 

                    # the number of input leafs must therefore be smaller or equal to the number of output leafs
                    # equality can happen if the split between part 2 and 3 allocated all leafs to part 3
                    assert len(input_simulation_folders) <= len(output_simulation_folders)
                    # Verify all input leaves exist in output
                    input_paths = {normalize_path(f.path) for f in input_simulation_folders}
                    output_paths = {normalize_path(f.path) for f in output_simulation_folders}
                    assert input_paths.issubset(output_paths)


                    # Number of input folders must be less than or equal to the number of output folders
                    # because each insert leaf can generate a whole branch of multiple folders
                    assert len(data_set.input.folders) <= len(data_set.output.folders)

                    # Number of input folders must be less than or equal to the number of output folders                    
                    # Verify all input folder exist in output
                    input_paths = {normalize_path(f.path) for f in data_set.input.folders}
                    output_paths = {normalize_path(f.path) for f in data_set.output.folders}
                    assert input_paths.issubset(output_paths)
                
                case _:
                    raise ValueError(f"Unknown scenario data key: {data_set.key}")


        #verify that the default cleanup configuration cannot start a cleanup round
        #for data_set in dataio_sets:
        #    assert data_set.output.rootfolder.get_cleanup_configuration().can_start_cleanup() is False

        #validate the modified dates are set correctly

        """
        #set the cleanup configuration before starting a cleanup round on the root folder
        cleanup_configuration: CleanupConfiguration
        updated_cleanup_configuration: CleanupConfiguration = self.update_cleanup_configuration(self, session, part_one_rootfolder, cleanup_configuration)
        assert cleanup_configuration==updated_cleanup_configuration
        assert updated_cleanup_configuration.can_start_cleanup()

        # Step 3: Start cleanup round and validate that we get the expected number of marked simulation for cleanup
        # @TODO calculate the simulations that should be marked for cleanup using the modified date and the "updated_cleanup_configuration"
        marked_for_cleanup_in_first_rootfolder = [(rootfolder, folder) for rootfolder, folder in first_rootfolder_tuple_list_from_db if folder.nodetype_id ==  "inactive"]
        marked_for_cleanup: list[(RootFolderDTO, FolderNodeDTO]] = self.start_cleanup_round(session)
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