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


        #get the data
        data_set_keys: list[str] = ["first_rootfolder_tuple", "second_random_rootfolder_tuples", "third_random_rootfolder_tuples"]
        nodetypes:dict[str,FolderTypeDTO] = read_folder_type_dict_pr_domain_id(simulation_domain_id)
        class DataIOSet(NamedTuple):
            key2scenario_data: str  # input to retrieve the scenario data from cleanup_scenario_data in conftest.py
            input_scenarios: list[RootFolderWithMemoryFolders] #input data
            output: list[RootFolderWithFolderNodeDTOList] #output from db after insertion of simulations

        str_input_set = [f"key:{key}, path:{input.rootfolder.path}" for key in data_set_keys for input in cleanup_scenario_data[key]]
        print(f"str_input_set: {str_input_set}")

        dataio_sets: list[DataIOSet] = []
        for key in data_set_keys:
            input_scenarios: list[RootFolderWithMemoryFolders] = cleanup_scenario_data[key]
            
            input_list: list[RootFolderWithMemoryFolders]=[]
            #start by inserting the rootfolders in the db so that we have the ids
            for input_rootfolder in input_scenarios: 
                # set the simulation domain, save the rootfolder to the db and then create first_rootfolder_tuple with the rootfolder from the db
                input_rootfolder.rootfolder.simulationdomain_id = simulation_domain_id
                rootfolder=insert_rootfolder(input_rootfolder.rootfolder)
                # create a new tuple with the rootfolder from the db
                input_list.append(RootFolderWithMemoryFolders( rootfolder=rootfolder,folders=input_rootfolder.folders))
                
                #verify that the rootfolder was saved correctly
                assert rootfolder.id is not None and rootfolder.id > 0

            # Insert simulations (the leaves). The return will be all folders in the db
            output_list: list[RootFolderWithFolderNodeDTOList] = self.insert_simulations(integration_session, input_list)
            str_input_ids = [f"{input.rootfolder.id}" for input in input_list]
            str_output_ids = [f"{output.rootfolder.id}" for output in output_list]
            str_summary = f"key:{key}, input rootfolder ids: {str_input_ids}, output rootfolder ids: {str_output_ids}"
            print(str_summary)
            dataio_sets.append(DataIOSet(key2scenario_data=key,
                                        input_scenarios=input_list, 
                                        output=output_list))

        str_dataio_sets_ids = [f"{data_set.key2scenario_data}, {input.rootfolder.id} == {output.rootfolder.id}" for data_set in dataio_sets for input, output in zip(data_set.input_scenarios, data_set.output)]
        print(f"Verification of rootfolder ids between input and output: {str_dataio_sets_ids}")

        # Step 2 validate the output data
        for data_set in dataio_sets:     
            assert len(data_set.input_scenarios) == len(data_set.output)  # should never fail because we just got back what we input to insert_simulations
            
            for input, output in zip(data_set.input_scenarios, data_set.output): #iterate over the rootfolders in the scenario
                assert input.rootfolder.id == output.rootfolder.id  # should never fail because we just got back what we input to insert_simulations

                # extract the the list of leaves in input and output so that it is easy to validate them
                leaf_node_type:int = nodetypes[FolderTypeEnum.VTS_SIMULATION].id
                input_simulation_folders  = [ folder for folder in input.folders if folder.is_leaf ]
                output_simulation_folders = [ folder for folder in output.folders if folder.nodetype_id == leaf_node_type ]

                match data_set.key2scenario_data:
                    case "first_rootfolder_tuple":
                        #part one: consist of the same rootfolder that is not shared with other scenarios

                        #check that the all leaves (the simulations) were inserted
                        assert len(input_simulation_folders) == len(output_simulation_folders)

                        # even if "insert_simulations" only insert the leaves the whole foldertree must have be generated from the leaves in the db
                        assert len(input.folders) == len(output.folders)

                        # Check that all the paths are the same. Sort both lists by path because they were inserted in random order
                        input.folders.sort(key=lambda x: x.path)
                        output.folders.sort(key=lambda x: x.path)
                        for input_folder, db_folder in zip(input.folders, output.folders):
                            assert normalize_path(input_folder.path) == normalize_path(db_folder.path)
                    
                    case "second_random_rootfolder_tuples":
                        # The part two and part 3 are about insertion into the same rootfolders: 
                        # part 2 was inserted before part 3 

                        # the number of input and output leafs must be the same in part 2
                        assert len(input_simulation_folders) == len(output_simulation_folders)


                        # Number of input folders must be less than or equal to the number of output folders
                        # because each insert leaf can generate a whole branch of multiple folders
                        assert len(input.folders) <= len(output.folders)

                        # Verify all input folders exist in output
                        input_paths = {normalize_path(f.path) for f in input_simulation_folders}
                        output_paths = {normalize_path(f.path) for f in output_simulation_folders}
                        assert input_paths.issubset(output_paths)
                    
                    case "third_random_rootfolder_tuples":
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
                        assert len(input.folders) <= len(output.folders)

                        # Number of input folders must be less than or equal to the number of output folders                    
                        # Verify all input folder exist in output
                        input_paths = {normalize_path(f.path) for f in input.folders}
                        output_paths = {normalize_path(f.path) for f in output.folders}
                        assert input_paths.issubset(output_paths)
                    
                    case _:
                        raise ValueError(f"Unknown scenario data key: {data_set.key2scenario_data}")




        """
        # part two and three: are random splits of the remaining rootfolder, folders
        #second_random_rootfolder_tuples: list[(RootFolderDTO, InMemoryFolderNode]] = cleanup_scenario_data["second_random_rootfolder_tuples"]
        #third_random_rootfolder_tuples:  list[(RootFolderDTO, InMemoryFolderNode]] = cleanup_scenario_data["third_random_rootfolder_tuples"]

        # Step 2: Insert simulations

        #assert that  
        # the defualt cleanup configuration is set correctly so that it can not be used to start a cleanup round
        # no simulation is marked for cleanup before we set a cleanup configuration that can start a cleanup round
        part_one_rootfolder: RootFolderDTO = part_one_leaves[0]
        cleanup_configuration: CleanupConfiguration = get_cleanup_configuration_by_rootfolder_id(part_one_rootfolder.id)
        assert cleanup_configuration.can_start_cleanup() is False

        marked_for_cleanup: list[(RootFolderDTO, FolderNodeDTO]] = self.get_marked_for_cleanup(integration_session, part_one_rootfolder)
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