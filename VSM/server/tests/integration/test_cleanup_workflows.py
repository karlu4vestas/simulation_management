from dataclasses import dataclass
from datetime import date
from typing import NamedTuple
import pytest
from datamodel.retention_validators import RetentionCalculator
from datamodel.dtos import CleanupConfiguration, FolderNodeDTO, FolderTypeDTO, FolderTypeEnum, RetentionTypeDTO, RootFolderDTO
from app.web_api import normalize_path, read_retentiontypes_by_domain_id, read_rootfolder_retention_type_dict, read_cleanupfrequency_by_domain_id, read_cycle_time_by_domain_id 
from app.web_api import read_retentiontypes_by_domain_id, read_folder_type_dict_pr_domain_id, read_simulation_domains, read_folder_types_pr_domain_id 
from db.db_api import insert_rootfolder
from .base_integration_test import BaseIntegrationTest, RootFolderWithFolderNodeDTOList
from .testdata_for_import import InMemoryFolderNode, RootFolderWithMemoryFolders

"""Integration tests for complete cleanup workflows"""
@dataclass
class DataIOSet:
    key:    str  # input to retrieve the scenario data from cleanup_scenario_data in conftest.py
    input:  RootFolderWithMemoryFolders #input rootfolder
    output: RootFolderWithFolderNodeDTOList #output rootfolder and folders from db after insertion of simulations
    input_leafs: list[InMemoryFolderNode] = None # input leafs (the simulations) extracted from input 
    output_for_input_leafs: list[FolderNodeDTO] = None # output leafs (the simulations) extracted from output and ordered as input leafs
    retention_calculator: RetentionCalculator = None # retention calculator for the rootfolder  
    path_retention: RetentionTypeDTO = None # the path retention for the rootfolder
    leaf_node_type: int = None # the folder type id for leaf nodes (simulations)


@pytest.mark.integration
@pytest.mark.cleanup_workflow
@pytest.mark.slow
class TestCleanupWorkflows(BaseIntegrationTest):

    def initialization_with_import_of_simulations_and_test_of_db_folder_hierarchy(self, integration_session, cleanup_scenario_data) -> list[DataIOSet]:
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

            # extract the the list of leaf in input and output so that it is easy to validate them. Leafs are the vts simulations
            input_leafs  = sorted( [ folder for folder in data_set.input.folders  if folder.is_leaf ], key=lambda f: normalize_path(f.path) )
            output_leafs = sorted( [ folder for folder in data_set.output.folders if folder.nodetype_id == leaf_node_type ], key=lambda f: normalize_path(f.path) )
            

            match data_set.key:
                case "first_rootfolder":
                    #part one: consist of the same rootfolder that is not shared with other scenarios

                    #check that the all leaves (the simulations) were inserted
                    assert len(input_leafs) == len(output_leafs)
                    input_leaf_set  = set([normalize_path(f.path).lower() for f in input_leafs])
                    output_leaf_set = set([normalize_path(f.path).lower() for f in output_leafs])
                    diff = input_leaf_set.difference( output_leaf_set )
                    assert len(diff) == 0


                    # even if "insert_simulations" only insert the leaves the whole foldertree must have be generated from the leaves in the db
                    assert len(data_set.input.folders) == len(data_set.output.folders)

                    # Check that all the paths are the same. Sort both lists by path because they were inserted in random order
                    input_path_set  = set([normalize_path(f.path).lower() for f in data_set.input.folders])
                    output_path_set = set([normalize_path(f.path).lower() for f in data_set.output.folders])
                    diff = input_path_set.difference( output_path_set )
                    assert len(diff) == 0
                
                case "second_rootfolder_part_one":
                    # "second_rootfolder_part_one" is executed before "second_rootfolder_part_two"

                    # At this point only part_one of the second_rootfolder has been inserted. 
                    # As a consequence, the folders in input and output may be different
                    # Output is generated from inserting leaf folders found in the input. 
                    #   - There may be folders in the input that are not part of the path to any leaf
                    #   - There may be folder in the output that are not in the input because they are part of the path to a leaf


                    # Likewise the number of input and output leafs must be the same because the insertions in case "second_rootfolder_part_one"
                    # is the first for the rootfolder
                    assert len(input_leafs) == len(output_leafs)
                    input_leaf_set  = set([normalize_path(f.path).lower() for f in input_leafs])
                    output_leaf_set = set([normalize_path(f.path).lower() for f in output_leafs])
                    diff = input_leaf_set.difference( output_leaf_set )
                    assert len(diff) == 0

                case "second_rootfolder_part_two":
                    # At this point part_one and part_two of the second_rootfolder have been inserted.
                    # The output includes all folders in db belonging to the rootfolder.
                    
                    # The folders in the input must, therefore, be in the output

                    # the number of input leafs must be smaller or equal to the number of output leafs.
                    # Equality can happen if the split of folders between the two cases allocates all leafs to "second_rootfolder_part_two"
                    assert len(input_leafs) <= len(output_leafs)

                    # Verify all input leaves exist in output
                    input_leaf_set  = set([normalize_path(f.path).lower() for f in input_leafs])
                    output_leaf_set = set([normalize_path(f.path).lower() for f in output_leafs])
                    assert input_leaf_set.issubset( output_leaf_set )

                    # all input folder must be in output
                    assert len(data_set.input.folders) <= len(data_set.output.folders)
                    input_path_set  = set([normalize_path(f.path).lower() for f in data_set.input.folders])
                    output_path_set = set([normalize_path(f.path).lower() for f in data_set.output.folders])
                    assert input_path_set.issubset( output_path_set )
                case _:
                    raise ValueError(f"Unknown scenario data key: {data_set.key}")
                
        #test the attrubutes of the inserted simulations
        for data_set in dataio_sets:

            data_set.leaf_node_type = read_folder_type_dict_pr_domain_id(data_set.output.rootfolder.simulationdomain_id)[FolderTypeEnum.VTS_SIMULATION].id
            data_set.retention_calculator = RetentionCalculator(read_rootfolder_retention_type_dict(data_set.output.rootfolder.id), data_set.output.rootfolder.get_cleanup_configuration()) 
            data_set.path_retention = data_set.retention_calculator.all_retention_types["path"]

            assert data_set.input.rootfolder.id == data_set.output.rootfolder.id  # should never fail because we just got back what we input to insert_simulations

            # extract the the list of leaf in input and output so that it is easy to validate them. Leafs are the vts simulations
            data_set.input_leafs  = sorted( [ folder for folder in data_set.input.folders  if folder.is_leaf ], key=lambda f: normalize_path(f.path) )
            output_leafs = sorted( [ folder for folder in data_set.output.folders if folder.nodetype_id == data_set.leaf_node_type ], key=lambda f: normalize_path(f.path) )

            #select and order output simulations so that they match the order of input
            output_leaf_path_dict:dict[str,InMemoryFolderNode] = {normalize_path(f.path): f for f in output_leafs}
            data_set.output_for_input_leafs = [output_leaf_path_dict[ normalize_path(p.path) ] for p in data_set.input_leafs]


        return dataio_sets

    def test_initialization_with_import_of_simulations(self, integration_session, cleanup_scenario_data):
        #initialize the db and then verify attributes of the inserted simulations
        data_io_sets:list[DataIOSet] = self.initialization_with_import_of_simulations_and_test_of_db_folder_hierarchy(integration_session, cleanup_scenario_data)

        #test the attrubutes of the inserted simulations
        for data_set in data_io_sets:

            # the testdata defines a cleanup configuration 
            assert data_set.output.rootfolder.get_cleanup_configuration().can_start_cleanup()

            for sim_folder, db_folder in zip(data_set.input_leafs, data_set.output_for_input_leafs):
                assert db_folder.modified_date == sim_folder.modified_date
                #assert retention_calculator.is_valid( db_folder.get_retention() ) #retention must be valid from the moment that the cleanup cycle starts

                #ensure that the external retention was applied unless it is path retention
                if sim_folder.retention is not None :
                    sim_folder_retention:RetentionTypeDTO = data_set.retention_calculator.all_retention_types[sim_folder.retention]
                    assert db_folder.retention_id == data_set.path_retention.id or db_folder.retention_id == sim_folder_retention.id  

    def test_start_cleanup_round_without_new_insertions(self, integration_session, cleanup_scenario_data):
        #initialize the db and then verify attributes of the inserted simulations
        data_io_sets:list[DataIOSet] = self.initialization_with_import_of_simulations_and_test_of_db_folder_hierarchy(integration_session, cleanup_scenario_data)

        #test the attrubutes of the inserted simulations
        for data_set in data_io_sets:
            rootfolder:RootFolderDTO = data_set.output.rootfolder
            assert rootfolder.get_cleanup_configuration().can_start_cleanup()

            #step 1: verify that noting is marked for cleanup before starting the cleanup round

            #step 2: start cleanup round

            #step 3: verify that the all retentions are valid

            #step 4: verify that the correct simulations are marked for cleanup

            #step 5: finalize cleanup round by cleaning marked simulations
            #step 6: Launch scan for new simulations
            #step 6: Launch scan for new simulations
    def test_insert_simulations_into_active_cleanup_round(self, integration_session, cleanup_scenario_data):
        #pass_insert_simulations_into_active_cleanup_round(self, integration_session, cleanup_scenario_data):
        pass
    def test_stop_and_resumption_of_cleanup_round(self, integration_session, cleanup_scenario_data):
        #ass_stop_and_resumption_of_cleanup_round(self, integration_session, cleanup_scenario_data):
        pass
