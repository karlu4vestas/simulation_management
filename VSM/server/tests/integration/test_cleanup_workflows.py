from dataclasses import dataclass
from datetime import date
from typing import NamedTuple
import pytest
from datamodel.retention_validators import ExternalToInternalRetentionTypeConverter, RetentionCalculator
from datamodel.dtos import FolderNodeDTO, FolderTypeDTO, FolderTypeEnum, RetentionTypeDTO, RootFolderDTO, ExternalRetentionTypes
from app.web_api import normalize_path, read_folder_marked_for_cleanup, read_folders, read_retentiontypes_by_domain_id, read_folder_marked_for_cleanup, read_rootfolder_retentiontypes_dict, read_cleanupfrequency_by_domain_id, read_cycle_time_by_domain_id, start_new_cleanup_cycle 
from app.web_api import read_retentiontypes_by_domain_id, read_folder_type_dict_pr_domain_id, read_simulation_domains, read_folder_types_pr_domain_id 
from db.db_api import insert_rootfolder,insert_cleanup_configuration
from .base_integration_test import BaseIntegrationTest, RootFolderWithFolderNodeDTOList
from .testdata_for_import import InMemoryFolderNode, RootFolderWithMemoryFolders,CleanupConfiguration
from datamodel.dtos import CleanupConfigurationDTO

# DataIOSet structure is setup in initialization_with_import_of_simulations_and_test_of_db_folder_hierarchy for reuse in other tests
@dataclass
class DataIOSet:
    key:    str  # input to retrieve the scenario data from cleanup_scenario_data in conftest.py
    input:  RootFolderWithMemoryFolders #input rootfolder
    output: RootFolderWithFolderNodeDTOList #output rootfolder and folders from db after insertion of simulations
    input_leafs: list[InMemoryFolderNode] = None # input leafs (the simulations) extracted from input 
    output_for_input_leafs: list[FolderNodeDTO] = None # output leafs (the simulations) extracted from output and ordered as input leafs
    retention_calculator: RetentionCalculator = None # retention calculator for the rootfolder  
    externalToInternalRetentionTypeConverter: ExternalToInternalRetentionTypeConverter = None # converter for external to internal retention types for the rootfolder   
    path_retention: RetentionTypeDTO = None # the path retention for the rootfolder
    marked_retention: RetentionTypeDTO = None # the marked retention for the rootfolder
    nodetype_leaf: int = None # the folder type id for leaf nodes (simulations)
    nodetype_inner: int = None # the folder type id for inner nodes

@pytest.mark.integration
@pytest.mark.cleanup_workflow
@pytest.mark.slow
class TestCleanupWorkflows(BaseIntegrationTest):

    # initialization_with_import_of_simulations_and_test_of_db_folder_hierarchy is in it self a comprehensive test focused on testing
    # whether the simulations are properly imported and the folder structure in the db is built correctly. It does however not test the simulation attributes
    # Other tests can do that by starting to call initialization_with_import_of_simulations_and_test_of_db_folder_hierarchy and then use list[DataIOSet] for further testing
    def import_simulations_and_test_db_folder_hierarchy(self, integration_session, keys_to_run_in_order:list[str], cleanup_scenario_data) -> list[DataIOSet]:
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

        in_memory_config:CleanupConfiguration = cleanup_scenario_data.get("cleanup_configuration", None)
        assert in_memory_config is not None, "cleanup_configuration is missing in cleanup_scenario_data fixture in conftest.py"
        dataio_sets: list[DataIOSet] = []
        for key in keys_to_run_in_order:
            #insert add the simulation domain to the rootfolder and inser the rootfolder in the db
            input: RootFolderWithMemoryFolders = cleanup_scenario_data[key]
            # set the simulation domain, save the rootfolder to the db and then create first_rootfolder_tuple with the rootfolder from the db
            input.rootfolder.simulationdomain_id = simulation_domain_id
            input.rootfolder = insert_rootfolder(input.rootfolder)
            #verify that the rootfolder was saved correctly
            assert input.rootfolder is not None
            assert input.rootfolder.id is not None and input.rootfolder.id > 0
            assert input.rootfolder.path == input.rootfolder.path
            
            # Create CleanupConfigurationDTO from the in-memory CleanupConfiguration
            # The cleanup_scenario_data fixture uses the old CleanupConfiguration dataclass for in-memory setup
            # Now we create the corresponding CleanupConfigurationDTO database record
            cleanup_config_dto = in_memory_config.to_dto(rootfolder_id=input.rootfolder.id)
            cleanup_config_dto = insert_cleanup_configuration(input.rootfolder.id, cleanup_config_dto)
            
            # Insert simulations (the leaves). The return will be all folders in the db
            output: RootFolderWithFolderNodeDTOList = self.insert_simulations(integration_session, input)
            str_summary = f"key:{key}, input rootfolder ids: {input.rootfolder.id}, output rootfolder ids: {output.rootfolder.id}"
            print(str_summary)

            data_set:DataIOSet = DataIOSet(key=key,input=input,output=output)
            data_set.nodetype_leaf        = read_folder_type_dict_pr_domain_id(data_set.output.rootfolder.simulationdomain_id)[FolderTypeEnum.VTS_SIMULATION].id
            data_set.nodetype_inner       = read_folder_type_dict_pr_domain_id(data_set.output.rootfolder.simulationdomain_id)[FolderTypeEnum.INNERNODE].id
            # Use the CleanupConfigurationDTO we just created for the RetentionCalculator
            if cleanup_config_dto:
                data_set.retention_calculator = RetentionCalculator(read_rootfolder_retentiontypes_dict(data_set.output.rootfolder.id), cleanup_config_dto)
            data_set.path_retention       = data_set.retention_calculator.retention_type_str_dict["path"]
            data_set.marked_retention     = data_set.retention_calculator.retention_type_str_dict["marked"]
            data_set.externalToInternalRetentionTypeConverter = ExternalToInternalRetentionTypeConverter(read_rootfolder_retentiontypes_dict(data_set.output.rootfolder.id))

            dataio_sets.append(data_set)


        # Step 2 validate the output data
        leaf_node_type:int = read_folder_type_dict_pr_domain_id(simulation_domain_id)[FolderTypeEnum.VTS_SIMULATION].id
        for data_set in dataio_sets:
            assert data_set.input.rootfolder.id == data_set.output.rootfolder.id  # should never fail because we just got back what we input to insert_simulations

            # extract the the list of leaf in input and output so that it is easy to validate them. Leafs are the vts simulations
            input_leafs  = sorted( [ folder for folder in data_set.input.folders  if folder.is_leaf ], key=lambda f: normalize_path(f.path) )
            output_leafs = sorted( [ folder for folder in data_set.output.folders if folder.nodetype_id == leaf_node_type ], key=lambda f: normalize_path(f.path) )
            data_set.input_leafs = input_leafs

            #select and order output simulations so that they match the order of input
            output_leaf_path_dict:dict[str,InMemoryFolderNode] = {normalize_path(f.path): f for f in output_leafs}
            data_set.output_for_input_leafs = [output_leaf_path_dict[ normalize_path(p.path) ] for p in data_set.input_leafs]


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

                    # verify that all output folders are of the folder type. 
                    #   if input folder.is_leaf=True then the output folder must have a nodetype_id of VTS_SIMULATION
                    #   otherwise the nodetype must be INNERNODE
                    # Create a mapping of paths
                    output_folder_lookup: dict[str, FolderNodeDTO] = {normalize_path(folder.path): folder for folder in data_set.output.folders}    
                    for folder in data_set.input.folders: 
                        db_folder:FolderNodeDTO = output_folder_lookup.get(normalize_path(folder.path))
                        
                        assert db_folder is not None, f"data_set.key={data_set.key}: Folder {folder.path} not found in database folders"
                        if folder.is_leaf:
                            assert db_folder.nodetype_id == data_set.nodetype_leaf, \
                                f"data_set.key={data_set.key}: Folder {folder.path} is a leaf in input but has nodetype_id {folder.nodetype_id} instead of {data_set.nodetype_leaf}"
                        else:             
                            assert db_folder.nodetype_id == data_set.nodetype_inner, \
                                f"data_set.key={data_set.key}: Folder {folder.path} is an inner node in input but has nodetype_id {folder.nodetype_id} instead of {data_set.nodetype_inner}"
                
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

                    # Verify each input leaf has a corresponding output folder with the correct node type
                    # Create a mapping of paths to is_leaf status from ALL input folders (not just leafs)
                    output_folder_lookup: dict[str, FolderNodeDTO] = {normalize_path(folder.path): folder for folder in data_set.output.folders}
                    for folder in data_set.input_leafs:
                        db_folder:FolderNodeDTO = output_folder_lookup.get(normalize_path(folder.path))                        
                        assert db_folder is not None, f"data_set.key={data_set.key}: Folder {folder.path} not found in database folders"
                        assert db_folder.nodetype_id == data_set.nodetype_leaf, \
                                f"data_set.key={data_set.key}: Folder {db_folder.path} is a leaf in input but has nodetype_id {db_folder.nodetype_id} instead of {data_set.nodetype_leaf}"

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

                    
                    # Verify each input leaf has a corresponding output folder with the correct node type
                    # Create a mapping of paths to is_leaf status from ALL input folders (not just leafs)
                    output_folder_lookup: dict[str, FolderNodeDTO] = {normalize_path(folder.path): folder for folder in data_set.output.folders}
                    for folder in data_set.input_leafs:
                        db_folder:FolderNodeDTO = output_folder_lookup.get(normalize_path(folder.path))
                        assert db_folder is not None, f"data_set.key={data_set.key}: Folder {folder.path} not found in database folders"
                        assert db_folder.nodetype_id == data_set.nodetype_leaf, \
                                f"data_set.key={data_set.key}: Folder {db_folder.path} is a leaf in input but has nodetype_id {db_folder.nodetype_id} instead of {data_set.nodetype_leaf}"
                case _:
                    raise ValueError(f"Unknown scenario data key: {data_set.key}")
                


        return dataio_sets


    def import_simulations_and_test_retentions(self, integration_session, keys_to_run_in_order:list[str], cleanup_scenario_data) -> list[DataIOSet]:
        #initialize the db and then verify attributes of the inserted simulations
        data_io_sets:list[DataIOSet] = self.import_simulations_and_test_db_folder_hierarchy(integration_session, keys_to_run_in_order, cleanup_scenario_data)
        #test the attributes of the inserted simulations
        for data_set in data_io_sets:

            # the testdata defines a cleanup configuration that can be started 
            #assert data_set.output.rootfolder.get_cleanup_configuration().can_start_cleanup()

            for sim_folder, folder in zip(data_set.input_leafs, data_set.output_for_input_leafs):
                # verify that modified_date was set correctly
                assert folder.modified_date == sim_folder.modified_date

                # verify that the folder type is correct
                assert folder.nodetype_id == data_set.nodetype_leaf

                # verify that the retention is correct
                # Notice that there is no active cleanup round and the simulation have just been inserted 
                # Therefore, external retentions should just be converted to an internal retention and assigned to the db_folder unless
                #   - the db folder is under a path retention
                #   - the user have previously define a retention. This is not the case now because we just imported the simulations
                if sim_folder.retention is not None:
                    if folder.retention_id == data_set.path_retention.id:
                        assert folder.pathprotection_id is not None
                    else:    
                        assert folder.pathprotection_id is None

                        # check that we have adapted the external retention to an internal retention
                        # in case of sim_folder.retention is Unknown the retention should be numeric
                        sim_retention_type:RetentionTypeDTO = data_set.externalToInternalRetentionTypeConverter.to_internal(sim_folder.retention)
                        if sim_retention_type is None:
                            assert data_set.retention_calculator.is_numeric(folder.retention_id)
                        else:
                            assert folder.retention_id == sim_retention_type.id

            for folder in data_set.output.folders:
                if folder.nodetype_id == data_set.nodetype_leaf:
                    #assert that the retention_id is not marked
                    assert folder.retention_id is not None and folder.retention_id != data_set.marked_retention.id, \
                            f"data_set.key={data_set.key}: Folder {folder.path} has marked retention which is not allowed outside an active cleanup round"
                else: # the retenton must be None for inner nodes
                    assert folder.retention_id is None, f"data_set.key={data_set.key}: Folder {folder.path} is an inner node and must have retention_id=None but has retention_id={folder.retention_id}"

        return data_io_sets

    def test_simulation_import_and_its_retention_settings(self, integration_session, cleanup_scenario_data):
        keys_to_run_in_order = ["first_rootfolder", "second_rootfolder_part_one", "second_rootfolder_part_two"]
        self.import_simulations_and_test_retentions(integration_session, keys_to_run_in_order, cleanup_scenario_data)

    def import_and_start_cleanup_round_with_test_of_retentions(self, integration_session, cleanup_scenario_data) -> list[DataIOSet]:
        #initialize the db and then verify attributes of the inserted simulations
        #   step 1 call the "import_simulations_and_test_db_folder_hierarchy" with "second_rootfolder_part_one" to initialize the data
        #       step 1.1: start the cleanup round and
        #       step 1.2: verify that the all retentions are valid. This include in particular that the correct simulation are marked for cleanup


        #   step 1 call the "import_simulations_and_test_retentions" with "second_rootfolder_part_one" to initialize and validate folder hierarchy and retentions
        second_part_one_data_set:DataIOSet= self.import_simulations_and_test_retentions(integration_session, ["second_rootfolder_part_one"], cleanup_scenario_data)[0]

        path_or_endstage_retention_ids = {second_part_one_data_set.path_retention.id, *[retention.id for retention in second_part_one_data_set.retention_calculator.get_endstage_retentions()]}
        
        #prepare lookup of input folders and the folders with modified_date+cycle_time set before the cleanup_start_date that are not in path retention and must therefor be marked for cleanup
        input_leafs_lookup: dict[str, InMemoryFolderNode] = {normalize_path(folder.path): folder for folder in second_part_one_data_set.input.folders if folder.is_leaf }
        input_leafs_to_be_marked_dict = {path: folder for path,folder in input_leafs_lookup.items() 
                                                    if folder.testcase_dict["folder_retention_case"] == InMemoryFolderNode.TestCaseEnum.BEFORE and \
                                                       folder.retention is ExternalRetentionTypes.Unknown }     
        second_part_one_data_set.input_leafs_to_be_marked_dict = input_leafs_to_be_marked_dict

        rootfolder:RootFolderDTO = second_part_one_data_set.output.rootfolder
        leafs_before_start: dict[str,FolderNodeDTO] = {normalize_path(folder.path): folder for folder in read_folders(rootfolder.id) if folder.nodetype_id == second_part_one_data_set.nodetype_leaf}

        # step 1.1: start the cleanup round and
        start_new_cleanup_cycle(rootfolder.id )
        output_folders = read_folders(rootfolder.id)
        second_part_one_data_set.output = RootFolderWithFolderNodeDTOList(rootfolder=rootfolder, folders=output_folders)  # refresh the folders in output to reflect any changes made by starting the cleanup round

        leafs_after_start: dict[str,FolderNodeDTO] = {normalize_path(folder.path): folder for folder in output_folders if folder.nodetype_id == second_part_one_data_set.nodetype_leaf}

        # verify that leafs planned to be in path_or_endstage_retention_ids have the correct retention both before and after the start of the cleanup round
        for path_after_start, leaf_after_start in leafs_after_start.items():
            input_folder: InMemoryFolderNode = input_leafs_lookup.get(path_after_start, None)
            leaf_before_start:FolderNodeDTO  = leafs_before_start.get(path_after_start, None)
            assert input_folder is not None, f"unable to lookup input_folder for {path_after_start}"
            assert leaf_before_start is not None, f"unable to lookup leaf_before_start for {path_after_start}"

            if input_folder.retention is not ExternalRetentionTypes.Unknown:

                assert leaf_before_start.retention_id in path_or_endstage_retention_ids, \
                    f"The Folder {leaf_before_start.path} should have been in path_or_endstage_retention_ids but is {leaf_before_start.retention_id}"
                assert leaf_after_start.retention_id  in path_or_endstage_retention_ids, \
                    f"The Folder {leaf_after_start.path} should have been in path_or_endstage_retention_ids but is {leaf_after_start.retention_id}"


        # step 1.2: verify that the all retentions are valid. 
        # There is a redundant part there where retention in path_or_endstage_retention_ids are verified again as above
        for path_after_start, leaf_after_start in leafs_after_start.items():
            to_be_marked_input_folder: InMemoryFolderNode = input_leafs_to_be_marked_dict.get(path_after_start, None)
            if to_be_marked_input_folder is not None: # did we plan for this testdata to be marked by start of cleanup round
                assert leaf_after_start.retention_id == second_part_one_data_set.marked_retention.id, \
                    f"Folder {leaf_after_start.path} should have been 'marked' but is {leaf_after_start.retention_id}"
            else: # we didnot plan the testdata to be marked for cleanup. Verify that it is not marked nad is either numeric or in path or endstage
                input_folder: InMemoryFolderNode = input_leafs_lookup.get(path_after_start, None)
                assert input_folder is not None, f"unable to lookup input_folder for {path_after_start}"

                #verify that the leaf is not marked
                assert leaf_after_start.retention_id != second_part_one_data_set.marked_retention.id, \
                    f"Folder {leaf_after_start.path} should NOT have been 'marked' but is {leaf_after_start.retention_id}"
               
                #verify that the leaf is in endstage or numeric depending on the input testdata
                if input_folder.retention != ExternalRetentionTypes.Unknown:
                    assert leaf_after_start.retention_id in path_or_endstage_retention_ids, \
                        f"Folder {leaf_after_start.path} the leafs retention should have been in path or endstage but is  {leaf_after_start.retention_id}"
                    
                else:
                    assert second_part_one_data_set.retention_calculator.is_numeric(leaf_after_start.retention_id), \
                        f"Folder {leaf_after_start.path} the leafs retention should have been numeric but is {leaf_after_start.retention_id}" 

        return [second_part_one_data_set]
    
    def test_import_and_start_cleanup_round_with_test_of_retentions(self, integration_session, cleanup_scenario_data):
        self.import_and_start_cleanup_round_with_test_of_retentions(integration_session, cleanup_scenario_data)


    def test_retentions_of_insertions_after_start_of_cleanup_round(self, integration_session, cleanup_scenario_data):
        #this will import the second rootfolders first part and start a cleanup round
        second_part_one_data_set:DataIOSet = self.import_and_start_cleanup_round_with_test_of_retentions(integration_session, cleanup_scenario_data)[0]

        # the following is all original import and does not change
        path_or_endstage_retention_ids = {second_part_one_data_set.path_retention.id, *[retention.id for retention in second_part_one_data_set.retention_calculator.get_endstage_retentions()]}
        #prepare lookup of input folders and the folders with modified_date+cycle_time set before the cleanup_start_date that are not in path retention and must therefor be marked for cleanup
        second_part_one_output_folders_lookup: dict[str, InMemoryFolderNode] = {normalize_path(folder.path): folder for folder in second_part_one_data_set.output.folders }
        input_leafs_to_be_marked_dict: dict[str, InMemoryFolderNode] = second_part_one_data_set.input_leafs_to_be_marked_dict
        assert input_leafs_to_be_marked_dict is not None, "input_leafs_to_be_marked_dict is None"

        # step 2: insert more folders into the same rootfolder by calling "import_simulations_and_test_db_folder_hierarchy" with "second_rootfolder_part_two"
        second_part_two_input: RootFolderWithMemoryFolders = cleanup_scenario_data["second_rootfolder_part_two"]
        second_part_one_input_leafs_lookup: dict[str, InMemoryFolderNode] = {normalize_path(folder.path): folder for folder in second_part_two_input.folders if folder.is_leaf }
        second_part_two_output: RootFolderWithFolderNodeDTOList = self.insert_simulations(integration_session, second_part_two_input)

        # this part contains ALL the rootfolders folders
        second_part_folders_after_insertion_lookup: dict[str,FolderNodeDTO] = {normalize_path(folder.path): folder for folder in second_part_two_output.folders}
        
        #verify that the inserttion did not change the retentions of the same rootfolders part_one folders
        for part_one_path,part_one_folder in second_part_one_output_folders_lookup.items():
            db_folder:FolderNodeDTO = second_part_folders_after_insertion_lookup.get(part_one_path, None)
            assert db_folder is not None, f"unable to lookup db_folder for {part_one_path}"

            assert part_one_folder.retention_id == db_folder.retention_id , \
                f"Folder {part_one_path} retention changed after insertion of new folders. Was {part_one_folder.retention_id}, now {db_folder.retention_id}"


        #rootfolder:RootFolderDTO = second_part_two_data_set.output.rootfolder
        #output_folders_before: list[FolderNodeDTO] = read_folders(rootfolder.id)
        #leafs_before_insertion: dict[str,FolderNodeDTO] = {normalize_path(folder.path): folder for folder in output_folders_before if folder.nodetype_id == second_part_two_data_set.nodetype_leaf}

        # step 1.1: start the cleanup round and
        #start_new_cleanup_cycle(rootfolder.id )
        #leafs_after_start: dict[str,FolderNodeDTO] = {normalize_path(folder.path): folder for folder in read_folders(rootfolder.id) if folder.nodetype_id == second_part_two_data_set.nodetype_leaf}

        for second_part_one_leaf_path, second_part_two_input_leaf_folder in second_part_one_input_leafs_lookup.items():
            second_part_two_leaf:FolderNodeDTO = second_part_folders_after_insertion_lookup.get(second_part_one_leaf_path, None)
            assert second_part_two_leaf is not None, f"unable to lookup second_part_two_leaf_output_folder for {second_part_one_leaf_path}"       
            assert second_part_two_leaf.retention_id != second_part_one_data_set.marked_retention, \
                f"Folder {second_part_one_leaf_path} retention got marked for cleanup"


        #       step 2.2: extract folders marked for cleanup and verify that they are the same as after step 1.1. Convert the dataset to RootFolderWithMemoryFolders
        marked_folders: list[FolderNodeDTO] = read_folder_marked_for_cleanup(second_part_two_output.rootfolder.id)
        assert len(marked_folders) == len(input_leafs_to_be_marked_dict), \
            f"Number of folders marked for cleanup {len(marked_folders)} is different from expected {len(input_leafs_to_be_marked_dict)}"
        for folder in marked_folders:
            input_folder: InMemoryFolderNode = input_leafs_to_be_marked_dict.get(normalize_path(folder.path), None)
            assert input_folder is not None, f"Folder {folder.path} was not expected to be marked for cleanup"


    def test_transitions_from_INACTIVE_to_RETENTION_REVIEW_to_CLEANING_to_FINISHED(self, integration_session, cleanup_scenario_data):
        
        self.import_and_start_cleanup_round_with_test_of_retentions(integration_session, cleanup_scenario_data)

        #   step 3: finalize the cleanup round so we are ready for the next cleanup round
        #       step 3.1: simulate clean up by setting the retention of the extract simulation from step 2.2 to "Clean" or "Issue" and insert them again
        #       step 3.2: verify that the simulations from step 2.2 are no longer marked for cleanup

        pass

