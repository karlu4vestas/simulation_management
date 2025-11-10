from dataclasses import dataclass
from typing import NamedTuple
from datetime import timedelta
from sqlmodel import Session
import pytest

from datamodel.retentions import Extern2InternRetentionTypeConverter, RetentionCalculator, FolderRetention, RetentionTypeEnum
from datamodel.dtos import FolderNodeDTO, FolderTypeEnum, Retention, RetentionTypeDTO, RootFolderDTO, ExternalRetentionTypes
from datamodel.vts_create_meta_data import insert_vts_metadata_in_db

from db.db_api import FileInfo
from db.db_api import insert_or_update_simulations_in_db, read_folders, normalize_path, read_rootfolders_by_domain_and_initials
from db.db_api import change_retentions, insert_or_update_simulations_in_db, normalize_path, read_folders_marked_for_cleanup, read_folders, read_retentiontypes_by_domain_id, read_folders_marked_for_cleanup, read_rootfolder_retentiontypes_dict
from db.db_api import read_retentiontypes_by_domain_id, read_folder_type_dict_pr_domain_id, read_simulation_domains, read_folder_types_pr_domain_id, read_cleanupfrequency_by_domain_id, read_cycle_time_by_domain_id   
from db.db_api import insert_rootfolder,insert_cleanup_configuration, read_simulation_domain_by_name

from cleanup_cycle.cleanup_db_actions import cleanup_cycle_start, CleanupProgress
from cleanup_cycle.cleanup_dtos import CleanupConfigurationDTO

from tests.integration.testdata_for_import import InMemoryFolderNode, RootFolderWithMemoryFolders,CleanupConfiguration

class RootFolderWithFolderNodeDTOList(NamedTuple):
    """Named tuple for a root folder and its flattened list of folder nodes"""
    rootfolder: RootFolderDTO
    folders: list[FolderNodeDTO]

# DataIOSet structure is setup in initialization_with_import_of_simulations_and_test_of_db_folder_hierarchy for reuse in other tests
@dataclass
class DataIOSet:
    key:    str  # input to retrieve the scenario data from cleanup_scenario_data in conftest.py
    input:  RootFolderWithMemoryFolders #input rootfolder
    output: RootFolderWithFolderNodeDTOList #output rootfolder and folders from db after insertion of simulations
    input_leafs: list[InMemoryFolderNode] = None # input leafs (the simulations) extracted from input 
    input_leafs_to_be_marked_dict: dict[str, InMemoryFolderNode]=None
    output_for_input_leafs: list[FolderNodeDTO] = None # output leafs (the simulations) extracted from output and ordered as input leafs
    retention_calculator: RetentionCalculator = None # retention calculator for the rootfolder  
    path_retention: RetentionTypeDTO = None # the path retention for the rootfolder
    marked_retention: RetentionTypeDTO = None # the marked retention for the rootfolder
    undefined_retention: RetentionTypeDTO = None # the undefined retention for the rootfolder
    nodetype_leaf: int = None # the folder type id for leaf nodes (simulations)
    nodetype_inner: int = None # the folder type id for inner nodes




from tests import test_storage
TEST_STORAGE_LOCATION = test_storage.LOCATION

@pytest.mark.integration
@pytest.mark.cleanup_workflow
@pytest.mark.slow
class TestCleanupWorkflows:
    def setup_new_db_with_vts_metadata(self, session: Session) -> None:
        insert_vts_metadata_in_db(session)
        # Step 0: Set up a new database and verify that it is empty apart from VTS metadata
        simulation_domains: list = read_simulation_domains()
        assert len(simulation_domains) > 0
        simulation_domain_id = simulation_domains[0].id
        assert simulation_domain_id is not None and simulation_domain_id > 0

        #verify that the domain has the necessary configuration data
        assert len(read_retentiontypes_by_domain_id(simulation_domain_id)) > 0
        assert len(read_folder_types_pr_domain_id(simulation_domain_id)) > 0
        assert len(read_cleanupfrequency_by_domain_id(simulation_domain_id)) > 0
        assert len(read_cycle_time_by_domain_id(simulation_domain_id)) > 0


    def insert_simulations(self, session: Session, rootfolder_with_folders: RootFolderWithMemoryFolders) -> RootFolderWithFolderNodeDTOList:
        # Insert simulations into database and return all the rootfolder database folders for validation 
        rootfolder:RootFolderDTO=rootfolder_with_folders.rootfolder
        assert rootfolder.id is not None and rootfolder.id > 0

        # extract and convert the leaves to FileInfo (the leaves  are the simulations) 
        file_info_list:list[FileInfo] = [ FileInfo( filepath=folder.path, modified_date=folder.modified_date, nodetype=FolderTypeEnum.SIMULATION, external_retention=folder.retention) 
                                            for folder in rootfolder_with_folders.folders if folder.is_leaf ] 
        insert_or_update_simulations_in_db(rootfolder.id, file_info_list)

        # get all rootfolders and folders in the db for validation
        rootfolders: list[RootFolderDTO] = read_rootfolders_by_domain_and_initials(rootfolder.simulationdomain_id)
        rootfolders = [r for r in rootfolders if r.id == rootfolder.id] 
        assert len(rootfolders) == 1

        folders: list[FolderNodeDTO] = read_folders(rootfolder.id)
        return RootFolderWithFolderNodeDTOList(rootfolder=rootfolders[0], folders=folders)

    # initialization_with_import_of_simulations_and_test_of_db_folder_hierarchy is in it self a comprehensive test focused on testing
    # whether the simulations are properly imported and the folder structure in the db is built correctly. It does however not test the simulation attributes
    # Other tests can do that by starting to call initialization_with_import_of_simulations_and_test_of_db_folder_hierarchy and then use list[DataIOSet] for further testing
    def import_simulations_and_test_db_folder_hierarchy(self, integration_session, keys_to_run_in_order:list[str], cleanup_scenario_data) -> list[DataIOSet]:
        simulation_domain_id:int = read_simulation_domain_by_name(domain_name="vts").id
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
            #str_summary = f"key:{key}, input rootfolder ids: {input.rootfolder.id}, output rootfolder ids: {output.rootfolder.id}"
            #print(str_summary)

            data_set:DataIOSet = DataIOSet(key=key,input=input,output=output)
            data_set.nodetype_leaf        = read_folder_type_dict_pr_domain_id(data_set.output.rootfolder.simulationdomain_id)[FolderTypeEnum.SIMULATION].id
            data_set.nodetype_inner       = read_folder_type_dict_pr_domain_id(data_set.output.rootfolder.simulationdomain_id)[FolderTypeEnum.INNERNODE].id
            # Use the CleanupConfigurationDTO we just created for the RetentionCalculator
            if cleanup_config_dto:
                data_set.retention_calculator = RetentionCalculator(read_rootfolder_retentiontypes_dict(data_set.output.rootfolder.id), cleanup_config_dto)
            data_set.path_retention        = data_set.retention_calculator.retention_type_str_dict["path"]
            data_set.marked_retention      = data_set.retention_calculator.retention_type_str_dict["marked"]
            data_set.undefined_retention   = data_set.retention_calculator.retention_type_str_dict["?"]
            data_set.externalToInternalRetentionConverter = Extern2InternRetentionTypeConverter(read_rootfolder_retentiontypes_dict(data_set.output.rootfolder.id))

            dataio_sets.append(data_set)


        # Step 2 validate the output data
        leaf_node_type:int = read_folder_type_dict_pr_domain_id(simulation_domain_id)[FolderTypeEnum.SIMULATION].id
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

    # import simulation using self.import_simulations_and_test_db_folder_hierarchy
    # Then test that the retentions of the inserted simulations are correct
    def import_simulations_and_test_retentions(self, integration_session, keys_to_run_in_order:list[str], cleanup_scenario_data) -> list[DataIOSet]:
        #initialize the db and then verify attributes of the inserted simulations
        data_io_sets:list[DataIOSet] = self.import_simulations_and_test_db_folder_hierarchy(integration_session, keys_to_run_in_order, cleanup_scenario_data)
        #test the attributes of the inserted simulations
        for data_set in data_io_sets:

            for sim_folder, folder in zip(data_set.input_leafs, data_set.output_for_input_leafs):
                # verify that modified_date was set correctly
                assert folder.modified_date == sim_folder.modified_date

                # verify that the folder type is correct
                assert folder.nodetype_id == data_set.nodetype_leaf

                # verify that the retention is correct
                # Notice that there is no active cleanup round and the simulation have just been inserted 
                # Therefore, external retentions should just be converted to an internal retention and assigned to the db_folder 
                #   - unless the db folder is under a path retention
                #   - unless the user have previously define a retention. This is not the case now because we just imported the simulations
                if folder.retention_id == data_set.path_retention.id: #path protections take precedence
                    assert folder.pathprotection_id is not None
                else:
                    #so the retention must be numeric, clean, Issue or missing. verify that the pathprotection_id is None
                    assert folder.pathprotection_id is None


                    sim_retention_type_id: int | None = data_set.retention_calculator.to_internal_type_id(sim_folder.retention)

                    if sim_folder.retention == ExternalRetentionTypes.NUMERIC and not data_set.retention_calculator.is_starting_cleanup_round:
                        # NUMERIC RETENTION must be assigned undefined so it can be imported even if it the cleanup configuration is incomplete
                        assert folder.retention_id == data_set.undefined_retention.id
                    elif sim_folder.retention == ExternalRetentionTypes.NUMERIC:
                        if data_set.retention_calculator.cleanup_progress == CleanupProgress.INITIAL:
                            # if the cleanup configuration is incomplete then the external NUMERIC retention must be converted to undefined retention
                            assert folder.retention_id == data_set.undefined_retention.id,  \
                                f"data_set.key={data_set.key}, folder={folder.path}. Retention should have been numeric or undefined but is {folder.retention_id}"
                        else:
                            assert data_set.retention_calculator.is_numeric(folder.retention_id), \
                                f"data_set.key={data_set.key}, folder={folder.path}. Retention should have been numeric or undefined but is {folder.retention_id}"
                    else:
                        assert not data_set.retention_calculator.is_numeric(folder.retention_id), \
                                f"data_set.key={data_set.key}, folder={folder.path}. Retention should have been NOT numeric but is {folder.retention_id}"

                        assert folder.retention_id == sim_retention_type_id, \
                            f"data_set.key={data_set.key}, folder={folder.path}. NOT retention is not corrrect {folder.retention_id}"


            #validate that retention is none for inner nodes
            for folder in data_set.output.folders:
                if folder.nodetype_id != data_set.nodetype_leaf:
                    assert folder.retention_id is None, f"data_set.key={data_set.key}: Folder {folder.path} is an inner node and must have retention_id=None but has retention_id={folder.retention_id}"

        return data_io_sets

    # import simulation using self.import_simulations_and_test_retentions
    # Then start a cleanup round and verify that retentions are updated correctly due to the start of the cleanup round  
    def import_and_start_cleanup_round_with_test_of_retentions(self, integration_session, cleanup_scenario_data, data_keys:list[str]=["second_rootfolder_part_one"]) -> list[DataIOSet]:
        #initialize the db and then verify attributes of the inserted simulations
        #   step 1 call the "import_simulations_and_test_db_folder_hierarchy" with "second_rootfolder_part_one" to initialize the data
        #       step 1.1: start the cleanup round and
        #       step 1.2: verify that the all retentions are valid. This include in particular that the correct simulation are marked for cleanup


        #   step 1 call the "import_simulations_and_test_retentions" with "second_rootfolder_part_one" to initialize and validate folder hierarchy and retentions
        second_part_one_data_set:DataIOSet= self.import_simulations_and_test_retentions(integration_session, data_keys, cleanup_scenario_data)[0]

        path_or_endstage_retention_ids = {second_part_one_data_set.path_retention.id, *[retention.id for retention in second_part_one_data_set.retention_calculator.get_endstage_retentions()]}
        
        #prepare 
        # lookup of input folders 
        # the list of folders be marked for cleanup because the "modified_date+cycle_time" is before the "cleanup_start_date" and the folder is not in path retention (there is no path protections yet)
        input_leafs_lookup: dict[str, InMemoryFolderNode] = {normalize_path(folder.path): folder for folder in second_part_one_data_set.input.folders if folder.is_leaf }
        second_part_one_data_set.input_leafs_to_be_marked_dict = {path: folder for path,folder in input_leafs_lookup.items() 
                                                                  if folder.testcase_dict["folder_retention_case"] == InMemoryFolderNode.TestCaseEnum.BEFORE and 
                                                                     folder.retention == ExternalRetentionTypes.NUMERIC }     

        rootfolder:RootFolderDTO = second_part_one_data_set.output.rootfolder
        output_leafs_before_start: dict[str,FolderNodeDTO] = {normalize_path(folder.path): folder for folder in read_folders(rootfolder.id) if folder.nodetype_id == second_part_one_data_set.nodetype_leaf}
        
        marked_folders: list[FolderNodeDTO] = read_folders_marked_for_cleanup(second_part_one_data_set.output.rootfolder.id)
        assert len(marked_folders) == 0, f"before starting cleanup round number of simulations marked for cleanup should be zero"

        # step 1.1: start the cleanup round and
        cleanup_cycle_start(rootfolder.id )
        second_part_one_data_set.output = RootFolderWithFolderNodeDTOList(rootfolder=rootfolder, folders=read_folders(rootfolder.id))  # refresh the folders in output to reflect any changes made by starting the cleanup round
        output_leafs_after_start: dict[str,FolderNodeDTO] = {normalize_path(folder.path): folder for folder in second_part_one_data_set.output.folders if folder.nodetype_id == second_part_one_data_set.nodetype_leaf}

        marked_folders: list[FolderNodeDTO] = read_folders_marked_for_cleanup(second_part_one_data_set.output.rootfolder.id)
        assert len(marked_folders) == len(second_part_one_data_set.input_leafs_to_be_marked_dict), f"after starting cleanup round number the planned and actual simulations marked for cleanup should be identical"

        # Verify 
        #  - that leafs planned to be in path_or_endstage_retention_ids have the correct retention both before and after the start of the cleanup round
        #  - that input leafs that are planned to be marked for cleanup are marked after the start of the cleanup round and that other input leafs are not marked
        for path_after_start, leaf_after_start in output_leafs_after_start.items():
            leaf_input:InMemoryFolderNode   = input_leafs_lookup.get(path_after_start, None)
            leaf_before_start:FolderNodeDTO = output_leafs_before_start.get(path_after_start, None)
            leaf_after_start:FolderNodeDTO  = output_leafs_after_start.get(path_after_start, None)
            assert leaf_input is not None, f"unable to lookup input_folder for {path_after_start}"
            assert leaf_before_start is not None, f"unable to lookup leaf_before_start for {path_after_start}"
            assert leaf_after_start is not None, f"unable to lookup leaf_after_start for {path_after_start}"

            # verify that leafs planned to be in path_or_endstage_retention_ids have the correct retention both before and after the start of the cleanup round
            if leaf_input.retention is not ExternalRetentionTypes.NUMERIC:
                assert leaf_before_start.retention_id in path_or_endstage_retention_ids, \
                    f"The Folder {leaf_before_start.path} should have been in path_or_endstage_retention_ids but is {leaf_before_start.retention_id}"
                assert leaf_after_start.retention_id  in path_or_endstage_retention_ids, \
                    f"The Folder {leaf_after_start.path} should have been in path_or_endstage_retention_ids but is {leaf_after_start.retention_id}"
            else: # the leaf_input will becom a numeric retention
                assert second_part_one_data_set.retention_calculator.is_numeric(leaf_before_start.retention_id) or leaf_before_start.retention_id == second_part_one_data_set.undefined_retention.id, \
                    f"The Folder {leaf_before_start.path} should have been numeric or undefined before start but is {leaf_before_start.retention_id}"
                assert second_part_one_data_set.retention_calculator.is_numeric(leaf_after_start.retention_id) or leaf_after_start.retention_id == second_part_one_data_set.undefined_retention.id, \
                    f"The Folder {leaf_after_start.path} should have been numeric or undefined after start but is {leaf_after_start.retention_id}"

                # verify that 
                # 1) input leafs planned to be marked for cleanup 
                #     are NOT marked BEFORE the call to cleanup_cycle_start 
                #     ARE marked after the call to cleanup_cycle_start
                # 2) that input leafs that are NOT planned to be marked for cleanup are non numeric BEFORE and AFTER the call to cleanup_cycle_start
                if path_after_start in second_part_one_data_set.input_leafs_to_be_marked_dict:
                    assert leaf_before_start.retention_id != second_part_one_data_set.marked_retention.id, \
                        f"Folder {leaf_before_start.path} should NOT have been 'marked' before start but is {leaf_before_start.retention_id}"
                    assert leaf_after_start.retention_id == second_part_one_data_set.marked_retention.id, \
                        f"Folder {leaf_after_start.path} should have been 'marked' after start but is {leaf_after_start.retention_id}"
                    
        return [second_part_one_data_set]

    def import_and_start_cleanup_round_and_import_more_simulations_with_test_of_retentions(self, integration_session, cleanup_scenario_data, data_keys:list[str]=["second_rootfolder_part_one"]) -> list[DataIOSet]:
        #this will import the second rootfolders first part and start a cleanup round
        second_part_one_data_set:DataIOSet = self.import_and_start_cleanup_round_with_test_of_retentions(integration_session, cleanup_scenario_data, data_keys)[0]

        marked_retention_id = second_part_one_data_set.marked_retention.id

        #prepare lookup of input folders and the list of folders that must be marked for cleanup
        second_part_one_output_folders_lookup: dict[str, InMemoryFolderNode] = {normalize_path(folder.path): folder for folder in second_part_one_data_set.output.folders }
        assert second_part_one_data_set.input_leafs_to_be_marked_dict is not None, "input_leafs_to_be_marked_dict is None"
        marked_folders: list[FolderNodeDTO] = read_folders_marked_for_cleanup(second_part_one_data_set.output.rootfolder.id)
        assert len(marked_folders) == len(second_part_one_data_set.input_leafs_to_be_marked_dict), f"before insertion number of input_leafs_to_be_marked_dict should be the same as in the db"

        # step 2: insert more folders into the same rootfolder by calling "import_simulations_and_test_db_folder_hierarchy" with "second_rootfolder_part_two"
        second_part_two_data_set:DataIOSet= self.import_simulations_and_test_retentions(integration_session, ["second_rootfolder_part_two"], cleanup_scenario_data)[0]
        second_part_two_input_leafs_lookup: dict[str, InMemoryFolderNode] =  {normalize_path(folder.path): folder for folder in second_part_two_data_set.input_leafs } 
        second_part_two_output: RootFolderWithFolderNodeDTOList = second_part_two_data_set.output 
        # this part contains ALL the rootfolders folders
        second_part_two_output_folders_lookup: dict[str,FolderNodeDTO] = {normalize_path(folder.path): folder for folder in second_part_two_output.folders}

        #Verification 1: verify that the insertion did not change the retention of the rootfolder' part_one folders
        for part_one_path,part_one_folder in second_part_one_output_folders_lookup.items():
            part_one_db_folder:FolderNodeDTO = second_part_two_output_folders_lookup.get(part_one_path, None)
            assert part_one_db_folder is not None, f"unable to lookup db_folder for {part_one_path}"

            assert part_one_folder.retention_id == part_one_db_folder.retention_id , \
                f"Folder {part_one_path} retention changed after insertion of new folders. Was {part_one_folder.retention_id}, now {part_one_db_folder.retention_id}"
        
        #Verification 2: verify that the insertion did not mark any other folders for cleanup
        for second_part_two_leaf_path, second_part_two_input_leaf_folder in second_part_two_input_leafs_lookup.items():
            part_two_db_folder:FolderNodeDTO = second_part_two_output_folders_lookup.get(second_part_two_leaf_path, None)
            assert part_two_db_folder is not None, f"unable to lookup second_part_two_leaf_output_folder for {second_part_two_leaf_path}"       
            assert part_two_db_folder.retention_id != marked_retention_id, f"Folder {second_part_two_leaf_path} retention got marked for cleanup"


        # Verification 3: verify that the input_leafs planned to be marked by the start of the cleanup round can be extract as marked and nore more than that
        # Furhtermore verify that folders marked for cleanup during start of cleanup is still marked for cleanup after insertion of more simulations into the same rootfolder
        marked_folders: list[FolderNodeDTO] = read_folders_marked_for_cleanup(second_part_two_output.rootfolder.id)
        assert len(marked_folders) == len(second_part_one_data_set.input_leafs_to_be_marked_dict), \
            f"Number of folders marked for cleanup {len(marked_folders)} is different from expected {len(second_part_one_data_set.input_leafs_to_be_marked_dict)}"
        for folder in marked_folders:
            input_folder: InMemoryFolderNode = second_part_one_data_set.input_leafs_to_be_marked_dict.get(normalize_path(folder.path), None)
            assert input_folder is not None, f"Folder {folder.path} was not expected to be marked for cleanup"

        return [second_part_one_data_set, second_part_two_data_set]

    def test_integrationphase_1_simulation_import_and_its_retention_settings(self, integration_session, cleanup_scenario_data):
        self.setup_new_db_with_vts_metadata(integration_session)
        keys_to_run_in_order = ["first_rootfolder", "second_rootfolder_part_one", "second_rootfolder_part_two"]
        self.import_simulations_and_test_retentions(integration_session, keys_to_run_in_order, cleanup_scenario_data)

    def test_integrationphase_2_import_and_start_cleanup_round_with_test_of_retentions(self, integration_session, cleanup_scenario_data):
        self.setup_new_db_with_vts_metadata(integration_session)
        self.import_and_start_cleanup_round_with_test_of_retentions(integration_session, cleanup_scenario_data)

    def test_integrationphase_3_retentions_of_insertions_after_start_of_cleanup_round(self, integration_session, cleanup_scenario_data):
        self.setup_new_db_with_vts_metadata(integration_session)
        data_keys:list[str] = ["second_rootfolder_part_one"]
        self.import_and_start_cleanup_round_and_import_more_simulations_with_test_of_retentions(integration_session, cleanup_scenario_data, data_keys)

    def test_integrationphase_4_retentions_of_insertions_after_start_of_cleanup_round_and_change_of_marked_retentions(self, integration_session, cleanup_scenario_data):
        self.setup_new_db_with_vts_metadata(integration_session)

        # the following is all based on the second_rootfolder from cleanup_scenario_data 
        data_keys:list[str]                     = ["second_rootfolder_part_one"]
        data_set:list[DataIOSet]                = self.import_and_start_cleanup_round_and_import_more_simulations_with_test_of_retentions(integration_session, cleanup_scenario_data, data_keys=data_keys)
        rootfolder:RootFolderDTO                = data_set[-1].output.rootfolder  # second part two data set
        marked_folders:list[FolderNodeDTO]      = read_folders_marked_for_cleanup(rootfolder.id)
        cleanup_config:CleanupConfigurationDTO  = rootfolder.get_cleanup_configuration(integration_session)
        assert cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.RETENTION_REVIEW, \
            f"cleanup_config.cleanup_progress should be RETENTION_REVIEW but is {cleanup_config.cleanup_progress}"

        # emulate a change of one retention from marked to Next+         
        sim_changed_from_ui:FolderNodeDTO = marked_folders[-1] 
        del marked_folders[-1]
        retention: Retention = sim_changed_from_ui.get_retention()
        retention.retention_id = retention.retention_id+1 # change to the Next retention type after marked 
        # With inheritance, we can create RetentionUpdateDTO directly from retention fields
        retention_dto = FolderRetention(
            retention_id=retention.retention_id, 
            pathprotection_id=retention.pathprotection_id,
            expiration_date=retention.expiration_date,
            folder_id=sim_changed_from_ui.id
        )
        change_retentions(rootfolder.id, [retention_dto])
        #verify that the two changed simulations are no longer marked for cleanup
        reduced_marked_folders: list[FolderNodeDTO] = read_folders_marked_for_cleanup(rootfolder.id)
        assert len(reduced_marked_folders) == len(marked_folders), \
            f"Expected {len(marked_folders)} marked folders but found {len(reduced_marked_folders)}"

                
        # emulate a change from marked to Next+ by import of a simulation with a new modification date to the rootfolder which is in RETENTION_REVIEW.
        sim_changed_by_import_ui:FolderNodeDTO = marked_folders[-1]
        del marked_folders[-1]
        sim_changed_by_import_ui.modified_date = sim_changed_by_import_ui.modified_date + timedelta(days=cleanup_config.cleanupfrequency + 1)
        fileinfo_sim_changed_by_import_ui:FileInfo = FileInfo(filepath=sim_changed_by_import_ui.path, 
                                                              modified_date=sim_changed_by_import_ui.modified_date, 
                                                              nodetype=FolderTypeEnum.SIMULATION,
                                                              external_retention=ExternalRetentionTypes.NUMERIC)
        insert_or_update_simulations_in_db(rootfolder.id, [fileinfo_sim_changed_by_import_ui])

        #verify that the two changed simulations are no longer marked for cleanup
        reduced_marked_folders: list[FolderNodeDTO] = read_folders_marked_for_cleanup(rootfolder.id)
        assert len(reduced_marked_folders) == len(marked_folders), \
            f"Expected {len(marked_folders)} marked folders but found {len(reduced_marked_folders)}"
