from dataclasses import dataclass
from typing import NamedTuple
from datetime import timedelta
from sqlmodel import Session
import pytest

from cleanup import agents_internal
from datamodel import dtos
from db import db_api

from datamodel.retentions import RetentionCalculator
from datamodel.dtos import FolderNodeDTO, FolderTypeEnum, RootFolderDTO, FileInfo, FolderRetention, RetentionTypeDTO, ExternalRetentionTypes, Retention, RetentionTypeEnum
from datamodel.vts_create_meta_data import insert_vts_metadata_in_db

from db.db_api import exist_rootfolder
from db.db_api import read_rootfolders_by_domain_and_initials, read_folders_marked_for_cleanup, read_folders, read_retentiontypes_by_domain_id, read_simulation_domain_by_name
from db.db_api import read_folder_type_dict_pr_domain_id, read_simulation_domains, read_folder_types_pr_domain_id, read_frequency_by_domain_id, read_cycle_time_by_domain_id   
from db.db_api import change_retentions, normalize_path, insert_or_update_simulations_in_db, insert_rootfolder

from tests.integration.testdata_for_import import InMemoryFolderNode, RootFolderWithMemoryFolders,CleanupConfiguration

class RootFolderWithFolderNodeDTOList(NamedTuple):
    """Named tuple for a root folder and its flattened list of folder nodes"""
    rootfolder: RootFolderDTO
    folders: list[FolderNodeDTO]
    path_protections: list[dtos.PathProtectionDTO]  # from def read_pathprotections( rootfolder_id: int ). may be None or empty

class DataIOSet:
    # This structure is used to collect data for verification during the tests

    key:    str  # input to retrieve the scenario data from cleanup_scenario_data in conftest.py
    input:  RootFolderWithMemoryFolders #input taken directly from the test data fixture
    # If input_path_protections is set then create these protections in db after the initial insertion of simulations
    # it cannot be done before because the API is based on the user selecting the paths in the UI
    path_protections_paths: list[str] = None # input path protections extracted from input. 
    expected_protected_leaf_count:int = 0 # if path_protections_paths is not None then this is the expected number of leafs protected by the path protections

    input_leafs: list[InMemoryFolderNode] = None # input leafs (the simulations) extracted from input
    input_leafs_to_be_marked_dict: dict[str, InMemoryFolderNode] = None
    
    # output
    #rootfolder:RootFolderDTO = None
    output: RootFolderWithFolderNodeDTOList = None  # output rootfolder and folders from db after insertion of simulations
    output_leafs: list[dtos.FolderNodeDTO] = None  # output leafs (the simulations) extracted from output
    output_for_input_leafs: list[dtos.FolderNodeDTO] = None  # output leafs (the simulations) extracted from output and ordered as input leafs

    retention_calculator: RetentionCalculator = None  # retention calculator for the rootfolder

    # retentions
    path_retention: RetentionTypeDTO = None # the path retention for the rootfolder
    marked_retention: RetentionTypeDTO = None # the marked retention for the rootfolder
    undefined_retention: RetentionTypeDTO = None # the undefined retention for the rootfolder
    # nodetypes
    nodetype_leaf: int = None # the folder type id for leaf nodes (simulations)
    nodetype_inner: int = None # the folder type id for inner nodes

    def __init__(self, key:str, input:RootFolderWithMemoryFolders):#, rootfolder:RootFolderDTO):
        self.key = key
        self.input = input
        self.input_leafs = []
        self.input_leafs_to_be_marked_dict = {}
        self.output = None
        self.output_for_input_leafs = []
        self.retention_calculator = None
        self.path_retention = None
        self.marked_retention = None
        self.undefined_retention = None
        self.nodetype_leaf = None
        self.nodetype_inner = None
            

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
        assert len(read_frequency_by_domain_id(simulation_domain_id)) > 0
        assert len(read_cycle_time_by_domain_id(simulation_domain_id)) > 0

    def setup_basic_dataset_io( self, integration_session, scenario_key:str, cleanup_scenario_data) -> DataIOSet:
        # setup the rootfolder and it cleanup configuration in the db
        # collect various metadata useful for the verification functions
          
        simulation_domain_id:int = read_simulation_domain_by_name(domain_name="vts").id

        dataset_io:DataIOSet = None
        for key in [scenario_key]:
            #insert add the simulation domain to the rootfolder and inser the rootfolder in the db
            input: RootFolderWithMemoryFolders = cleanup_scenario_data[key]
            # set the simulation domain, save the rootfolder to the db and then create first_rootfolder_tuple with the rootfolder from the db
            input.rootfolder.simulationdomain_id = simulation_domain_id

            # Its about messy that we are keeping the the rootfolder both in input and output.
            # test if we have already assigne the db_root to the input rootfolder. ifo so then do not change it
            if not exist_rootfolder(input.rootfolder):
                rootfolder:RootFolderDTO = insert_rootfolder(input.rootfolder)
                input.rootfolder = rootfolder
                #verify that the rootfolder was saved correctly
                assert input.rootfolder is not None
                assert input.rootfolder.id is not None and input.rootfolder.id > 0
                assert input.rootfolder.path == input.rootfolder.path
                
                # Create CleanupConfigurationDTO from the in-memory CleanupConfiguration
                # The cleanup_scenario_data fixture uses the old CleanupConfiguration dataclass for in-memory setup
                # Now we create the corresponding CleanupConfigurationDTO database record
                cleanup_config_dto:dtos.CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(integration_session)

                in_memory_config:CleanupConfiguration   = cleanup_scenario_data.get("cleanup_configuration", None)
                cleanup_config_dto.lead_time            = in_memory_config.lead_time
                cleanup_config_dto.frequency     = in_memory_config.frequency
                cleanup_config_dto.start_date   = in_memory_config.start_date
                cleanup_config_dto.progress     = in_memory_config.progress
                integration_session.add(cleanup_config_dto)
                integration_session.commit()
            else: #this case is activate for "root2_part2_data"
                rootfolder:RootFolderDTO = insert_rootfolder(input.rootfolder) # insert return existing rootfolder it it exists 
                input.rootfolder = rootfolder
                cleanup_config_dto:dtos.CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(integration_session)
                
            #cleanup_config_dto = insert_cleanup_configuration(input.rootfolder.id, cleanup_config_dto)
            
            dataset_io:DataIOSet = DataIOSet(key=key,input=input)#, rootfolder=input.rootfolder)
            dataset_io.nodetype_leaf        = read_folder_type_dict_pr_domain_id(dataset_io.input.rootfolder.simulationdomain_id)[FolderTypeEnum.SIMULATION].id
            dataset_io.nodetype_inner       = read_folder_type_dict_pr_domain_id(dataset_io.input.rootfolder.simulationdomain_id)[FolderTypeEnum.INNERNODE].id
            # Use the CleanupConfigurationDTO we just created for the RetentionCalculator
            if cleanup_config_dto:
                #data_set.retention_calculator = RetentionCalculator(read_rootfolder_retentiontypes_dict(data_set.output.rootfolder.id), cleanup_config_dto)
                dataset_io.retention_calculator = RetentionCalculator(dataset_io.input.rootfolder.id, dataset_io.input.rootfolder.cleanup_config_id, integration_session)

            dataset_io.path_retention        = dataset_io.retention_calculator.retention_type_str_dict["path"]
            dataset_io.marked_retention      = dataset_io.retention_calculator.retention_type_str_dict["marked"]
            dataset_io.undefined_retention   = dataset_io.retention_calculator.retention_type_str_dict["?"]

        return dataset_io

    def insert_simulations(self, session: Session, rootfolder_with_folders: RootFolderWithMemoryFolders, path_protections_paths:list[str]) -> RootFolderWithFolderNodeDTOList:
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

        #folders: list[FolderNodeDTO] = read_folders(rootfolder.id)

        # insert path protections if any
        if path_protections_paths:
            db_api.add_pathprotection_by_paths(rootfolder.id, path_protections_paths)
            db_api.apply_pathprotections(rootfolder.id)

        folders: list[FolderNodeDTO] = read_folders(rootfolder.id)

        return RootFolderWithFolderNodeDTOList(rootfolder=rootfolders[0], folders=folders, path_protections=db_api.read_pathprotections(rootfolder.id))

    def import_simulations(self, integration_session, dataio_sets: list[DataIOSet]) -> list[DataIOSet]:
        # import the simulations and prepare the in and output leafs structures for later verification

        # Step 2 validate the output data
        for data_set in dataio_sets:
            data_set.output = self.insert_simulations(integration_session, data_set.input, data_set.path_protections_paths)

            assert data_set.input.rootfolder.id == data_set.output.rootfolder.id  # should never fail because we just got back what we input to insert_simulations

            # extract the the list of leaf in input and output so that it is easy to validate them. Leafs are the vts simulations
            data_set.input_leafs  = sorted( [ folder for folder in data_set.input.folders  if folder.is_leaf ], key=lambda f: normalize_path(f.path) )
            data_set.output_leafs = sorted( [ folder for folder in data_set.output.folders if folder.nodetype_id == data_set.nodetype_leaf ], key=lambda f: normalize_path(f.path) )

            #select and order output simulations so that they match the order of input
            output_leaf_path_dict:dict[str,InMemoryFolderNode] = {normalize_path(f.path): f for f in data_set.output_leafs}
            data_set.output_for_input_leafs = [output_leaf_path_dict[ normalize_path(p.path) ] for p in data_set.input_leafs]

        return dataio_sets


    def verify_db_folder_hierarchy(self, integration_session, dataio_sets: list[DataIOSet]) -> list[DataIOSet]:
        # validate that the folder hierarchy is inserted correctly
        # the expectation is that "second_rootfolder_part_one" was inserted in the DB before "second_rootfolder_part_two"
        # Breaking this constraint will cause the test to fail

        for data_set in dataio_sets:
            match data_set.key:
                case "first_rootfolder":
                    #part one: consist of the same rootfolder that is not shared with other scenarios

                    #check that the all leaves (the simulations) were inserted
                    assert len(data_set.input_leafs) == len(data_set.output_leafs)
                    input_leaf_set  = set([normalize_path(f.path).lower() for f in data_set.input_leafs])
                    output_leaf_set = set([normalize_path(f.path).lower() for f in data_set.output_leafs])
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
                    output_folder_lookup: dict[str, dtos.FolderNodeDTO] = {normalize_path(folder.path): folder for folder in data_set.output.folders}    
                    for folder in data_set.input.folders: 
                        db_folder:dtos.FolderNodeDTO = output_folder_lookup.get(normalize_path(folder.path))
                        
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
                    assert len(data_set.input_leafs) == len(data_set.output_leafs)
                    input_leaf_set  = set([normalize_path(f.path).lower() for f in data_set.input_leafs])
                    output_leaf_set = set([normalize_path(f.path).lower() for f in data_set.output_leafs])
                    diff = input_leaf_set.difference( output_leaf_set )
                    assert len(diff) == 0

                    # Verify each input leaf has a corresponding output folder with the correct node type
                    # Create a mapping of paths to is_leaf status from ALL input folders (not just leafs)
                    output_folder_lookup: dict[str, dtos.FolderNodeDTO] = {normalize_path(folder.path): folder for folder in data_set.output.folders}
                    for folder in data_set.input_leafs:
                        db_folder:dtos.FolderNodeDTO = output_folder_lookup.get(normalize_path(folder.path))                        
                        assert db_folder is not None, f"data_set.key={data_set.key}: Folder {folder.path} not found in database folders"
                        assert db_folder.nodetype_id == data_set.nodetype_leaf, \
                                f"data_set.key={data_set.key}: Folder {db_folder.path} is a leaf in input but has nodetype_id {db_folder.nodetype_id} instead of {data_set.nodetype_leaf}"

                case "second_rootfolder_part_two":
                    # At this point part_one and part_two of the second_rootfolder have been inserted.
                    # The output includes all folders in db belonging to the rootfolder.
                    
                    # The folders in the input must, therefore, be in the output

                    # the number of input leafs must be smaller or equal to the number of output leafs.
                    # Equality can happen if the split of folders between the two cases allocates all leafs to "second_rootfolder_part_two"
                    assert len(data_set.input_leafs) <= len(data_set.output_leafs)

                    # Verify all input leaves exist in output
                    input_leaf_set  = set([normalize_path(f.path).lower() for f in data_set.input_leafs])
                    output_leaf_set = set([normalize_path(f.path).lower() for f in data_set.output_leafs])
                    assert input_leaf_set.issubset( output_leaf_set )

                    # all input folder must be in output
                    assert len(data_set.input.folders) <= len(data_set.output.folders)
                    input_path_set  = set([normalize_path(f.path).lower() for f in data_set.input.folders])
                    output_path_set = set([normalize_path(f.path).lower() for f in data_set.output.folders])
                    assert input_path_set.issubset( output_path_set )

                    
                    # Verify each input leaf has a corresponding output folder with the correct node type
                    # Create a mapping of paths to is_leaf status from ALL input folders (not just leafs)
                    output_folder_lookup: dict[str, dtos.FolderNodeDTO] = {normalize_path(folder.path): folder for folder in data_set.output.folders}
                    for folder in data_set.input_leafs:
                        db_folder:dtos.FolderNodeDTO = output_folder_lookup.get(normalize_path(folder.path))
                        assert db_folder is not None, f"data_set.key={data_set.key}: Folder {folder.path} not found in database folders"
                        assert db_folder.nodetype_id == data_set.nodetype_leaf, \
                                f"data_set.key={data_set.key}: Folder {db_folder.path} is a leaf in input but has nodetype_id {db_folder.nodetype_id} instead of {data_set.nodetype_leaf}"
                case _:
                    raise ValueError(f"Unknown scenario data key: {data_set.key}")
                
        return dataio_sets


    def verify_path_protections(self, data_set:DataIOSet):
        # Verify path protections if the input was configured for that
        if data_set.output.path_protections :
            # assert the test was configured properly if path protections in db then they must have been configured in the input
            # otherwise the test is invalid
            if data_set.key == "second_rootfolder_part_one":
                # path protections inserted in part one before the folder exist will be ignored
                assert len(data_set.output.path_protections) <= len(data_set.path_protections_paths), \
                    f"data_set.key={data_set.key}: Number of path protections in db {len(data_set.output.path_protections)} does not match input {len(data_set.path_protections_paths)}"
            else:    
                # case for data_set.key == "second_rootfolder_part_one"  path protections inserted in part one before the folder exist will be ignored
                assert data_set.key == "first_rootfolder" or data_set.key == "second_rootfolder_part_two", f"data_set.key must be either 'first_rootfolder' or 'second_rootfolder_part_one' "
                
                # this case is special because part one inserted the path protections
                assert len(data_set.output.path_protections) == len(data_set.path_protections_paths), \
                    f"data_set.key={data_set.key}: Number of path protections in db {len(data_set.output.path_protections)} does not match input {len(data_set.path_protections_paths)}"
                assert data_set.expected_protected_leaf_count > 0, \
                    f"data_set.key={data_set.key}: expected_protected_leaf_count must be set when path protections are configured in input"
            
            # extract the number of protected folders from the database
            # select all folders under data_set.output.rootfolder with a pathprotection_id
            folders_with_path_protection:list[FolderNodeDTO] = [ folder for folder in data_set.output.folders if folder.pathprotection_id is not None ]
            #verify that they are all leafs
            assert all( folder.nodetype_id == data_set.nodetype_leaf for folder in folders_with_path_protection ), f"data_set.key={data_set.key}: Not all folders with pathprotection_id are leafs"
            # assert that the pathprotection_id is assigned for alle the folders
            assert all( (folder.pathprotection_id is not None and folder.pathprotection_id !=0)  for folder in folders_with_path_protection ), f"data_set.key={data_set.key}: All folders with pathprotection_id must have it assigned"

            #assert that the number of protected leafs is as expected
            assert len(folders_with_path_protection) == data_set.expected_protected_leaf_count, \
                f"data_set.key={data_set.key}: Expected {data_set.expected_protected_leaf_count} protected leafs but found {len(folders_with_path_protection)} in database"
            
    def verify_retentions(self, integration_session, data_io_sets: list[DataIOSet]) -> list[DataIOSet]:
        # test the attributes of the inserted simulations before the start of a cleanup round
        # @TODO: a review of this function would be good
        for data_set in data_io_sets:
                
            if data_set.output.path_protections :
                self.verify_path_protections(data_set)

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
                        if data_set.retention_calculator.progress == dtos.CleanupProgress.Progress.INACTIVE:
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
                            f"data_set.key={data_set.key}, folder={folder.path}. The external and internal non-numeric retention are different intern:{folder.retention_id} external converted to internal:{sim_retention_type_id}"


            #validate that retention is none for inner nodes
            for folder in data_set.output.folders:
                if folder.nodetype_id != data_set.nodetype_leaf:
                    assert folder.retention_id is None, f"data_set.key={data_set.key}: Folder {folder.path} is an inner node and must have retention_id=None but has retention_id={folder.retention_id}"

        return data_io_sets
    
    def cleanup_cycle_start(self, rootfolder_id:int ):
        # we must force the cleanup progress state to SCANNING then MARKING_FOR_RETENTION_REVIEW before starting the cleanup cycle
        # This is normally done by a scheduled job but here we need to do it manually to be able to test the start of the cleanup round 
        from cleanup import cleanup_dtos
        cleanup_state:cleanup_dtos.CleanupState = cleanup_dtos.CleanupState.load_by_rootfolder_id(rootfolder_id)
        if not (cleanup_state.transition_to( dtos.CleanupProgress.Progress.SCANNING ) and \
                cleanup_state.transition_to( dtos.CleanupProgress.Progress.MARKING_FOR_RETENTION_REVIEW ) ):
            raise ValueError(f"Unable to transition cleanup state to SCANNING then MARKING_FOR_RETENTION_REVIEW for rootfolder_id={rootfolder_id}")
        cleanup_state.save_to_db()
        
        agents_internal.AgentMarkSimulationsPreReview.mark_simulations(rootfolder_id )

        #simulations are now marked for retention review. Transition to RETENTION_REVIEW
        cleanup_state.transition_to( dtos.CleanupProgress.Progress.RETENTION_REVIEW )
        cleanup_state.save_to_db()

    def verify_retentions_after_start_of_cleanup_round(self, integration_session, data_sets: list[DataIOSet]) -> list[DataIOSet]:
        # Verify that retentions are updated correctly due to the start of the cleanup round
        for data_set in data_sets:
                
            if data_set.output.path_protections :
                self.verify_path_protections(data_set)

            path_or_endstage_retention_ids = {data_set.path_retention.id, *[retention.id for retention in data_set.retention_calculator.get_endstage_retentions()]}

            #prepare
            # lookup of input folders
            # the list of folders be marked for cleanup because the "modified_date+cycle_time" is before the "start_date" and the folder is not in path retention (there is no path protections yet)
            input_leafs_lookup: dict[str, InMemoryFolderNode] = {normalize_path(folder.path): folder for folder in data_set.input.folders if folder.is_leaf}
            data_set.input_leafs_to_be_marked_dict = data_set.input.get_leafs_to_be_marked_dict(data_set.path_protections_paths)

            rootfolder:RootFolderDTO = data_set.output.rootfolder
            output_leafs_before_start: dict[str,FolderNodeDTO] = {normalize_path(folder.path): folder for folder in read_folders(rootfolder.id) if folder.nodetype_id == data_set.nodetype_leaf}
            
            marked_folders: list[FolderNodeDTO] = read_folders_marked_for_cleanup(data_set.output.rootfolder.id)
            assert len(marked_folders) == 0, f"before starting cleanup round number of simulations marked for cleanup should be zero"

            # step 1.1: start the cleanup round and
            self.cleanup_cycle_start(rootfolder.id )
            data_set.output = RootFolderWithFolderNodeDTOList(rootfolder=rootfolder, folders=read_folders(rootfolder.id), path_protections=db_api.read_pathprotections(rootfolder.id))
            output_leafs_after_start: dict[str,FolderNodeDTO] = {normalize_path(folder.path): folder for folder in data_set.output.folders if folder.nodetype_id == data_set.nodetype_leaf}

            marked_folders: list[FolderNodeDTO] = read_folders_marked_for_cleanup(data_set.output.rootfolder.id)
            assert len(marked_folders) == len(data_set.input_leafs_to_be_marked_dict), f"after starting cleanup round number the planned and actual simulations marked for cleanup should be identical"

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
                elif leaf_before_start.retention_id == data_set.path_retention.id: # the leaf_input will becom a numeric retention unless it is under path retention
                    assert leaf_after_start.retention_id == data_set.path_retention.id,f"if the Folder {leaf_after_start.path} was under path retention before start it must remain so after start but is {leaf_after_start.retention_id}"
                else:    
                    assert data_set.retention_calculator.is_numeric(leaf_before_start.retention_id) or leaf_before_start.retention_id == data_set.undefined_retention.id, \
                        f"The Folder {leaf_before_start.path} should have been numeric or undefined before start but is {leaf_before_start.retention_id}"
                    assert data_set.retention_calculator.is_numeric(leaf_after_start.retention_id) or leaf_after_start.retention_id == data_set.undefined_retention.id, \
                        f"The Folder {leaf_after_start.path} should have been numeric or undefined after start but is {leaf_after_start.retention_id}"

                    # verify that 
                    # 1) input leafs planned to be marked for cleanup 
                    #     are NOT marked BEFORE the call to cleanup_cycle_start 
                    #     ARE marked after the call to cleanup_cycle_start
                    # 2) that input leafs that are NOT planned to be marked for cleanup are non numeric BEFORE and AFTER the call to cleanup_cycle_start
                    if path_after_start in data_set.input_leafs_to_be_marked_dict:
                        assert leaf_before_start.retention_id != data_set.marked_retention.id, \
                            f"Folder {leaf_before_start.path} should NOT have been 'marked' before start but is {leaf_before_start.retention_id}"
                        assert leaf_after_start.retention_id == data_set.marked_retention.id, \
                            f"Folder {leaf_after_start.path} should have been 'marked' after start but is {leaf_after_start.retention_id}"
                    
        return data_sets


    def import_and_start_cleanup_round_and_import_more_simulations_with_test_of_retentions(self, integration_session, root2_part1_data:DataIOSet, root2_part2_data:DataIOSet) -> list[DataIOSet]:
        # validate the state of the db before inserting more simulations into the same rootfolder

        # First entry validation: When calling this the first part of the second rootfolder is imported and a cleanup round is started
        cleanup_config:dtos.CleanupConfigurationDTO = root2_part1_data.output.rootfolder.get_cleanup_configuration(integration_session)
        assert cleanup_config.progress == dtos.CleanupProgress.Progress.RETENTION_REVIEW, f"the second root folders part one must have been started for you to use this function. CleanupProgress is {cleanup_config.progress}"

        # Second entry validation:
        #   Also verify that the folders planned to be marked for cleanup can be extracted from the db. 
        #   These verification were already done but we need to be certain of the state of the start of this function
        assert root2_part1_data.input_leafs_to_be_marked_dict is not None, "input_leafs_to_be_marked_dict is None"
        marked_folders: list[FolderNodeDTO] = read_folders_marked_for_cleanup(root2_part1_data.output.rootfolder.id)
        assert len(marked_folders) == len(root2_part1_data.input_leafs_to_be_marked_dict), f"before insertion number of input_leafs_to_be_marked_dict should be the same as in the db"


        # Insert folders from the root2_part2_data 
        self.import_simulations(integration_session, [root2_part2_data])
        self.verify_db_folder_hierarchy(integration_session, [root2_part2_data])
        self.verify_retentions(integration_session, [root2_part2_data])


        #Verification 1: verify that the insertion did not change the retention of the rootfolder' part_one folders
        # compare the retention of part_one and part_two folders retnetions after more folders have been inserted from part two
        second_part_one_output_folders_lookup: dict[str, FolderNodeDTO] = {normalize_path(folder.path): folder for folder in root2_part1_data.output.folders }
        second_part_two_output_folders_lookup: dict[str, FolderNodeDTO] = {normalize_path(folder.path): folder for folder in root2_part2_data.output.folders}
        for part_one_path,part_one_folder in second_part_one_output_folders_lookup.items():
            part_one_db_folder:FolderNodeDTO = second_part_two_output_folders_lookup.get(part_one_path, None)
            assert part_one_db_folder is not None, f"unable to lookup db_folder for {part_one_path}"

            assert part_one_folder.retention_id == part_one_db_folder.retention_id , \
                f"Folder {part_one_path} The insertion of part two change a retention from part one. Before {part_one_folder.retention_id}, after {part_one_db_folder.retention_id}"
        

        # Verification 2: verify that no folder from part_two_data got marked
        marked_retention_id = root2_part1_data.marked_retention.id
        second_part_two_input_leafs_lookup: dict[str, InMemoryFolderNode] =  {normalize_path(folder.path): folder for folder in root2_part2_data.input_leafs } 
        for second_part_two_leaf_path, second_part_two_input_leaf_folder in second_part_two_input_leafs_lookup.items():
            part_two_db_folder:FolderNodeDTO = second_part_two_output_folders_lookup.get(second_part_two_leaf_path, None)
            assert part_two_db_folder is not None, f"unable to lookup second_part_two_leaf_output_folder for {second_part_two_leaf_path}"       
            assert part_two_db_folder.retention_id != marked_retention_id, f"A part two folder {second_part_two_leaf_path} got marked for cleanup"


        # Verification 3: verify that leafs marked for cleanup is identical to those marked for cleanup before the insertion of part_two_data folders after the start of the cleanup round        
        # The only condition that would allow this is a changed modification date of a marked folder that were imported aging but we are only inserting new folders
        marked_folders: list[FolderNodeDTO] = read_folders_marked_for_cleanup(root2_part2_data.output.rootfolder.id)
        # verify the count
        assert len(marked_folders) == len(root2_part1_data.input_leafs_to_be_marked_dict), f"The number of marked folders was change by the insertion. Before {len(marked_folders)} After {len(root2_part1_data.input_leafs_to_be_marked_dict)}"
        # verify that only the folders plannend to be marked in the input data are marked
        for folder in marked_folders:
            input_folder: InMemoryFolderNode = root2_part1_data.input_leafs_to_be_marked_dict.get(normalize_path(folder.path), None)
            assert input_folder is not None, f"Folder {folder.path} was not expected to be marked for cleanup"

        return [root2_part1_data, root2_part2_data]

    def setup_path_protections_for_root1_data(self,root1_data:DataIOSet):
        """Setup path protection for root1_data with two-levels and the expected result.
        
        This will be used in verify_retentions that calls verify_path_protections.
        get_two_level_path_protections() now returns a list of tuples [(high, low), ...].
        We select the first pair and flatten it to [high_path, low_path].
        """
        path_protection_pairs: list[tuple[str,str]] = root1_data.input.get_two_level_path_protections()
        
        # Validate we got at least one pair
        assert path_protection_pairs and len(path_protection_pairs) > 0, f"test setup failure: get_two_level_path_protections() returned empty list for root1_data"
        
        # Select the first pair and flatten it to a list of 2 paths [high_level, lower_level]
        first_pair = path_protection_pairs[0]
        root1_data.path_protections_paths = [first_pair[0], first_pair[1]]

        assert len(root1_data.path_protections_paths) == 2, f"test setup failure: expected 2 path protections but found {len(root1_data.path_protections_paths)} abort the test"
        
        # Establish the number of expected path protections for each rootfolder so they can be verified after import in verify_retentions
        # count the input leaf folders that start with any of the path protection paths
        # Note: Using any() ensures each leaf is counted only once, even if multiple path protections could match
        root1_data.expected_protected_leaf_count = root1_data.input.count_protected_leafs(root1_data.path_protections_paths)

    def setup_path_protections_for_root2_data(self, integration_session, root2_part1_data:DataIOSet, root2_part2_data:DataIOSet):
        """Setup path protection for root2_part1_data and root2_part2_data with two-levels and the expected result.
        Notice that part1 and part2 share the same rootfolder. To obtain the highest "coverage/challenging conditions"
        the selected path protections must have simulations in both parts.
        We ASSUME that root2_part1 is inserted before root2_part2.
        
        The setup will be used to:
         - insert the path protection in "insert_simulations"
         - verify the path protections in "verify_path_protections" using the expected results
        """
        path_pairs_part1: list[tuple[str,str]] = root2_part1_data.input.get_two_level_path_protections()
        path_pairs_part2: list[tuple[str,str]] = root2_part2_data.input.get_two_level_path_protections()
        
        # Validate we got pairs from both parts
        assert path_pairs_part1 and len(path_pairs_part1) > 0, f"test setup failure: get_two_level_path_protections() returned empty list for root2_part1_data"
        assert path_pairs_part2 and len(path_pairs_part2) > 0, f"test setup failure: get_two_level_path_protections() returned empty list for root2_part2_data"
        
        # Find pairs with matching high-level paths between part1 and part2
        # Collect all high-level paths from part1
        part1_high_paths: set[str] = {high for high, _ in path_pairs_part1}
        
        # Find matching high-level paths in part2 (these are the shared high-level paths)
        shared_high_paths: list[tuple[str,str]] = [(high, low) for high, low in path_pairs_part2 if high in part1_high_paths]
        
        # Validate we found at least one match
        assert len(shared_high_paths) > 0, f"test setup failure: unable to find any matching high-level path protections between part1 and part2 of rootfolder 2"
        
        # Select the first matching high-level path
        selected_highlevel = shared_high_paths[0][0]
        
        # Collect the first matching (high, low) tuple from both part1 and part2
        # and merge them into a single protection path set
        merged_protection_paths = set([selected_highlevel])
        
        # Add the first matching lower-level path from part1
        for high, low in path_pairs_part1:
            if high == selected_highlevel:
                merged_protection_paths.add(low)
                break  # Only take the first match
        
        # Add the first matching lower-level path from part2
        for high, low in path_pairs_part2:
            if high == selected_highlevel:
                merged_protection_paths.add(low)
                break  # Only take the first match
        
        merged_protection_paths_list = sorted(list(merged_protection_paths))
        
        assert len(merged_protection_paths_list) >= 2, \
            f"test setup failure: expected at least 2 path protections from merged selection but found {len(merged_protection_paths_list)} abort the test"
        
        # Set the merged path protections for both part1 and part2
        # This tests important edge cases:
        # - Part1: Creates protections for paths that exist in part1 (selected_highlevel, part1_lower)
        #          Paths that don't exist yet (part2_lower) are skipped and returned in failed_paths
        # - Part2: Reuses existing protections (selected_highlevel, part1_lower marked as already_existed)
        #          Creates new protection for part2_lower which now exists - if necessary
        root2_part1_data.path_protections_paths = merged_protection_paths_list
        root2_part2_data.path_protections_paths = merged_protection_paths_list

        # Calculate the expected results based on insertion order:
        # - part1: Only leafs from part1 that match the merged protection paths will be protected
        # - part2: The cumulative total (part1 + part2 leafs matching the same protection paths) will be protected
        #   because both parts share the same rootfolder and the path protections persist
        root2_part1_data.expected_protected_leaf_count = root2_part1_data.input.count_protected_leafs(merged_protection_paths_list)
        root2_part2_data.expected_protected_leaf_count = root2_part1_data.expected_protected_leaf_count + root2_part2_data.input.count_protected_leafs(merged_protection_paths_list)

    def test_1_verify_folder_hierarchy_and_retentions_after_import(self, integration_session, cleanup_scenario_data):
        # Import all test dataset and verify the folder structures in the db and the retentions of the inserted simulations
        self.setup_new_db_with_vts_metadata(integration_session)
        data_keys = ["first_rootfolder", "second_rootfolder_part_one", "second_rootfolder_part_two"]       
        data_io_sets: list[DataIOSet] = [self.setup_basic_dataset_io(integration_session, scenario_key=key, cleanup_scenario_data=cleanup_scenario_data) for key in data_keys]

        root1_data_set:DataIOSet           = data_io_sets[0]
        second_part_one_data_set:DataIOSet = data_io_sets[1]
        second_part_two_data_set:DataIOSet = data_io_sets[2]
        self.setup_path_protections_for_root1_data(root1_data_set)
        self.setup_path_protections_for_root2_data(integration_session, second_part_one_data_set, second_part_two_data_set) 

        for data_set in data_io_sets:
            #initialize the db and then verify attributes of the inserted simulations
            self.import_simulations(integration_session, [data_set])
            self.verify_db_folder_hierarchy(integration_session, [data_set])
            data_io_sets = self.verify_retentions(integration_session, [data_set])


    def test_2_verify_retentions_after_import_and_start_of_cleanup_round(self, integration_session, cleanup_scenario_data):
        # Verify the retnetions after start of cleanup and in particular that the testdata' folder retentions planned to get marked are marked in teh db
    
        self.setup_new_db_with_vts_metadata(integration_session)
        data_keys = ["first_rootfolder", "second_rootfolder_part_one"]       
        data_io_sets: list[DataIOSet] = [self.setup_basic_dataset_io(integration_session, scenario_key=key, cleanup_scenario_data=cleanup_scenario_data) for key in data_keys]
        root1_data_set = data_io_sets[0]
        root2_part1_data_set = data_io_sets[1]
        self.setup_path_protections_for_root1_data(root1_data_set)
        self.setup_path_protections_for_root1_data(root2_part1_data_set)

        for data_set in data_io_sets:
            self.import_simulations(integration_session, [data_set])
            self.verify_db_folder_hierarchy(integration_session, [data_set])
            self.verify_retentions(integration_session, [data_set])
            self.verify_retentions_after_start_of_cleanup_round(integration_session, [data_set])


    def test_3_retentions_of_insertions_after_start_of_cleanup_round(self, integration_session, cleanup_scenario_data):
        # verify that inserting more simulations into a rootfolder after a cleanup round has been started works does not affect the marked folders
        self.setup_new_db_with_vts_metadata(integration_session)
        data_keys:list[str] = ["second_rootfolder_part_one", "second_rootfolder_part_two"]
        data_io_sets: list[DataIOSet] = [self.setup_basic_dataset_io(integration_session, scenario_key=key, cleanup_scenario_data=cleanup_scenario_data) for key in data_keys]
        second_part_one_data_set = data_io_sets[0]
        second_part_two_data_set = data_io_sets[1]
        self.setup_path_protections_for_root2_data(integration_session, second_part_one_data_set, second_part_two_data_set) 

        #import part one of the rootfolder
        self.import_simulations(integration_session, [second_part_one_data_set])
        self.verify_db_folder_hierarchy(integration_session, [second_part_one_data_set])
        self.verify_retentions(integration_session, [second_part_one_data_set])
        self.verify_retentions_after_start_of_cleanup_round(integration_session, [second_part_one_data_set])
        
        #part one is now imported and started. 
        #Now import part two and verify that the retentions are correct
        self.import_and_start_cleanup_round_and_import_more_simulations_with_test_of_retentions(integration_session, second_part_one_data_set, second_part_two_data_set)

    def test_4_verify_changes_to_retentions_after_start_of_cleanup_round(self, integration_session, cleanup_scenario_data):
        # Verify that marked folders retentions can be changed:
        #  - from marked to other retention types by changing the retention using the change_retentions API
        #  - from marked to other retention by changing the simulations modified data and re-importing the simulations
        self.setup_new_db_with_vts_metadata(integration_session)

        # Best to use second_rootfolder_part_one because the data were testd in other tests already
        data_keys:list[str] = ["first_rootfolder"]
        data_io_sets: list[DataIOSet] = [self.setup_basic_dataset_io(integration_session, scenario_key=key, cleanup_scenario_data=cleanup_scenario_data) for key in data_keys]
        root1_data_set = data_io_sets[0]
        self.setup_path_protections_for_root1_data(root1_data_set)

        self.import_simulations(integration_session, data_io_sets)
        self.verify_db_folder_hierarchy(integration_session, data_io_sets)
        self.verify_retentions(integration_session, data_io_sets)
        self.verify_retentions_after_start_of_cleanup_round(integration_session, data_io_sets)


        # validate that the folder is in review state
        rootfolder:RootFolderDTO                    = root1_data_set.output.rootfolder
        marked_folders:list[FolderNodeDTO]          = read_folders_marked_for_cleanup(rootfolder.id)
        cleanup_config:dtos.CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(integration_session)
        assert cleanup_config.progress == dtos.CleanupProgress.Progress.RETENTION_REVIEW, f"The rootfolder should be RETENTION_REVIEW but is {cleanup_config.progress}"

        # emulate a change of one retention from marked to a retention after it
        # Pick the last marked folder for change and remove it from the list of marked folders to keep track of how many are left      
        sim_changed_from_ui:FolderNodeDTO = marked_folders[-1] 
        del marked_folders[-1]
        # Create a FolderRetention with a modified retention so we can use the 'change_retentions' api
        retention: Retention = sim_changed_from_ui.get_retention()
        retention.retention_id = retention.retention_id + 1  #this will work because there are multiple numeric retentions after marked. see vts_create_meta_data.py
        retention_dto:FolderRetention = FolderRetention.create(folder_id=sim_changed_from_ui.id, retention=retention)
        change_retentions(rootfolder.id, [retention_dto])

        #verify that the modified simulations is  longer marked for cleanup
        reduced_marked_folders: list[FolderNodeDTO] = read_folders_marked_for_cleanup(rootfolder.id)
        assert len(reduced_marked_folders) == len(marked_folders), f"near sim_changed_from_ui: Expected {len(marked_folders)} marked folders but found {len(reduced_marked_folders)}"
        # Verify that sim_changed_from_ui is not found in the reduced marked folders
        assert sim_changed_from_ui.id not in [folder.id for folder in reduced_marked_folders], f"sim_changed_from_ui (id={sim_changed_from_ui.id}, {sim_changed_from_ui.path}) should not be in the reduced marked folders"


        # emulate changing the simulation sbny changing the modified date:
        #  - change one marked simulation. expected is that is is no longer marked for cleanup
        #  - change one path protected simulation. Expected is that it is still path protected
        sim_changed_by_import_ui:FolderNodeDTO = marked_folders[-1]
        del marked_folders[-1]
        #sim_changed_by_import_ui.modified_date = sim_changed_by_import_ui.modified_date + timedelta(days=cleanup_config.frequency + 1)
        sim_changed_by_import_ui.modified_date = sim_changed_by_import_ui.modified_date + timedelta(days=1)
        fileinfo_sim_changed_by_import_ui:FileInfo = FileInfo(filepath=sim_changed_by_import_ui.path, 
                                                              modified_date=sim_changed_by_import_ui.modified_date, 
                                                              nodetype=FolderTypeEnum.SIMULATION,
                                                              external_retention=ExternalRetentionTypes.NUMERIC)

        pathprotected_folders: list[FolderNodeDTO] = db_api.read_simulations_by_retention_type(rootfolder.id, dtos.RetentionTypeEnum.PATH)
        assert pathprotected_folders is not None, "unable to find a path protected folder for changing modified date"
        pathprotected_folder: FolderNodeDTO = pathprotected_folders[0]
        fileinfo_pathprotected_folder:FileInfo = FileInfo(filepath=pathprotected_folder.path, 
                                                        modified_date=pathprotected_folder.modified_date, 
                                                        nodetype=FolderTypeEnum.SIMULATION,
                                                        external_retention=ExternalRetentionTypes.NUMERIC)
        insert_or_update_simulations_in_db(rootfolder.id, [fileinfo_sim_changed_by_import_ui, fileinfo_pathprotected_folder])

        # Verify that the two changed simulations are no longer marked for cleanup
        reduced_marked_folders: list[FolderNodeDTO] = read_folders_marked_for_cleanup(rootfolder.id)
        assert len(reduced_marked_folders) == len(marked_folders), f"near sim_changed_by_import_ui: Expected {len(marked_folders)} marked folders but found {len(reduced_marked_folders)}"
        # Verify that sim_changed_from_ui is not found in the reduced marked folders
        assert sim_changed_by_import_ui.id not in [folder.id for folder in reduced_marked_folders], f"sim_changed_by_import_ui (id={sim_changed_by_import_ui.id}, {sim_changed_by_import_ui.path}) should not be in the reduced marked folders"

        pathprotected_folder_after_insert: FolderNodeDTO = db_api.read_folder(pathprotected_folder.id)
        assert pathprotected_folder_after_insert.pathprotection_id is not None, \
            f"pathprotected_folder_after_insert (id={pathprotected_folder.id}, {pathprotected_folder.path}) should still be path protected but pathprotection_id is None"
        assert pathprotected_folder.retention_id == pathprotected_folder_after_insert.retention_id, \
            f"pathprotected_folder_after_insert (id={pathprotected_folder.id}, {pathprotected_folder.path}) retention_id should be unchanged but was {pathprotected_folder_after_insert.retention_id}"

    def test_5_root1_verify_folder_hierarchy_and_retentions_after_import_with_path_protections(self, integration_session, cleanup_scenario_data):
        # Import all test dataset and verify the folder structures in the db and the retentions of the inserted simulations
        self.setup_new_db_with_vts_metadata(integration_session)
        data_keys = ["first_rootfolder"]       
        data_io_sets: list[DataIOSet] = [self.setup_basic_dataset_io(integration_session, scenario_key=key, cleanup_scenario_data=cleanup_scenario_data) for key in data_keys]
        root1_data = data_io_sets[0]

        self.setup_path_protections_for_root1_data(root1_data)

        for data_set in data_io_sets:
            #initialize the db and then verify attributes of the inserted simulations
            self.import_simulations(integration_session, [data_set])
            self.verify_db_folder_hierarchy(integration_session, [data_set])
            self.verify_retentions(integration_session, [data_set])
