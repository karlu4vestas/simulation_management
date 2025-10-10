import pytest
from typing import Dict, List, Any, NamedTuple, Optional
from sqlmodel import Session
from datetime import date, datetime, timedelta
from datamodel.dtos import CleanupConfiguration, FolderNodeDTO, FolderTypeEnum, RootFolderDTO
from datamodel.vts_create_meta_data import insert_vts_metadata_in_db
from app.web_api import FileInfo, insert_or_update_simulation_in_db, read_folders, read_rootfolders, read_rootfolders_by_domain_and_initials
from .testdata_for_import import InMemoryFolderNode, RootFolderWithMemoryFolderTree, RootFolderWithMemoryFolders, generate_in_memory_rootfolder_and_folder_hierarchies

class RootFolderWithFolderInfoList(NamedTuple):
    """Named tuple for a root folder and its flattened list of folder nodes"""
    rootfolder: RootFolderDTO
    folderinfolist: list[FileInfo]

class RootFolderWithFolderNodeDTOList(NamedTuple):
    """Named tuple for a root folder and its flattened list of folder nodes"""
    rootfolder: RootFolderDTO
    folders: list[FolderNodeDTO]

class BaseIntegrationTest:

    # ---------- helper functions  ----------    
    def get_marked_for_cleanup(self, session: Session, rootfolder: RootFolderDTO) -> RootFolderWithMemoryFolders:
        """Get folders marked for cleanup for a given root folder"""
        pass

    # ---------- process steps to be implemented in the test ----------    
    def setup_new_db_with_vts_metadata(self, session: Session) -> None:
        """Step 1: Set up a new database with VTS metadata"""
        # Implementation must validate that one metadata records are present in the db
        insert_vts_metadata_in_db(session)

        
    def insert_simulations(self, session: Session, rootfolder_with_folders_list: list[RootFolderWithMemoryFolders]) -> list[RootFolderWithFolderNodeDTOList]:
        """Step 2.2: Insert simulations into database and return all folders and rootfolders in the database for validation
        
        Args:
            session: Database session
            simulation_data: List of tuples containing root folders and their file info
            
        Returns:
            List of RootFolderWithFolderList with the inserted folders from database
        """
        results: list[RootFolderWithFolderNodeDTOList] = []
        # for now we handle one root folder
        i: int = 0
        for rootfolder_with_folders in rootfolder_with_folders_list:

            # extract and convert the leaves to FileInfo (the leaves  are the simulations) 
            from app.web_api import FileInfo
            file_info_list = [ FileInfo( filepath=folder.path, modified_date=date.today(), nodetype=FolderTypeEnum.VTS_SIMULATION, retention_id=None) 
                            for folder in rootfolder_with_folders.folders if folder.is_leaf ]
            leafs :RootFolderWithFolderInfoList = RootFolderWithFolderInfoList(rootfolder=rootfolder_with_folders.rootfolder, folderinfolist=file_info_list)

            assert leafs.rootfolder.id is not None and leafs.rootfolder.id > 0
            
            # Convert InMemoryFolderNodes to FileInfo for insertion
            file_infos: list[FileInfo] = []  # Implementation would convert folder_nodes to FileInfo
            insert_or_update_simulation_in_db(leafs.rootfolder.id, leafs.folderinfolist)

            # get all rootfolders and folders in the db for validation
            rootfolders: List[RootFolderDTO] = read_rootfolders_by_domain_and_initials(leafs.rootfolder.simulationdomain_id)
            assert len([r for r in rootfolders if r.id == leafs.rootfolder.id]) == 1
            rootfolder: RootFolderDTO = rootfolders[0]


            folders: List[FolderNodeDTO] = read_folders(rootfolder.id)
            results.append(RootFolderWithFolderNodeDTOList(rootfolder=rootfolder, folders=folders))

        return results

    def update_cleanup_configuration(self, session: Session, rootfolder: RootFolderDTO, cleanup_configuration: CleanupConfiguration) -> CleanupConfiguration:
        """Step 3: Update the CleanupConfiguration and return the new configuration for a root folder and return the updated configuration"""
        # Implementation would create cleanup round
        pass
        return self.get_cleanup_configuration(session, rootfolder)

    def start_cleanup_round(self, session: Session, rootfolder: RootFolderDTO) -> RootFolderWithMemoryFolders:
        """Step 3: Initialize a multi-week cleanup round and return simulations marked for cleanup
        
        Args:
            session: Database session
            rootfolder: The root folder to start cleanup for
            
        Returns:
            RootFolderWithFolderList containing the root folder and folders marked for cleanup
        """
        # Implementation would create cleanup round
        pass
        return self.get_marked_for_cleanup(session, rootfolder)
    
    def update_retention_during_cleanup(self, session: Session, retention_changes: Dict) -> None:
        """Step 4: Update retention policies during active cleanup"""
        # Implementation would modify retention policies
        pass
    
    def update_simulations_during_cleanup(self, session: Session, simulation_updates: RootFolderWithMemoryFolders) -> None:
        """Step 5: Update simulations during active cleanup round
        
        Args:
            session: Database session
            simulation_updates: RootFolderWithFolderList containing the folders to update
        """
        # Implementation would modify simulation records
        pass
    
    def execute_cleanup(self, session: Session, cleanup_round_id: int) -> Dict[str, int]:
        """Step 6: Execute cleanup at end of cleanup round"""
        # Implementation would perform cleanup operations
        # Return stats like {"cleaned": 5, "retained": 10}
        pass
    
    def verify_cleanup_results(self, session: Session, expected_results: Dict) -> None:
        """Verify the final state matches expectations"""
        # Implementation would assert final database state
        pass
