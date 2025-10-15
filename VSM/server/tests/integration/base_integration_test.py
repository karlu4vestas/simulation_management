import pytest
from typing import Dict, List, Any, NamedTuple, Optional
from sqlmodel import Session
from datetime import date, datetime, timedelta
from datamodel.dtos import CleanupConfiguration, FolderNodeDTO, FolderTypeEnum, RootFolderDTO
from datamodel.vts_create_meta_data import insert_vts_metadata_in_db
from app.web_api import FileInfo, insert_or_update_simulation_in_db, read_folders, read_rootfolders, read_rootfolders_by_domain_and_initials
from .testdata_for_import import InMemoryFolderNode, RootFolderWithMemoryFolderTree, RootFolderWithMemoryFolders

class RootFolderWithFolderNodeDTOList(NamedTuple):
    """Named tuple for a root folder and its flattened list of folder nodes"""
    rootfolder: RootFolderDTO
    folders: list[FolderNodeDTO]

class BaseIntegrationTest:
    # --------- helper functions  ----------    
    def get_marked_for_cleanup(self, session: Session, rootfolder: RootFolderDTO) -> RootFolderWithMemoryFolders:
        """Get folders marked for cleanup for a given root folder"""
        pass

    # ---------- process steps to be implemented in the test ----------    
    def setup_new_db_with_vts_metadata(self, session: Session) -> None:
        """Step 1: Set up a new database with VTS metadata"""
        # Implementation must validate that one metadata records are present in the db
        insert_vts_metadata_in_db(session)

        
    def insert_simulations(self, session: Session, rootfolder_with_folders: RootFolderWithMemoryFolders) -> RootFolderWithFolderNodeDTOList:
        #Step 2.2: Insert simulations into database and return all the rootfolder database folders for validation
        
        rootfolder:RootFolderDTO=rootfolder_with_folders.rootfolder
        assert rootfolder.id is not None and rootfolder.id > 0

        # extract and convert the leaves to FileInfo (the leaves  are the simulations) 
        from app.web_api import FileInfo
        file_info_list:list[FileInfo] = [ FileInfo( filepath=folder.path, modified_date=folder.modified_date, nodetype=FolderTypeEnum.VTS_SIMULATION, external_retention=folder.retention) 
                                            for folder in rootfolder_with_folders.folders if folder.is_leaf ]
       
        insert_or_update_simulation_in_db(rootfolder.id, file_info_list)

        # get all rootfolders and folders in the db for validation
        rootfolders: List[RootFolderDTO] = read_rootfolders_by_domain_and_initials(rootfolder.simulationdomain_id)
        rootfolders = [r for r in rootfolders if r.id == rootfolder.id] 
        assert len(rootfolders) == 1
        rootfolder: RootFolderDTO = rootfolders[0]

        folders: List[FolderNodeDTO] = read_folders(rootfolder.id)
        return RootFolderWithFolderNodeDTOList(rootfolder=rootfolder, folders=folders)
    
    """
    def update_cleanup_configuration(self, session: Session, rootfolder: RootFolderDTO, cleanup_configuration: CleanupConfiguration) -> CleanupConfiguration:
        #Step 3: Update the CleanupConfiguration and return the new configuration for a root folder and return the updated configuration
        # Implementation would create cleanup round
        pass
        return self.get_cleanup_configuration(session, rootfolder)
    """
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
