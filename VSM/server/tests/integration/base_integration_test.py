import pytest
from typing import Dict, List, Any, Optional
from sqlmodel import Session
from datetime import datetime, timedelta
from datamodel.dtos import CleanupConfiguration, FolderNodeDTO, RootFolderDTO
from datamodel.vts_create_meta_data import insert_vts_metadata_in_db
from app.web_api import FileInfo, insert_or_update_simulation_in_db, read_folders, read_rootfolders, read_rootfolders_by_domain_and_initials
from .testdata_for_import import InMemoryFolderNode, generate_in_memory_rootfolder_and_folder_hierarchies


class BaseIntegrationTest:

    # ---------- helper functions  ----------    
    def get_marked_for_cleanup(self, session: Session, rootfolder: RootFolderDTO) -> list[tuple[RootFolderDTO, FolderNodeDTO]]:
        pass

    # ---------- process steps to be implemented in the test ----------    
    def setup_new_db_with_vts_metadata(self, session: Session) -> None:
        """Step 1: Set up a new database with VTS metadata"""
        # Implementation must validate that one metadata records are present in the db
        insert_vts_metadata_in_db(session)

    def insert_simulations(self, session: Session, simulation_data: list[tuple[RootFolderDTO, FileInfo]]) -> list[tuple[RootFolderDTO, FolderNodeDTO]]:
        # Step 2.2: Insert simulations into database and return all folders and rootfolders in the database for validation

        #fornow we handle one root folder
        rootfolder: RootFolderDTO = simulation_data[0][0]        
        assert rootfolder.id is not None and rootfolder.id > 0
        simulations: list[FileInfo] = [fileinfo for _, fileinfo in simulation_data]
        insert_or_update_simulation_in_db(rootfolder.id, simulations)
        
        #get all rootfolders and folders in the db for validation
        rootfolder: List[FolderNodeDTO] = read_rootfolders_by_domain_and_initials(rootfolder.simulationdomain_id)[0]
        folders: List[FolderNodeDTO] = read_folders(rootfolder.id)
        return [(rootfolder, folders)]

    def update_cleanup_configuration(self, session: Session, rootfolder: RootFolderDTO, cleanup_configuration: CleanupConfiguration) -> CleanupConfiguration:
        """Step 3: Update the CleanupConfiguration and return the new configuration for a root folder and return the updated configuration"""
        # Implementation would create cleanup round
        pass
        return self.get_cleanup_configuration(session, rootfolder)

    def start_cleanup_round(self, session: Session, rootfolder: RootFolderDTO) -> list[tuple[RootFolderDTO, FolderNodeDTO]]:
        """Step 3: Initialize a multi-week cleanup round and return simulations Marked for cleanup"""
        # Implementation would create cleanup round
        pass
        return self.get_marked_for_cleanup(session, rootfolder)
    
    def update_retention_during_cleanup(self, session: Session, retention_changes: Dict) -> None:
        """Step 4: Update retention policies during active cleanup"""
        # Implementation would modify retention policies
        pass
    
    def update_simulations_during_cleanup(self, session: Session, simulation_updates: Dict) -> None:
        """Step 5: Update simulations during active cleanup round"""
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
