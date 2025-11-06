import pytest
from typing import Dict, List, Any, NamedTuple, Optional
from sqlmodel import Session
from datetime import date, datetime, timedelta
from datamodel.dtos import FolderNodeDTO, FolderTypeEnum, RootFolderDTO

from datamodel.vts_create_meta_data import insert_vts_metadata_in_db
from db.db_api import FileInfo, insert_or_update_simulations_in_db, read_folders, normalize_path, read_rootfolders_by_domain_and_initials
from .testdata_for_import import InMemoryFolderNode, RootFolderWithMemoryFolderTree, RootFolderWithMemoryFolders

class RootFolderWithFolderNodeDTOList(NamedTuple):
    """Named tuple for a root folder and its flattened list of folder nodes"""
    rootfolder: RootFolderDTO
    folders: list[FolderNodeDTO]

class BaseIntegrationTest:

    # ---------- process steps to be implemented in the test ----------    
    def setup_new_db_with_vts_metadata(self, session: Session) -> None:
        insert_vts_metadata_in_db(session)
        
    def insert_simulations(self, session: Session, rootfolder_with_folders: RootFolderWithMemoryFolders) -> RootFolderWithFolderNodeDTOList:
        #Step 2.2: Insert simulations into database and return all the rootfolder database folders for validation
        
        rootfolder:RootFolderDTO=rootfolder_with_folders.rootfolder
        assert rootfolder.id is not None and rootfolder.id > 0

        # extract and convert the leaves to FileInfo (the leaves  are the simulations) 
        file_info_list:list[FileInfo] = [ FileInfo( filepath=folder.path, modified_date=folder.modified_date, nodetype=FolderTypeEnum.VTS_SIMULATION, external_retention=folder.retention) 
                                            for folder in rootfolder_with_folders.folders if folder.is_leaf ]
       
        insert_or_update_simulations_in_db(rootfolder.id, file_info_list)

        # get all rootfolders and folders in the db for validation
        rootfolders: List[RootFolderDTO] = read_rootfolders_by_domain_and_initials(rootfolder.simulationdomain_id)
        rootfolders = [r for r in rootfolders if r.id == rootfolder.id] 
        assert len(rootfolders) == 1
        rootfolder: RootFolderDTO = rootfolders[0]

        folders: List[FolderNodeDTO] = read_folders(rootfolder.id)
        return RootFolderWithFolderNodeDTOList(rootfolder=rootfolder, folders=folders)
