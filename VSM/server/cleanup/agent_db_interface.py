
from datetime import date, datetime
from cleanup import scheduler
from sqlmodel import Session, select
from fastapi import HTTPException
from db.database import Database
from datamodel import dtos, retentions
from db import db_api
from datamodel import dtos
from cleanup import cleanup_dtos, scheduler_dtos



def task_scan_insert_or_update_simulations_in_db(task_id: int, simulations: list[dtos.FileInfo]) -> dict[str, str]:
    #validate the task_id as a minimum security check
    with Session(Database.get_engine()) as session:
        task = session.exec(
            select(scheduler_dtos.CleanupTaskDTO).where((scheduler_dtos.CleanupTaskDTO.id == task_id))
        ).first()
        if not task:
            raise HTTPException(status_code=404, detail=f"The task with id {task_id} was not found")

    return db_api.insert_or_update_simulations_in_db(task.rootfolder_id, simulations)

def task_clean_insert_or_update_simulations_in_db(task_id: int, simulations: list[dtos.FileInfo]) -> dict[str, str]:
    return task_scan_insert_or_update_simulations_in_db(task_id, simulations) 

def task_read_folders_marked_for_cleanup(task_id: int) -> list[dtos.FileInfo]:
    # return the list of FileInfo objects for folders that are marked for cleanup in the given rootfolder
    with Session(Database.get_engine()) as session:
        task = session.exec( select(scheduler_dtos.CleanupTaskDTO).where((scheduler_dtos.CleanupTaskDTO.id == task_id)) ).first()
        if not task:
            raise HTTPException(status_code=404, detail=f"The task with id {task_id} was not found")

        # Get rootfolder to access simulationdomain_id
        rootfolder = session.exec(
            select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == task.rootfolder_id)
        ).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail=f"The rootfolder with id {task.rootfolder_id} was not found")

        # Get folder type dictionary (by name)
        nodetype_dict_by_name = db_api.read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)
        retention_dict_by_name = db_api.read_retentiontypes_dict_by_domain_id(rootfolder.simulationdomain_id)
        
        # Convert to ID-based dictionaries
        nodetype_dict  = {ft.id: ft for ft in nodetype_dict_by_name.values()}
        retention_dict = {rt.id: rt for rt in retention_dict_by_name.values()}

        # Get simulations marked for cleanup
        simulations:list[dtos.FolderNodeDTO] = db_api.read_folders_marked_for_cleanup(task.rootfolder_id)
        
        # Convert each FolderNodeDTO to FileInfo using get_fileinfo()
        file_infos:list[dtos.FileInfo] = [folder.get_fileinfo(nodetype_dict, retention_dict) for folder in simulations]
    return file_infos
