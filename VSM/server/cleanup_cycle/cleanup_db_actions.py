from __future__ import annotations
from typing import TYPE_CHECKING
from datetime import timedelta
from sqlmodel import Session, select
from fastapi import HTTPException
from db.database import Database
from cleanup_cycle.cleanup_dtos import CleanupProgress, CleanupConfigurationDTO
from datamodel.retentions import RetentionCalculator
from db.db_api import read_folder_type_dict_pr_domain_id, read_folders_marked_for_cleanup
from db import db_api

if TYPE_CHECKING:
    from cleanup_cycle import scheduler_db_actions 

# This function put the cleanup cycle into CleanupProgress.ProgressEnum.STARTING_RETENTION_REVIEW in order to recalculate all numeric retentions
# This is also the only progress state where retentions can be marked for cleanup
# before exit it advances to CleanupProgress.ProgressEnum.RETENTION_REVIEW
def cleanup_cycle_start(rootfolder_id: int) -> dict[str, str]:
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeEnum
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        cleanup_config: CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session)
        if not cleanup_config.can_start_cleanup_now():
            return {"message": f"Unable to transition rootfolder {rootfolder_id} to CleanupProgress.ProgressEnum.STARTING_RETENTION_REVIEW with cleanup config {cleanup_config}"}
        else:
            if not cleanup_config.transition_to(CleanupProgress.ProgressEnum.STARTING_RETENTION_REVIEW):
                return {"message": f"For rootfolder {rootfolder_id}: cleanup_config.can_start_cleanup_now() is valid but start_new_cleanup_cycle is failed to transition to CleanupProgress.ProgressEnum.STARTING_RETENTION_REVIEW"}
            session.add(cleanup_config)
            session.commit()
            session.refresh(cleanup_config)

            # recalculate numeric retentions

            # extract all leafs with rootfolder_id.
            # We can possibly optimise by limiting the selection to folders with a numeric retentiontypes
            nodetype_leaf_id: int = read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)[FolderTypeEnum.SIMULATION].id
            folders = session.exec( select(FolderNodeDTO).where( (FolderNodeDTO.rootfolder_id == rootfolder_id) & \
                                                                 (FolderNodeDTO.nodetype_id == nodetype_leaf_id) ) ).all()
            
            # The following should no be necessary but just to be sure that faulty transactions did not miss any pathprotections
            db_api.apply_pathprotections(rootfolder_id) 

            # update retention in order to mark folders for cleanup if needed
            retention_calculator: RetentionCalculator = RetentionCalculator(rootfolder.id, rootfolder.cleanup_config_id, session)
            for folder in folders:
                folder.set_retention(retention_calculator.adjust_from_cleanup_configuration_and_modified_date(folder.get_retention(), folder.modified_date))
                session.add(folder)

            if not cleanup_config.transition_to(CleanupProgress.ProgressEnum.RETENTION_REVIEW):
                return {"message": f"For rootfolder {rootfolder_id}: failed to transition to CleanupProgress.ProgressEnum.RETENTION_REVIEW"}
            session.add(cleanup_config)

            session.commit()

        return {"message": f"new cleanup cycle started for : {rootfolder_id}. updated retention of {len(folders)} folders" }

# This function will be when the cleanup round transition to finished
# The purpose is to set all folders marked to the next retentiontype
def cleanup_cycle_finishing(rootfolder_id: int) -> dict[str, str]:
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeEnum
    # steps 
    # 1) verify that the rootfolder is in cleaning state
    # 2) if in cleaning state then unmarked remaining simulation in the rootfolder before trasitioning to CleanupProgress.ProgressEnum.DONE
    with Session(Database.get_engine()) as session:
        rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        cleanup_config: CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session)
        if not cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.CLEANING:
            raise HTTPException(status_code=400, detail="RootFolder is not in CLEANING state")
    
        marked_simulations:list[FolderNodeDTO] = read_folders_marked_for_cleanup(rootfolder_id)
        if len(marked_simulations) > 0:
            # change the retention to the next retention after marked
            retention_calculator: RetentionCalculator = RetentionCalculator(rootfolder.id, cleanup_config.id, session)

            retention_id_after_marked:int = retention_calculator.get_retention_id_after_marked()
            for folder in marked_simulations:
                folder.retention_id = retention_id_after_marked
                session.add(folder)

        if not cleanup_config.transition_to_next():
            raise HTTPException(status_code=400, detail=f"Failed to transition from {cleanup_config.cleanup_progress} to the next phase")

        session.add(cleanup_config)
        session.commit()
        return {"message": f"Finished cleanup cycle for rootfolder {rootfolder_id}"}

def register_cleanup_done(rootfolder_id: int) :
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeEnum
    with Session(Database.get_engine()) as session:
        rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")

        cleanup_configuration = rootfolder.get_cleanup_configuration(session)
        if not cleanup_configuration:
            raise HTTPException(status_code=404, detail="cleanup_configuration not found")

        #if not cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.CLEANING:
        #    raise HTTPException(status_code=400, detail="RootFolder is not in CLEANING state")
        if not cleanup_configuration.transition_to_next():
            raise HTTPException(status_code=400, detail=f"Failed to transition from {cleanup_configuration.cleanup_progress} to the next phase")

        session.add(cleanup_configuration)
        session.commit()


#this function will be called by the cleanup agents to update the marked folders after cleanup attempt
def cleanup_cycle_prepare_next_cycle(rootfolder_id: int, prepare_next_cycle_and_stop: bool=False) -> dict[str, str]:
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeEnum
    #Advance the cleanup startdate to today to ensure that the next round will be calculated from today
    with Session(Database.get_engine()) as session:
        rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        cleanup_config: CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session)
        #if not cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.DONE:
        if not cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.FINISHING:
            raise HTTPException(status_code=400, detail="RootFolder is not in FINISHING state")


        if not cleanup_config.transition_to_next():
            raise HTTPException(status_code=400, detail=f"Failed to transition from {cleanup_config.cleanup_progress} to the next phase")

        # cleanup_config.cleanup_start_date = None will make it impossible to go to next transition so it has to be done after transition_to_next.
        # We need prepare_next_cycle_and_stop=True this for the integration test
        if prepare_next_cycle_and_stop:
            cleanup_config.cleanup_start_date = None
        else:    
            cleanup_config.cleanup_start_date = cleanup_config.cleanup_start_date + timedelta(days=cleanup_config.cleanupfrequency)
        
        session.add(cleanup_config)
        session.commit()

        return {"message": f"Cleanup cycle cleaning done for rootfolder {rootfolder_id} new cleanup_start_date {cleanup_config.cleanup_start_date if cleanup_config.cleanup_start_date else 'None'}"}
    
#--------------------

def update_rootfolder_cleanup_configuration(rootfolder_id: int, cleanup_configuration: CleanupConfigurationDTO):
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeEnum
    is_valid = cleanup_configuration.is_valid()
    if not is_valid:
        raise HTTPException(status_code=404, detail=f"for rootfolder {rootfolder_id}: update of cleanup_configuration failed")

    #now the configuration is consistent
    with Session(Database.get_engine()) as session:
        rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")
      
        # NEW: Use ensure_cleanup_config to get or create CleanupConfigurationDTO
        config_dto = rootfolder.get_cleanup_configuration(session)
        # Update the DTO with values from the incoming dataclass
        # any change will reset the progress to INACTIVE and deactivate all active calender and tasks 
        config_dto.cycletime          = cleanup_configuration.cycletime
        config_dto.cleanupfrequency   = cleanup_configuration.cleanupfrequency
        config_dto.cleanup_start_date = cleanup_configuration.cleanup_start_date
        config_dto.cleanup_progress   = CleanupProgress.ProgressEnum.INACTIVE 
        #config_dto.cleanup_progress   = cleanup_configuration.cleanup_progress if cleanup_configuration.cleanup_progress is None else cleanup_configuration.cleanup_progress 
        rootfolder.save_cleanup_configuration(session, config_dto)

        #if cleanup_configuration.can_start_cleanup():
        #    print(f"Starting cleanup for rootfolder {rootfolder_id} with configuration {cleanup_configuration}")
        #    #from app.web_server_retention_api import start_new_cleanup_cycle  #avoid circular import
        #    #start_new_cleanup_cycle(rootfolder_id)
        return {"message": f"for rootfolder {rootfolder_id}: update of cleanup configuration {config_dto.id} "}

def get_cleanup_configuration_by_rootfolder_id(rootfolder_id: int)-> CleanupConfigurationDTO:
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeEnum
    with Session(Database.get_engine()) as session:
        rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")

        cleanup_configuration = rootfolder.get_cleanup_configuration(session)
        if not cleanup_configuration:
            raise HTTPException(status_code=404, detail="cleanup_configuration not found")

    return cleanup_configuration

#insert the cleanup configuration for a rootfolder and update the rootfolder to point to the cleanup configuration
def insert_cleanup_configuration(rootfolder_id:int, cleanup_config: CleanupConfigurationDTO):
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeEnum
    if (rootfolder_id is None) or (rootfolder_id == 0):
        raise HTTPException(status_code=404, detail="You must provide a valid rootfolder_id to create a cleanup configuration")
    
    with Session(Database.get_engine()) as session:
        #verify if the rootfolder already exists
        existing_rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(
                (RootFolderDTO.id == rootfolder_id)
            )).first()
        if not existing_rootfolder:
            raise HTTPException(status_code=404, detail=f"Failed to find rootfolder with id {rootfolder_id} to create a cleanup configuration")

        #verify if a cleanup configuration already exists for this rootfolder
        existing_cleanup_config:CleanupConfigurationDTO = session.exec(select(CleanupConfigurationDTO).where(
                (CleanupConfigurationDTO.rootfolder_id == rootfolder_id)
            )).first()
        if existing_cleanup_config:
            return existing_cleanup_config
        
        from cleanup_cycle.scheduler_db_actions import CleanupScheduler
        cleanup_config.rootfolder_id = rootfolder_id
        cleanup_config.rootfolder_id = rootfolder_id
        cleanup_config.cleanup_progress = CleanupProgress.ProgressEnum.INACTIVE
        CleanupScheduler.deactivate_calendar(cleanup_config.rootfolder_id)
        session.add(cleanup_config)
        session.commit()
        session.refresh(cleanup_config)
        existing_rootfolder.cleanup_config_id = cleanup_config.id
        session.add(existing_rootfolder)
        session.commit()

        if (cleanup_config.id is None) or (cleanup_config.id == 0):
            raise HTTPException(status_code=404, detail=f"Failed to provide cleanup configuration for rootfolder_id {rootfolder_id} with an id")
        return cleanup_config   

#--------------------    