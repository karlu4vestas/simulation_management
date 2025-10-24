from datetime import timedelta
from sqlmodel import Session, select
from fastapi import HTTPException
from db.database import Database
from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeEnum
from cleanup_cycle.cleanup_dtos import CleanupProgress, CleanupConfigurationDTO
from datamodel.retention_validators import RetentionCalculator
from db.db_api import read_folder_type_dict_pr_domain_id, read_rootfolder_retentiontypes_dict, read_folders_marked_for_cleanup
 

# This function put the cleanup cycle into CleanupProgress.ProgressEnum.STARTING_RETENTION_REVIEW in order to recalcualte all numeric retentions
# This is also the only progress state where retentions can be marked for cleanup
# before exit it advances to CleanupProgress.ProgressEnum.RETENTION_REVIEW
def cleanup_cycle_start(rootfolder_id: int) -> dict[str, str]:
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

            # recalculate numeric retentions

            # extract all leafs with rootfolder_id.
            # We can possibly optimise by limiting the selection to folders with a numeric retentiontypes
            nodetype_leaf_id: int = read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)[FolderTypeEnum.VTS_SIMULATION].id
            folders = session.exec( select(FolderNodeDTO).where( (FolderNodeDTO.rootfolder_id == rootfolder_id) & \
                                                                 (FolderNodeDTO.nodetype_id == nodetype_leaf_id) ) ).all()

            # Update retentions 
            retention_calculator: RetentionCalculator = RetentionCalculator(read_rootfolder_retentiontypes_dict(rootfolder_id), cleanup_config)
            
            for folder in folders:          
                folder.set_retention( retention_calculator.adjust_from_cleanup_configuration_and_modified_date( folder.get_retention(), folder.modified_date) )
                session.add(folder)

            if not cleanup_config.transition_to(CleanupProgress.ProgressEnum.RETENTION_REVIEW):
                return {"message": f"For rootfolder {rootfolder_id}: failed to transition to CleanupProgress.ProgressEnum.RETENTION_REVIEW"}

            session.commit()

        return {"message": f"new cleanup cycle started for : {rootfolder_id}. updated retention of {len(folders)} folders" }

# This function will be when the cleanup round transition to finished
# The purpose is to set all folders marked to the next retentiontype
def cleanup_cycle_finishing(rootfolder_id: int) -> dict[str, str]:
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
            retention_calculator: RetentionCalculator = RetentionCalculator(read_rootfolder_retentiontypes_dict(rootfolder_id), cleanup_config) 
            retention_id_after_marked:int = retention_calculator.get_retention_id_after_marked()
            for folder in marked_simulations:
                folder.retention_id = retention_id_after_marked
                session.add(folder)

        if not cleanup_config.transition_to_next():
            raise HTTPException(status_code=400, detail=f"Failed to transition from {cleanup_config.cleanup_progress} to the next phase")

        session.add(cleanup_config)
        session.commit()
        return {"message": f"Finished cleanup cycle for rootfolder {rootfolder_id}"}

#this function will be called by the cleanup agents to update the marked folders after cleanup attempt
def cleanup_cycle_prepare_next_cycle(rootfolder_id: int) -> dict[str, str]:
    #Advance the cleanup startdate to today to ensure that the next round will be calculated from today
    with Session(Database.get_engine()) as session:
        rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        cleanup_config: CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session)
        if not cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.DONE:
            raise HTTPException(status_code=400, detail="RootFolder is not in DONE state")

        cleanup_config.cleanup_start_date = cleanup_config.cleanup_start_date + timedelta(days=cleanup_config.cleanupfrequency_days)

        if not cleanup_config.transition_to_next():
            raise HTTPException(status_code=400, detail=f"Failed to transition from {cleanup_config.cleanup_progress} to the next phase")
        session.add(cleanup_config)
        session.commit()

    return {"message": f"Cleanup cycle cleaning done for rootfolder {rootfolder_id} new cleanup_start_date {cleanup_config.cleanup_start_date}"}