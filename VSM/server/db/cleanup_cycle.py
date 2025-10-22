from datetime import date
from sqlmodel import Session, func, select
from fastapi import Query, HTTPException
from db.database import Database
from datamodel.dtos import CleanupProgress, CleanupConfigurationDTO, RootFolderDTO, FolderNodeDTO, FolderTypeEnum
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
def cleanup_cycle_finishing(rootfolder_id: int ) -> dict[str, str]:
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
            raise HTTPException(status_code=400, detail="Failed to transition cleanup config")

        session.commit()
        return {"message": f"Finished cleanup cycle for rootfolder {rootfolder_id}"}

#this function will be called by the cleanup agents to update the marked folders after cleanup attempt
def cleanup_cycle_cleaning_done(rootfolder_id: int ) -> dict[str, str]:
    #Advance the cleanup startdate to today to ensure that the next round will be calculated from today
    with Session(Database.get_engine()) as session:
        rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        cleanup_config: CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session)
        if not cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.DONE:
            raise HTTPException(status_code=400, detail="RootFolder is not in DONE state")

        cleanup_config.cleanup_start_date = date.today()
        session.add(cleanup_config)
        session.commit()

    return {"message": f"Cleanup cycle cleaning done for rootfolder {rootfolder_id} new cleanup_start_date {cleanup_config.cleanup_start_date}"}

#check if som action needs to be taken depending on the current progress of the cleanup
# def run_cleanup_progress(rootfolder_id: int):
#     with Session(Database.get_engine()) as session:
#         rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
#         if not rootfolder:
#             raise HTTPException(status_code=404, detail="RootFolder not found")

#         cleanup_config: CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session)
#         if not cleanup_config:
#             raise HTTPException(status_code=404, detail="CleanupConfiguration not found")

#         # Depending on the current progress, take appropriate actions
#         if cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.STARTING_RETENTION_REVIEW:
#             # Perform actions for STARTING_RETENTION_REVIEW
#             # the start_new_cleanup_cycle function should have handled this state already
#             pass  # Placeholder for actual logic
#         elif cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.RETENTION_REVIEW:
#             # Perform actions for RETENTION_REVIEW
#             # we can progress to the next state when date.today() >= cleanup_start_date + cycletime
#             if date.today() >= cleanup_config.cleanup_start_date + cleanup_config.cycle_time:
#                 if not cleanup_config.transition_to(CleanupProgress.ProgressEnum.CLEANING):
#                     return {"message": f"For rootfolder {rootfolder_id}: failed to transition to CleanupProgress.ProgressEnum.CLEANING"}
            
#             pass  # Placeholder for actual logic
#         elif cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.CLEANING:
#             # Perform actions for CLEANING
#             # in this state we must call the cleaning agent with the folders to be cleaned for this rootfolder


#             # When the agent is done cleaning then it must call cleanup_cycle_cleaning_done to register what got cleaned. 
#             # This will also progress the state to FINISHING when all marked folders are processed
#             pass  # Placeholder for actual logic
#         elif cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.FINISHING:
#             # Perform actions for FINISHED: cleanup_cycle_finish
#             cleanup_cycle_finishing(rootfolder_id) #will also progress the state to DONE
#         elif cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.DONE:
#             # No actions needed for DONE
#             # we could call cleanup_cycle_start to start a new round. This is TBD
#             pass  # Placeholder for actual logic
#         else:
#             raise HTTPException(status_code=400, detail="Invalid cleanup progress state")

#         session.commit()
#         return {"message": f"Processed cleanup progress for rootfolder {rootfolder_id} at state {cleanup_config.cleanup_progress}"}