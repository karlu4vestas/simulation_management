
from datetime import date, datetime
from sqlmodel import Session, select
from fastapi import HTTPException
from db.database import Database
from datamodel import dtos
from datamodel.retentions import RetentionCalculator
from db import db_api
from datamodel import dtos
from cleanup_cycle import cleanup_dtos, scheduler_dtos, scheduler_db_actions



# def close_finished_calenders() -> None:
#     scheduler_db_actions.CleanupScheduler.close_finished_calenders()

def cleanup_cycle_start(rootfolder_id: int) -> dict[str, str]:
    #start the cleanup cycle by recalculating retentions for all leaf folders in the rootfolder

    cleanup_config: cleanup_dtos.CleanupState = cleanup_dtos.CleanupState.load_by_rootfolder_id(rootfolder_id)
    if not cleanup_config.dto.cleanup_progress in [dtos.CleanupProgress.ProgressEnum.INACTIVE.value, dtos.CleanupProgress.ProgressEnum.DONE.value]:
        raise HTTPException(status_code=400, detail=f"The state of the cleanup_configuration was in {cleanup_config.cleanup_progress} but should have been in INACTIVE or DONE")

    if not cleanup_config.can_start_cleanup_now():
        raise HTTPException(status_code=400, detail=f"The rootfolder {rootfolder_id} CleanupConfiguration is not ready for STARTING_RETENTION_REVIEW: {cleanup_config}")

    if not cleanup_config.transition_to(dtos.CleanupProgress.ProgressEnum.STARTING_RETENTION_REVIEW):
        raise HTTPException(status_code=400, detail=f"Failed to transition from {cleanup_config.cleanup_progress} to {dtos.CleanupProgress.ProgressEnum.STARTING_RETENTION_REVIEW.value}")
    
    cleanup_config.save_to_db() #save the state so the RetentionCalculator loads the upto date data

    db_api.apply_pathprotections(rootfolder_id)  # ThIS should noT be necessary but just to be sure that faulty transactions did not miss any pathprotections
    
    len_folders:int = 0
    with Session(Database.get_engine()) as session:
        rootfolder:dtos.RootFolderDTO = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")

        # recalculate numeric retentions for all simulations
        # @TODO This can possibly be optimise by limiting the selection to folders with a numeric retentiontypes
        nodetype_leaf_id: int = db_api.read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)[dtos.FolderTypeEnum.SIMULATION].id
        folders = session.exec( select(dtos.FolderNodeDTO).where( (dtos.FolderNodeDTO.rootfolder_id == rootfolder_id) & \
                                                                  (dtos.FolderNodeDTO.nodetype_id == nodetype_leaf_id) ) ).all()
        
        # update retention: This also mark simulations for cleanup if they are ready
        retention_calculator: RetentionCalculator = RetentionCalculator(rootfolder_id, cleanup_config.dto.id, session)
        for folder in folders:
            folder.set_retention(retention_calculator.adjust_from_cleanup_configuration_and_modified_date(folder.get_retention(), folder.modified_date))
            session.add(folder)

        if not cleanup_config.transition_to(dtos.CleanupProgress.ProgressEnum.RETENTION_REVIEW):
            raise HTTPException(status_code=400, detail=f"Failed to transition from {cleanup_config.cleanup_progress} to {dtos.CleanupProgress.ProgressEnum.RETENTION_REVIEW.value}")
        
        session.commit()
        len_folders = len(folders)

    cleanup_config.save_to_db()
    return {"message": f"new cleanup cycle started for : {rootfolder_id}. updated retention of {len_folders} folders" }

def register_cleaning_start(rootfolder_id: int) :
    #change to CLEANING state. An cleaning agent might run for a long time (+5 hours) to clean the marked simulationa
    cleanup_config: cleanup_dtos.CleanupState = cleanup_dtos.CleanupState.load_by_rootfolder_id(rootfolder_id)
    if not cleanup_config or not cleanup_config.cleanup_progress == dtos.CleanupProgress.ProgressEnum.RETENTION_REVIEW.value:
        raise HTTPException(status_code=404, detail=f"The state of the cleanup_configuration was in {cleanup_config.cleanup_progress} but should have been in RETENTION_REVIEW")
                                
    if not cleanup_config.transition_to(dtos.CleanupProgress.ProgressEnum.CLEANING):
        raise HTTPException(status_code=400, detail=f"Failed to transition from {cleanup_config.cleanup_progress} to {dtos.CleanupProgress.ProgressEnum.CLEANING.value}")
    
    cleanup_config.save_to_db()


def register_cleanup_done(rootfolder_id: int) :
    #called when the agent is done cleaning all marked simulations, transition to FINISHING state
    cleanup_config: cleanup_dtos.CleanupState = cleanup_dtos.CleanupState.load_by_rootfolder_id(rootfolder_id)
    if not cleanup_config or cleanup_config.cleanup_progress != dtos.CleanupProgress.ProgressEnum.CLEANING.value:
        raise HTTPException(status_code=404, detail=f"The state of the cleanup_configuration was in {cleanup_config.cleanup_progress} but should have been in CLEANING")

    if not cleanup_config.transition_to(dtos.CleanupProgress.ProgressEnum.FINISHING):
        raise HTTPException(status_code=400, detail=f"Failed to transition from {cleanup_config.cleanup_progress} to {dtos.CleanupProgress.ProgressEnum.FINISHING.value}")

    cleanup_config.save_to_db()


def cleanup_cycle_finishing(rootfolder_id: int) -> dict[str, str]:
    # unmarked remaining simulation in the rootfolder, if any, before transitioning to CleanupProgress.ProgressEnum.DONE

    cleanup_config: cleanup_dtos.CleanupState = cleanup_dtos.CleanupState.load_by_rootfolder_id(rootfolder_id)
    if not cleanup_config.cleanup_progress == dtos.CleanupProgress.ProgressEnum.FINISHING.value:
        raise HTTPException(status_code=404, detail=f"The state of the cleanup_configuration was in {cleanup_config.cleanup_progress} but should have been in FINISHING")
    
    with Session(Database.get_engine()) as session:
        marked_simulations:list[dtos.FolderNodeDTO] = db_api.read_folders_marked_for_cleanup(rootfolder_id)
        if len(marked_simulations) > 0:
            # change the retention to the next retention after marked
            retention_calculator: RetentionCalculator = RetentionCalculator(rootfolder_id, cleanup_config.id, session)
            after_marked_retention_id:int             = retention_calculator.get_retention_id_after_marked()
            for folder in marked_simulations:
                folder.retention_id = after_marked_retention_id # thois that we not clean will marked for the next cleanup round
                session.add(folder)

        if not cleanup_config.transition_to( dtos.CleanupProgress.ProgressEnum.DONE):
            raise HTTPException(status_code=400, detail=f"Failed to transition from {cleanup_config.cleanup_progress} to {dtos.CleanupProgress.ProgressEnum.FINISHING.value}")
        session.commit()

    cleanup_config.save_to_db()
    return {"message": f"Finished cleanup cycle for rootfolder {rootfolder_id}"}
    
#this function will be called by the cleanup agents to update the marked folders after cleanup attempt
# def cleanup_cycle_prepare_next_cycle(rootfolder_id: int, prepare_next_cycle_and_stop: bool=False) -> dict[str, str]:
#     #Advance the cleanup startdate to today to ensure that the next round will be calculated from today
#     with Session(Database.get_engine()) as session:
#         rootfolder:dtos.RootFolderDTO = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
#         if not rootfolder:
#             raise HTTPException(status_code=404, detail="RootFolder not found")

#         cleanup_config: cleanup_dtos.CleanupState = cleanup_dtos.CleanupState(rootfolder.get_cleanup_configuration(session))
#         #if not cleanup_config.cleanup_progress == CleanupProgress.ProgressEnum.DONE:
#         if not cleanup_config.dto.cleanup_progress == dtos.CleanupProgress.ProgressEnum.FINISHING:
#             raise HTTPException(status_code=400, detail="RootFolder is not in FINISHING state")


#         if not cleanup_config.transition_to_next():
#             raise HTTPException(status_code=400, detail=f"Failed to transition from {cleanup_config.dto.cleanup_progress} to the next phase")

#         # cleanup_config.cleanup_start_date = None will make it impossible to go to next transition so it has to be done after transition_to_next.
#         # We need prepare_next_cycle_and_stop=True this for the integration test
#         if prepare_next_cycle_and_stop:
#             cleanup_config.dto.cleanup_start_date = None
#         else:    
#             cleanup_config.dto.cleanup_start_date = cleanup_config.dto.cleanup_start_date + timedelta(days=cleanup_config.dto.cleanupfrequency)
        
#         session.add(cleanup_config.dto)
#         session.commit()

#         return {"message": f"Cleanup cycle cleaning done for rootfolder {rootfolder_id} new cleanup_start_date {cleanup_config.dto.cleanup_start_date if cleanup_config.dto.cleanup_start_date else 'None'}"}