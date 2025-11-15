from datetime import datetime, timezone
from sqlmodel import Session, func, select
from fastapi import HTTPException
from db.database import Database
from db import db_api
from datamodel import dtos
from cleanup_cycle import cleanup_db_actions
from cleanup_cycle.scheduler_dtos import TaskStatus, ActionType, AgentInfo, CleanupTaskDTO 
from cleanup_cycle.scheduler_db_actions import CleanupScheduler

class CleanupTaskManager:
    
    #agent interface methods
    @staticmethod
    def reserve_task(agent: AgentInfo) -> CleanupTaskDTO:
        agent_action_types:list[str] = agent.action_types if not agent.action_types is None else []
        agent_storage_ids:list[str]|None = agent.supported_storage_ids if not agent.supported_storage_ids is None else []

        if (len(agent_action_types)==0):
            raise HTTPException(status_code=404, detail=f"The agent {agent.agent_id} failed to provide the action types it can support ")

        with Session(Database.get_engine()) as session:
            tasks:list[CleanupTaskDTO]= []   
            if agent_storage_ids is not None and len(agent_storage_ids)>0:
                tasks = session.exec(
                    select(CleanupTaskDTO).where( (CleanupTaskDTO.status == TaskStatus.ACTIVATED.value) &
                        (CleanupTaskDTO.action_type.in_(agent_action_types)) &
                        (CleanupTaskDTO.storage_id.in_(agent_storage_ids))
                    )
                ).all()
            else:
                tasks = session.exec(
                    select(CleanupTaskDTO).where( (CleanupTaskDTO.status == TaskStatus.ACTIVATED.value) &
                        (CleanupTaskDTO.action_type.in_(agent_action_types)) &
                        (CleanupTaskDTO.storage_id == None)
                    )
                ).all()
        
            if len(tasks) == 0:
                return None
            else:
                reserved_task = tasks[0]
                #@TODO: hmm is this a hack or a the start of a better design ?
                if reserved_task.action_type == ActionType.CLEAN_ROOTFOLDER:
                    cleanup_db_actions.register_cleaning_start(reserved_task.rootfolder_id)

                reserved_task.status = TaskStatus.RESERVED.value
                reserved_task.reserved_by_agent_id = agent.agent_id
                reserved_task.reserved_at = datetime.now(timezone.utc)
                session.add(reserved_task)
                session.commit()
                session.refresh(reserved_task)  # Ensure all attributes are loaded before detaching
                return reserved_task

    @staticmethod
    def task_progress(task_id: str, progress_message: str|None = None) -> dict[str,str]:
        
        with Session(Database.get_engine()) as session:
            task = session.exec( select(CleanupTaskDTO).where((CleanupTaskDTO.id == task_id)) ).first()
            if not task:
                raise HTTPException(status_code=404, detail=f"The task with id {task_id} was not found")
            
            if task.status  != TaskStatus.RESERVED.value:
                raise HTTPException(status_code=400, detail=f"The task with id {task_id} is not in RESERVED state and cannot be updated to INPROGRESS") 

            task.status = TaskStatus.RESERVED.value
            task.status_message = progress_message
            session.add(task)
            session.commit()

            return {"message": f"Task {task_id} updated to status {task.status}"}

    @staticmethod
    def task_completion(task_id: int, status: str, status_message: str|None = None) -> dict[str,str]:
        # validate that status is valid
        if status not in [TaskStatus.COMPLETED.value, TaskStatus.FAILED.value]:
            raise HTTPException(status_code=404, detail=f"The task status {status} is not valid. Must be one of {[TaskStatus.COMPLETED.value, TaskStatus.FAILED.value]}")

        with Session(Database.get_engine()) as session:
            task = session.exec(
                select(CleanupTaskDTO).where((CleanupTaskDTO.id == task_id))
            ).first()
            if not task:
                raise HTTPException(status_code=404, detail=f"The task with id {task_id} was not found")

            if task.status not in [TaskStatus.RESERVED.value, TaskStatus.INPROGRESS.value]:
                raise HTTPException(status_code=400, detail=f"The task with id {task_id} is not valid. Must be one of {[TaskStatus.RESERVED.value, TaskStatus.INPROGRESS.value]}")

            task.status = status
            task.status_message = status_message
            task.completed_at = datetime.now(timezone.utc)
            
            #@TODO: hmm is this a hack or a the start of a better design ?
            if task.action_type == ActionType.CLEAN_ROOTFOLDER:
                cleanup_db_actions.register_cleanup_done(task.rootfolder_id)

            session.add(task)
            session.commit()

            CleanupScheduler.update_calendars_and_tasks() # prepare so the next task
            return {"message": f"Task {task_id} updated to status {status}"}


    @staticmethod
    def task_insert_or_update_simulations_in_db(task_id: int, simulations: list[dtos.FileInfo]) -> dict[str, str]:
        #validate the task_id as a minimum security check
        with Session(Database.get_engine()) as session:
            task = session.exec(
                select(CleanupTaskDTO).where((CleanupTaskDTO.id == task_id))
            ).first()
            if not task:
                raise HTTPException(status_code=404, detail=f"The task with id {task_id} was not found")

        return db_api.insert_or_update_simulations_in_db(task.rootfolder_id, simulations)


    # return the list of FileInfo objects for folders that are marked for cleanup in the given rootfolder
    @staticmethod
    def task_read_folders_marked_for_cleanup(task_id: int) -> list[dtos.FileInfo]:
        #validate the task_id as a minimum security check
        with Session(Database.get_engine()) as session:
            task = session.exec(
                select(CleanupTaskDTO).where((CleanupTaskDTO.id == task_id))
            ).first()
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
            file_infos:list[dtos.FileInfo] = [folder.get_fileinfo(session, nodetype_dict, retention_dict) for folder in simulations]
        return file_infos
