from datetime import datetime, timezone
from sqlmodel import Session, func, select
from fastapi import HTTPException
from db.database import Database
from db import db_api
from datamodel import dtos
from cleanup import agent_db_interface
from cleanup.scheduler_dtos import TaskStatus, ActionType, AgentInfo, CleanupTaskDTO 
from cleanup.scheduler_db_actions import CleanupScheduler

class AgentTaskManager:
    
    #agent interface methods
    @staticmethod
    def reserve_task(agent: AgentInfo) -> CleanupTaskDTO:
        agent_action_types:list[str]     = agent.action_types if not agent.action_types is None else []
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
                
                # Generic state verification and transition logic
                if reserved_task.precondition_states is not None and len(reserved_task.precondition_states) > 0:
                    # Load the cleanup configuration to check current state
                    from cleanup.cleanup_dtos import CleanupState
                    cleanup_config = CleanupState.load_by_rootfolder_id(reserved_task.rootfolder_id)
                    
                    # Verify current state is one of the allowed precondition states
                    if cleanup_config.progress not in reserved_task.precondition_states:
                        raise HTTPException(
                            status_code=400, 
                            detail=f"Task {reserved_task.id} ({reserved_task.action_type}) requires one of states {reserved_task.precondition_states} but current state is {cleanup_config.progress}"
                        )
                    
                    # Transition to target state if required
                    if reserved_task.state_transition_on_reservation:
                        if not cleanup_config.transition_to(dtos.CleanupProgress.Progress(reserved_task.target_state)):
                            raise HTTPException(
                                status_code=400,
                                detail=f"Failed to transition from {cleanup_config.progress} to {reserved_task.target_state} for task {reserved_task.id}"
                            )
                        cleanup_config.save_to_db(session)

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

            # State verification at completion
            if status == TaskStatus.COMPLETED.value and task.state_verification_on_completion:
                if task.target_state is not None:
                    # Load the cleanup configuration to verify current state
                    from cleanup.cleanup_dtos import CleanupState
                    cleanup_config = CleanupState.load_by_rootfolder_id(task.rootfolder_id)
                    
                    # Verify we're still in the expected target state
                    if cleanup_config.progress != task.target_state:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Task {task_id} ({task.action_type}) expected state {task.target_state} at completion but current state is {cleanup_config.progress}"
                        )

            task.status = status
            task.status_message = status_message
            task.completed_at = datetime.now(timezone.utc)
            
            session.add(task)
            session.commit()

            CleanupScheduler.update_calendars_and_tasks() # prepare so the next task
            return {"message": f"Task {task_id} updated to status {status}"}
