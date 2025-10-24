from datetime import date, datetime, timedelta, timezone
from sqlmodel import Session, func, select
from fastapi import HTTPException
from db.database import Database
from db.db_api import insert_or_update_simulations_in_db, read_folders_marked_for_cleanup
from datamodel.dtos import FolderNodeDTO, RootFolderDTO, FileInfo
from cleanup_cycle.cleanup_dtos import AgentInfo, CleanupCalendarDTO, CleanupTaskDTO, TaskStatus, CalendarStatus, CleanupConfigurationDTO, ActionType, CleanupProgress


class CleanupScheduler:
    # scheduler functions
    @staticmethod
    def update_calendar_and_verify_tasks(session: Session, calendar: CleanupCalendarDTO, tasks: list[CleanupTaskDTO]) -> tuple[int, int, int]:

        calendars_completed = 0
        calendars_failed = 0
        tasks_failed_timeout = 0

        if len(tasks) == 0:
            # No tasks for this calendar - this shouldn't happen, but mark as failed
            calendar.status = CalendarStatus.FAILED
            session.add(calendar)
            calendars_failed += 1

        
        # Check if any task has failed
        failed_tasks = [t for t in tasks if t.status == TaskStatus.FAILED.value]
        if failed_tasks:
            # If any task failed, mark the calendar as FAILED
            calendar.status = CalendarStatus.FAILED
            session.add(calendar)
            calendars_failed += 1
        

        # Check for reserved tasks that have exceeded their max execution time
        reserved_tasks = [task for task in tasks if task.status == TaskStatus.RESERVED.value and task.reserved_at]
        now = datetime.now(timezone.utc)
        for task in reserved_tasks:
            # Check if execution time has been exceeded
            execution_duration = now - task.reserved_at
            max_duration = timedelta(hours=task.max_execution_hours)
            
            if execution_duration > max_duration:
                # Mark task as failed due to timeout
                task.status = TaskStatus.FAILED.value
                task.status_message = f"Task exceeded maximum execution time of {task.max_execution_hours} hours"
                task.completed_at = now
                session.add(task)
                tasks_failed_timeout += 1
                
                # Mark calendar as failed too
                calendar.status = CalendarStatus.FAILED
                session.add(calendar)
                calendars_failed += 1
        
        # Check if all tasks are completed
        all_completed = all(t.status == TaskStatus.COMPLETED.value for t in tasks)
        if all_completed:
            calendar.status = CalendarStatus.COMPLETED
            session.add(calendar)
            calendars_completed += 1

        return calendars_completed, calendars_failed, tasks_failed_timeout

    @staticmethod
    def activate_planned_tasks(session: Session, calendar: CleanupCalendarDTO, tasks: list[CleanupTaskDTO]) -> int:
        tasks_activated = 0

        # Activate PLANNED tasks if ready
        # Logic: Activate tasks only when:
        #   1. The scheduled date (start_date + days_offset) has been reached or exceeded
        #   2. All previous tasks in the calendar are completed (sequential execution)
        planned_tasks = [t for t in tasks if t.status == TaskStatus.PLANNED.value]
        today = date.today()
        
        for task in planned_tasks:
            # Calculate the scheduled date for this task
            scheduled_date = calendar.start_date + timedelta(days=task.task_offset)
            
            # Check if the scheduled date has been reached
            if today < scheduled_date:
                # Not ready yet, skip this task
                continue
            
            # Check if all previous tasks are completed
            # Tasks are already ordered by days_offset, then by ID from the database query
            task_index = next((i for i, t in enumerate(tasks) if t.id == task.id), -1)
            
            if task_index > 0:
                # Check if all previous tasks are completed
                previous_tasks = tasks[:task_index]
                all_previous_completed = all(
                    t.status == TaskStatus.COMPLETED.value for t in previous_tasks
                )
                
                if not all_previous_completed:
                    # Previous tasks not done, skip this task
                    continue
            
            # All prerequisites met (date reached and previous tasks completed), activate the task
            task.status = TaskStatus.ACTIVATED.value
            session.add(task)
            tasks_activated += 1

        return tasks_activated
    

    # Check all rootfolders Active calendars periodically to verify:
    #  - are all tasks completed so that the calendars state can change to completed
    #  - can I activate the next task because the previous task is completed
    #  - do I need to change the status of the calendar to FAILED because a task failed
    @staticmethod
    def update_calendars_and_tasks() -> dict[str, any]:
        # Periodically check all active calendars and their tasks to:
        # 1. Activate PLANNED tasks that are ready for execution
        # 2. Check if RESERVED tasks have exceeded their max execution time and mark them as FAILED
        # 3. Mark calendars as COMPLETED when all their tasks are completed
        # 4. Mark calendars as FAILED if any of their tasks failed
        
        # A task transitions from PLANNED to ACTIVATED only when both conditions are met:
        # - The scheduled date has been reached or exceeded
        # - All previous tasks in the calendar are completed
        #Returns:
        #    Dictionary with summary of actions taken
        with Session(Database.get_engine()) as session:

            # Get all ACTIVE calendars and update their status before any task is activated
            active_calendars = session.exec(
                select(CleanupCalendarDTO).where(
                    CleanupCalendarDTO.status == CalendarStatus.ACTIVE
                )
            ).all()
            
            
            calendars_completed = 0
            calendars_failed = 0
            tasks_activated = 0
            task_timeouts = 0
            
            for calendar in active_calendars:

                # Get all tasks for this calendar, ordered by days_offset then by ID
                tasks = session.exec(
                    select(CleanupTaskDTO)
                    .where(CleanupTaskDTO.calendar_id == calendar.id)
                    .order_by(CleanupTaskDTO.task_offset, CleanupTaskDTO.id)
                ).all()

                if not tasks:
                    tasks = []

                completed, failed, task_timeouts = CleanupScheduler.update_calendar_and_verify_tasks(session, calendar, tasks)
                calendars_completed  += completed
                calendars_failed     += failed
                task_timeouts        += task_timeouts

                tasks_activated_ = CleanupScheduler.activate_planned_tasks(session, calendar, tasks)
                tasks_activated += tasks_activated_
            
            # Commit all changes
            session.commit()
            
            return {
                "message": f"Scheduler run completed",
                "calendars_processed": len(active_calendars),
                "calendars_completed": calendars_completed,
                "calendars_failed": calendars_failed,
                "tasks_activated": tasks_activated,
                "tasks_failed_timeout": task_timeouts
            }
        
    @staticmethod
    def generate_cleanup_calendar(config: CleanupConfigurationDTO) -> "CleanupCalendarDTO":
        calendar: CleanupCalendarDTO = None 
        with Session(Database.get_engine()) as session:
            rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == config.rootfolder_id)).first()
            if not rootfolder:
                raise HTTPException(status_code=404, detail="RootFolder not found")

            calendar = CleanupCalendarDTO(rootfolder_id=config.rootfolder_id,
                                            start_date=config.cleanup_start_date,
                                            status=CalendarStatus.ACTIVE.value)
            session.add(calendar)
            session.flush()  # Ensure calendar.id is available
            
            # Generate the tasks for this calendar based on ActionType enum
            
            # 1) START_RETENTION_REVIEW - Day 0 of STARTING_RETENTION_REVIEW phase
            # Internal CleanupProgress Agent: call cleanup_cycle_startInitialize. Takes less than 5 minutes
            task_start_retention_review = CleanupTaskDTO(
                calendar_id=calendar.id,
                rootfolder_id=config.rootfolder_id,
                path=rootfolder.path,
                task_offset=0,
                action_type=ActionType.START_RETENTION_REVIEW.value,
                storage_id=None,  # Internal action
                status=TaskStatus.PLANNED.value,
                max_execution_hours=1
            )
            session.add(task_start_retention_review)

            # 2) SEND_INITIAL_NOTIFICATION - 1 day into the RETENTION_REVIEW phase
            # Internal email agent: Notify stakeholders about the new retention review. Takes less than a minute
            task_send_initial_notification = CleanupTaskDTO(
                calendar_id=calendar.id,
                rootfolder_id=config.rootfolder_id,
                path=rootfolder.path,
                task_offset=1,
                action_type=ActionType.SEND_INITIAL_NOTIFICATION.value,
                storage_id=None,  # Internal action
                status=TaskStatus.PLANNED.value,
                max_execution_hours=1
            )
            session.add(task_send_initial_notification)

            # 3) SEND_FINAL_NOTIFICATION - About a week before end of RETENTION_REVIEW phase
            # Internal email agent: Notify stakeholders about the ongoing retention review. Takes less than a minute
            # Assuming RETENTION_REVIEW lasts for config.cleanupfrequency days, schedule 7 days before end
            retention_review_duration = config.cleanupfrequency if config.cleanupfrequency > 7 else 7
            task_send_final_notification = CleanupTaskDTO(
                calendar_id=calendar.id,
                rootfolder_id=config.rootfolder_id,
                path=rootfolder.path,
                task_offset=retention_review_duration - 7,
                action_type=ActionType.SEND_FINAL_NOTIFICATION.value,
                storage_id=None,  # Internal action
                status=TaskStatus.PLANNED.value,
                max_execution_hours=1
            )
            session.add(task_send_final_notification)

            # 4) SCAN_ROOTFOLDER - About 3 days before the end of the RETENTION_REVIEW phase
            # Storage agent: scan the rootfolder for simulations. Can take up to one day
            task_scan_rootfolder = CleanupTaskDTO(
                calendar_id=calendar.id,
                rootfolder_id=config.rootfolder_id,
                path=rootfolder.path,
                task_offset=retention_review_duration - 3,
                action_type=ActionType.SCAN_ROOTFOLDER.value,
                storage_id=rootfolder.storage_id,  
                status=TaskStatus.PLANNED.value,
                max_execution_hours=48
            )
            session.add(task_scan_rootfolder)

            # 5) CLEAN_ROOTFOLDER - 0 day into the CLEANING phase
            # Storage agent: clean marked simulations. Can take up to a day
            task_clean_rootfolder = CleanupTaskDTO(
                calendar_id=calendar.id,
                rootfolder_id=config.rootfolder_id,
                path=rootfolder.path,
                task_offset=retention_review_duration,  # Start of CLEANING phase
                action_type=ActionType.CLEAN_ROOTFOLDER.value,
                storage_id=rootfolder.storage_id,  
                status=TaskStatus.PLANNED.value,
                max_execution_hours=48
            )
            session.add(task_clean_rootfolder)

            # 6) FINISH_CLEANUP_CYCLE - After cleaning is complete
            # Internal CleanupProgress Agent: call cleanup_cycle_finishing to change the remaining marked retention to the next retention type
            task_finish_cleanup_cycle = CleanupTaskDTO(
                calendar_id=calendar.id,
                rootfolder_id=config.rootfolder_id,
                path=rootfolder.path,
                task_offset=retention_review_duration + 1,  # 1 day after cleaning starts
                action_type=ActionType.FINISH_CLEANUP_CYCLE.value,
                storage_id=None,  # Internal action
                status=TaskStatus.PLANNED.value,
                max_execution_hours=1
            )
            session.add(task_finish_cleanup_cycle)

            # 7) PREPARE_NEXT_CLEANUP_CYCLE - Final step
            # Internal CleanupProgress Agent: execute the last step in the cleanup cycle by calling prepare_next_cleanup_cycle
            task_prepare_next_cleanup_cycle = CleanupTaskDTO(
                calendar_id=calendar.id,
                rootfolder_id=config.rootfolder_id,
                path=rootfolder.path,
                task_offset=retention_review_duration + 2,  # 2 days after cleaning starts
                action_type=ActionType.PREPARE_NEXT_CLEANUP_CYCLE.value,
                storage_id=None,  # Internal action
                status=TaskStatus.PLANNED.value,
                max_execution_hours=1
            )
            session.add(task_prepare_next_cleanup_cycle)

            session.commit()
        return calendar

    @staticmethod
    def create_calendars_for_cleanup_configuration_ready_to_start() -> str:
        """
        Fetch all cleanup configurations that are ready to start a new cleanup cycle.
        
        Returns:
            List of CleanupConfigurationDTO objects that meet all criteria:
            - cycletime > 0
            - cleanupfrequency > 0
            - cleanup_start_date >= today
            - cleanup_progress in [INACTIVE, DONE]
        """
        today = date.today()
        
        with Session(Database.get_engine()) as session:
            configs = session.exec(
                select(CleanupConfigurationDTO).where(
                    (CleanupConfigurationDTO.cycletime > 0) &
                    (CleanupConfigurationDTO.cleanupfrequency > 0) &
                    (CleanupConfigurationDTO.cleanup_start_date != None) &
                    (CleanupConfigurationDTO.cleanup_start_date >= today) &
                    (CleanupConfigurationDTO.cleanup_progress.in_([
                        CleanupProgress.ProgressEnum.INACTIVE.value,
                        CleanupProgress.ProgressEnum.DONE.value
                    ]))
                )
            ).all()

            calendars:list[CleanupCalendarDTO] = []
            for config in configs:
                if config.can_start_cleanup_now():
                    calendars.append(CleanupScheduler.generate_cleanup_calendar(config))

            return f"Generated {len(calendars)} calendars for configurations ready to start."


class AgentInterfaceMethods:
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
                reserved_task.status = TaskStatus.RESERVED.value
                reserved_task.reserved_by_agent_id = agent.agent_id
                reserved_task.reserved_at = datetime.now(timezone.utc)
                session.add(reserved_task)
                session.commit()
                return reserved_task
            
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
            
            task.status = status
            task.status_message = status_message
            task.completed_at = datetime.now(timezone.utc)
            session.add(task)
            session.commit()
            return {"message": f"Task {task_id} updated to status {status}"}


    @staticmethod
    def task_insert_or_update_simulations_in_db(task_id: int, rootfolder_id: int, simulations: list[FileInfo]) -> dict[str, str]:
        #validate the task_id as a minimum security check
        with Session(Database.get_engine()) as session:
            task = session.exec(
                select(CleanupTaskDTO).where((CleanupTaskDTO.id == task_id))
            ).first()
            if not task:
                raise HTTPException(status_code=404, detail=f"The task with id {task_id} was not found")

        return insert_or_update_simulations_in_db(rootfolder_id, simulations)


    # return the list of full folder paths that are marked for cleanup in the given rootfolder
    @staticmethod
    def read_simulations_marked_for_cleanup(task_id: int, rootfolder_id: int) -> list[str]:
        #validate the task_id as a minimum security check
        with Session(Database.get_engine()) as session:
            task = session.exec(
                select(CleanupTaskDTO).where((CleanupTaskDTO.id == task_id))
            ).first()
            if not task:
                raise HTTPException(status_code=404, detail=f"The task with id {task_id} was not found")

        simulations:list[FolderNodeDTO] = read_folders_marked_for_cleanup(task_id, rootfolder_id)
        paths:list[str] = [folder.path for folder in simulations ]
        return paths