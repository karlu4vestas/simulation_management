from datetime import date, datetime, timedelta, timezone
from sqlmodel import Session, func, select
from fastapi import HTTPException
from db.database import Database
from datamodel import dtos
from cleanup import cleanup_dtos, scheduler_dtos
from cleanup.scheduler_dtos import TaskStatus, CalendarStatus, ActionType, CleanupCalendarDTO, CleanupTaskDTO 

class CleanupScheduler:
    # Dynamic scheduler that creates tasks just-in-time (JIT) directly in ACTIVATED state.
    
    # Differences from CleanupScheduler:
    # - Tasks are created only when needed, not all upfront
    # - Tasks go directly to ACTIVATED state (no PLANNED state)
    # - Calendar tracks progress via next_task_index to know which task to create next
    # - Task sequence is defined in _get_task_definitions() method
    
    # Define the task sequence and their properties
    @staticmethod
    def _get_task_definitions(retention_review_duration: float) -> list[dict]:
        # Returns the sequence of tasks to be created for a cleanup calendar.
        # Each task definition contains the parameters needed to create a CleanupTaskDTO.        
        # Args:
        #     retention_review_duration: The duration of the retention review phase in days
        # Returns:
        #     List of task definition dictionaries with keys:
        #     - action_type: ActionType enum value
        #     - task_offset: Days from calendar start_date
        #     - needs_storage_id: Whether this task requires storage_id
        #     - max_execution_hours: Maximum hours for task execution
        #     - precondition_state: List of accepted states at reservation ("a") - at least one must match
        #     - target_state: State to transition to for work ("b")
        #     - state_transition_on_reservation: Whether to transition state when reserving
        #     - state_verification_on_completion: Whether to verify state at completion
        
        return [
            {
                "action_type": ActionType.SCAN_ROOTFOLDER.value,
                "task_offset": 0,
                "needs_storage_id": True,
                "max_execution_hours": 48,
                "precondition_states": [dtos.CleanupProgress.Progress.INACTIVE.value,dtos.CleanupProgress.Progress.DONE.value],
                "target_state": dtos.CleanupProgress.Progress.SCANNING.value,
                "state_transition_on_reservation": True,
                "state_verification_on_completion": True
            },
            {
                "action_type": ActionType.MARK_SIMULATIONS_FOR_REVIEW.value,
                "task_offset": 0,
                "needs_storage_id": False,
                "max_execution_hours": 1,
                "precondition_states": [dtos.CleanupProgress.Progress.SCANNING.value],
                "target_state": dtos.CleanupProgress.Progress.MARKING_FOR_RETENTION_REVIEW.value,
                "state_transition_on_reservation": True,
                "state_verification_on_completion": True
            },
            {   # using this task to transition from STARTING_RETENTION_REVIEW to RETENTION_REVIEW
                # this is fair because going into retention review would not be OK without notifying the users
                "action_type": ActionType.SEND_INITIAL_NOTIFICATION.value,
                "task_offset": 0,
                "needs_storage_id": False,
                "max_execution_hours": 1,
                "precondition_states": [dtos.CleanupProgress.Progress.MARKING_FOR_RETENTION_REVIEW.value],
                "target_state": dtos.CleanupProgress.Progress.RETENTION_REVIEW.value,  # No change
                "state_transition_on_reservation": True,
                "state_verification_on_completion": True
            },
            {
                "action_type": ActionType.SEND_FINAL_NOTIFICATION.value,
                "task_offset": 0, #max(retention_review_duration,retention_review_duration - 7),
                "needs_storage_id": False,
                "max_execution_hours": 1,
                "precondition_states": [dtos.CleanupProgress.Progress.RETENTION_REVIEW.value],
                "target_state": dtos.CleanupProgress.Progress.RETENTION_REVIEW.value,  # No change
                "state_transition_on_reservation": False,
                "state_verification_on_completion": True
            },
            {
                "action_type": ActionType.CLEAN_ROOTFOLDER.value,
                "task_offset": 0, #retention_review_duration,
                "needs_storage_id": True,
                "max_execution_hours": 48,
                "precondition_states": [dtos.CleanupProgress.Progress.RETENTION_REVIEW.value],
                "target_state": dtos.CleanupProgress.Progress.CLEANING.value,
                "state_transition_on_reservation": True,  
                "state_verification_on_completion": True
            },
            {
                "action_type": ActionType.UNMARK_SIMULATIONS_AFTER_REVIEW.value,
                "task_offset": 0, #max(retention_review_duration ),#, retention_review_duration + 1),
                "needs_storage_id": False,
                "max_execution_hours": 1,
                "precondition_states": [dtos.CleanupProgress.Progress.CLEANING.value],
                "target_state": dtos.CleanupProgress.Progress.UNMARKING_AFTER_REVIEW.value,
                "state_transition_on_reservation": True,  
                "state_verification_on_completion": True
            },
            {
                "action_type": ActionType.FINALISE_CLEANUP_CYCLE.value,
                "task_offset": 0, #max(retention_review_duration ),#, retention_review_duration + 1),
                "needs_storage_id": False,
                "max_execution_hours": 1,
                "precondition_states": [dtos.CleanupProgress.Progress.UNMARKING_AFTER_REVIEW.value],
                "target_state": dtos.CleanupProgress.Progress.DONE.value,
                "state_transition_on_reservation": True,
                "state_verification_on_completion": True
            }        
            ]

    @staticmethod
    def create_calendars_for_cleanup_configuration_ready_to_start() -> str:
        # Fetch all rootfolder cleanup configurations that are ready to start a new cleanup cycle.
        # that includes verify that there are no active calendars for the rootfolder.
        # Creates calendars but NO tasks upfront - tasks are created JIT.        
        # Returns:
        #     String message indicating how many calendars were generated

        now = datetime.now()
        
        with Session(Database.get_engine()) as session:
            configs = session.exec( select(dtos.CleanupConfigurationDTO).where(
                    (dtos.CleanupConfigurationDTO.leadtime > 0) &
                    (dtos.CleanupConfigurationDTO.frequency > 0) &
                    (dtos.CleanupConfigurationDTO.start_date != None) &
                    (dtos.CleanupConfigurationDTO.start_date <= now) &
                    (dtos.CleanupConfigurationDTO.progress.in_([
                        dtos.CleanupProgress.Progress.INACTIVE.value,
                        dtos.CleanupProgress.Progress.DONE.value
                    ]))
                )
            ).all()

            state_configs:list[cleanup_dtos.CleanupState] = [cleanup_dtos.CleanupState(config) for config in configs]
            calendars:list[scheduler_dtos.CleanupCalendarDTO] = []
            for config in state_configs:
                # Verify that there are no active calendars for this rootfolder
                active_calendar = session.exec( select(CleanupCalendarDTO).where(
                        (CleanupCalendarDTO.rootfolder_id == config.dto.rootfolder_id) &
                        (CleanupCalendarDTO.status == CalendarStatus.ACTIVE.value))).first()
                # Skip this config if an active calendar already exists
                if active_calendar:
                    continue
                
                if config.can_start_cleanup_now():
                    if config.progress == dtos.CleanupProgress.Progress.DONE.value:
                        config.dto.start_date = datetime.now()
                        config.save_to_db(session)
                    calendar:scheduler_dtos.CleanupCalendarDTO = CleanupScheduler.generate_cleanup_calendar(config)    
                    calendars.append(calendar)

            return f"Generated or found {len(calendars)} calendars (JIT mode)."
    
    @staticmethod
    def generate_cleanup_calendar(config: cleanup_dtos.CleanupState, stop_after_cleanup_cycle: bool=False) -> "CleanupCalendarDTO":
        # Generate a cleanup calendar WITHOUT pre-creating tasks.
        # Tasks will be created just-in-time by update_calendars_and_tasks().

        with Session(Database.get_engine()) as session:
            # Check if there is already an active calendar for this rootfolder
            calendar: CleanupCalendarDTO = session.exec( select(CleanupCalendarDTO).where(
                    (CleanupCalendarDTO.rootfolder_id == config.dto.rootfolder_id) & 
                    (CleanupCalendarDTO.status == CalendarStatus.ACTIVE.value) ) ).first()
            if calendar is not None:
                return calendar

            # No active calendar so go ahead
            rootfolder = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == config.dto.rootfolder_id)).first()
            if not rootfolder:
                raise HTTPException(status_code=404, detail="RootFolder not found")

            # Reload the config in the current session
            config_dto = session.exec(select(dtos.CleanupConfigurationDTO).where(
                dtos.CleanupConfigurationDTO.id == config.dto.id
            )).first()
            if not config_dto:
                raise HTTPException(status_code=404, detail="CleanupConfiguration not found")

            # Create calendar only - NO tasks created here
            calendar: CleanupCalendarDTO = CleanupCalendarDTO(
                rootfolder_id=config_dto.rootfolder_id,
                start_date=config_dto.start_date,
                status=CalendarStatus.ACTIVE.value
            )
            session.add(calendar)
            session.commit()
            session.refresh(calendar)
            return calendar

    @staticmethod
    def update_calendars_and_tasks() -> dict[str, any]:
        # Periodically check all active calendars and create/activate tasks just-in-time.
        # JIT task creation logic:
        # 1. Check if the last task is completed
        # 2. If yes, create the next task directly in ACTIVATED state
        # 3. Check RESERVED tasks for timeouts
        # 4. Mark calendar as COMPLETED when all tasks in sequence are done        
        # Returns:
        #     Dictionary with summary of actions taken

        with Session(Database.get_engine()) as session:
            # Get all ACTIVE calendars
            active_calendars = session.exec( select(CleanupCalendarDTO).where( CleanupCalendarDTO.status == CalendarStatus.ACTIVE ) ).all()
            calendars_completed = 0
            calendars_failed = 0
            tasks_created = 0
            task_timeouts = 0
            
            for calendar in active_calendars:
                # Get all existing tasks for this calendar
                tasks = session.exec( select(CleanupTaskDTO)
                    .where(CleanupTaskDTO.calendar_id == calendar.id)
                    .order_by(CleanupTaskDTO.id)  # Order by creation time
                ).all()

                # Check calendar and task status
                completed, failed, timeouts = CleanupScheduler.update_calendar_and_verify_tasks( session, calendar, tasks )
                calendars_completed += completed
                calendars_failed += failed
                task_timeouts += timeouts

                # Try to create next task if calendar is still active
                if calendar.status == CalendarStatus.ACTIVE.value:
                    created = CleanupScheduler.create_next_task_if_ready(session, calendar, tasks)
                    tasks_created += created
            
            # Commit all changes
            session.commit()
            
            return {
                "message": f"Scheduler run completed (JIT mode)",
                "calendars_processed": len(active_calendars),
                "calendars_completed": calendars_completed,
                "calendars_failed": calendars_failed,
                "tasks_created": tasks_created,
                "tasks_failed_timeout": task_timeouts
            }

    @staticmethod
    def update_calendar_and_verify_tasks(session: Session, calendar: CleanupCalendarDTO, tasks: list[CleanupTaskDTO]) -> tuple[int, int, int]:
        # Verify task statuses and update calendar status accordingly.        
        # Returns:
        #    tuple: (calendars_completed, calendars_failed, tasks_failed_timeout)
        calendars_completed = 0
        calendars_failed = 0
        tasks_failed_timeout = 0

        # Check if any task has failed
        failed_tasks = [t for t in tasks if t.status == TaskStatus.FAILED.value]
        if failed_tasks:
            calendar.status = CalendarStatus.FAILED
            session.add(calendar)
            calendars_failed += 1
            return calendars_completed, calendars_failed, tasks_failed_timeout

        # Check for reserved tasks that have exceeded their max execution time
        reserved_tasks = [task for task in tasks if task.status == TaskStatus.RESERVED.value and task.reserved_at]
        now = datetime.now(timezone.utc)
        for task in reserved_tasks:
            execution_duration = now - task.reserved_at
            max_duration = timedelta(hours=task.max_execution_hours)
            
            if execution_duration > max_duration:
                task.status = TaskStatus.FAILED.value
                task.status_message = f"Task exceeded maximum execution time of {task.max_execution_hours} hours"
                task.completed_at = now
                session.add(task)
                tasks_failed_timeout += 1
                
                # Mark calendar as failed too
                calendar.status = CalendarStatus.FAILED
                session.add(calendar)
                calendars_failed += 1

        # Get the rootfolder configuration to determine total expected tasks
        config = session.exec( select(dtos.CleanupConfigurationDTO).where(
                dtos.CleanupConfigurationDTO.rootfolder_id == calendar.rootfolder_id ) ).first()
        
        if config:
            retention_review_duration = config.frequency if config.frequency > 0 else 7.0
            task_definitions = CleanupScheduler._get_task_definitions(retention_review_duration)
            expected_task_count = len(task_definitions)
            
            # Check if all expected tasks are completed
            completed_task_count = len([t for t in tasks if t.status == TaskStatus.COMPLETED.value])
            if completed_task_count >= expected_task_count:
                calendar.status = CalendarStatus.COMPLETED
                session.add(calendar)
                calendars_completed += 1

        return calendars_completed, calendars_failed, tasks_failed_timeout

    @staticmethod
    def create_next_task_if_ready(session: Session, calendar: CleanupCalendarDTO, existing_tasks: list[CleanupTaskDTO]) -> int:
        """
        Create the next task in the sequence if conditions are met.
        
        Conditions:
        1. Previous task (if any) must be COMPLETED
        2. Scheduled date for next task must be reached
        3. Next task doesn't already exist
        
        Returns:
            int: 1 if task was created, 0 otherwise
        """
        # Get rootfolder and config
        rootfolder = session.exec( select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == calendar.rootfolder_id) ).first()
        
        config = session.exec( select(dtos.CleanupConfigurationDTO).where(
                dtos.CleanupConfigurationDTO.rootfolder_id == calendar.rootfolder_id )).first()
        
        if not rootfolder or not config:
            return 0

        # Get task definitions
        retention_review_duration = config.frequency if config.frequency > 0 else 7.0
        task_definitions = CleanupScheduler._get_task_definitions(retention_review_duration)
        
        # Determine which task to create next (based on how many tasks exist)
        next_task_index = len(existing_tasks)
        
        # Check if we've created all tasks already
        if next_task_index >= len(task_definitions):
            return 0
        
        # Check if previous task is completed (or this is the first task)
        if next_task_index > 0:
            last_task = existing_tasks[-1]
            if last_task.status != TaskStatus.COMPLETED.value:
                return 0  # Wait for previous task to complete
        
        # Get the next task definition
        next_task_def = task_definitions[next_task_index]
        
        # Check if scheduled date has been reached
        scheduled_date = calendar.start_date + timedelta(days=next_task_def["task_offset"])
        now = datetime.now()
        if now < scheduled_date:
            return 0  # Not time yet
        
        # Create the task directly in ACTIVATED state
        new_task = CleanupTaskDTO(
            calendar_id=calendar.id,
            rootfolder_id=calendar.rootfolder_id,
            path=rootfolder.path,
            task_offset=next_task_def["task_offset"],
            action_type=next_task_def["action_type"],
            storage_id=rootfolder.storage_id if next_task_def["needs_storage_id"] else None,
            status=TaskStatus.ACTIVATED.value,
            max_execution_hours=next_task_def["max_execution_hours"],
            # State management fields
            precondition_states=next_task_def["precondition_states"],
            target_state=next_task_def["target_state"],
            state_transition_on_reservation=next_task_def["state_transition_on_reservation"],
            state_verification_on_completion=next_task_def["state_verification_on_completion"]
        )
        
        session.add(new_task)
        print(f"Created JIT task {next_task_def['action_type']} for calendar {calendar.id}")
        return 1
    
    @staticmethod
    def deactivate_calendar(rootfolder_id: int) -> None:
        # Deactivate all the rootfolder active calendars and their tasks for a given rootfolder.
        with Session(Database.get_engine()) as session:
            # Get all active calendars for this rootfolder
            active_calendars = session.exec(
                select(CleanupCalendarDTO).where(
                    (CleanupCalendarDTO.rootfolder_id == rootfolder_id) &
                    (CleanupCalendarDTO.status == CalendarStatus.ACTIVE.value)
                )
            ).all()
            
            for calendar in active_calendars:
                # Mark calendar as INTERRUPTED
                calendar.status = CalendarStatus.INTERRUPTED.value
                session.add(calendar)
                
                # Get all non-terminal tasks for this calendar
                tasks = session.exec(
                    select(CleanupTaskDTO).where(
                        (CleanupTaskDTO.calendar_id == calendar.id) &
                        (CleanupTaskDTO.status.in_([
                            TaskStatus.ACTIVATED.value,  # JIT mode uses ACTIVATED, not PLANNED
                            TaskStatus.RESERVED.value,
                            TaskStatus.INPROGRESS.value
                        ]))
                    )
                ).all()
                
                # Mark all non-terminal tasks as FAILED
                for task in tasks:
                    task.status = TaskStatus.FAILED.value
                    task.status_message = "Task cancelled due to calendar deactivation"
                    task.completed_at = datetime.now(timezone.utc)
                    session.add(task)
            
            session.commit()
