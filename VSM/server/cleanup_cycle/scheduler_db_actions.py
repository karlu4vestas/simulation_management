from datetime import date, datetime, timedelta, timezone
from sqlmodel import Session, func, select
from fastapi import HTTPException
from db.database import Database
from datamodel import dtos
from cleanup_cycle import cleanup_dtos, scheduler_dtos
from cleanup_cycle.scheduler_dtos import TaskStatus, CalendarStatus, ActionType, CleanupCalendarDTO, CleanupTaskDTO 

class CleanupSchedulerPrePlanned:
    # Check all rootfolders Active calendars periodically to verify:
    #  - are all tasks completed so that the calendars state can change to completed
    #  - can I activate the next task because the previous task is completed
    #  - do I need to change the status of the calendar to FAILED because a task failed

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
            configs = session.exec( select(dtos.CleanupConfigurationDTO).where(
                    (dtos.CleanupConfigurationDTO.cycletime > 0) &
                    (dtos.CleanupConfigurationDTO.cleanupfrequency > 0) &
                    (dtos.CleanupConfigurationDTO.cleanup_start_date != None) &
                    (dtos.CleanupConfigurationDTO.cleanup_start_date <= today) &
                    (dtos.CleanupConfigurationDTO.cleanup_progress.in_([
                        dtos.CleanupProgress.ProgressEnum.INACTIVE.value,
                        dtos.CleanupProgress.ProgressEnum.DONE.value
                    ]))
                ) ).all()

            state_configs:list[cleanup_dtos.CleanupState] = [cleanup_dtos.CleanupState(config) for config in configs]
            calendars:list[scheduler_dtos.CleanupCalendarDTO] = []
            for config in state_configs:
                if config.can_start_cleanup_now():
                    # if the state is DONE then we must advance the start date to today before generating the calendar
                    # otherwise new simulation will not come into scope for cleanup
                    if config.cleanup_progress == dtos.CleanupProgress.ProgressEnum.DONE.value:
                        config.dto.cleanup_start_date = datetime.now() #date.today()
                        config.save_to_db(session)
                    calendar:scheduler_dtos.CleanupCalendarDTO = CleanupScheduler.generate_cleanup_calendar(config)    
                    calendars.append(calendar)

            return f"Generated or found {len(calendars)} calendars ."
    
    @staticmethod
    def generate_cleanup_calendar(config: cleanup_dtos.CleanupState, stop_after_cleanup_cycle: bool=False) -> "CleanupCalendarDTO":
        with Session(Database.get_engine()) as session:
            # Check if there is already an active calendar for this rootfolder
            calendar: CleanupCalendarDTO = session.exec( select(CleanupCalendarDTO).where(
                    (CleanupCalendarDTO.rootfolder_id == config.dto.rootfolder_id) & 
                    (CleanupCalendarDTO.status == CalendarStatus.ACTIVE.value) ) ).first()
            if calendar is not None:
                return calendar

            # No active calendar so go ahead
            from datamodel.dtos import RootFolderDTO
            rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == config.dto.rootfolder_id)).first()
            if not rootfolder:
                raise HTTPException(status_code=404, detail="RootFolder not found")

            calendar: CleanupCalendarDTO = CleanupCalendarDTO(rootfolder_id=config.dto.rootfolder_id,
                                                              start_date=config.dto.cleanup_start_date,
                                                              status=CalendarStatus.ACTIVE.value)
            session.add(calendar)
            session.flush()  # Ensure calendar.id is available
            
            # Generate the tasks for this calendar based on ActionType enum

            # 4) SCAN_ROOTFOLDER - About 3 days before the end of the RETENTION_REVIEW phase
            # Storage agent: scan the rootfolder for simulations. Can take up to one day
            task_scan_rootfolder = CleanupTaskDTO(
                calendar_id=calendar.id,
                rootfolder_id=config.dto.rootfolder_id,
                path=rootfolder.path,
                task_offset=0,   # max due to testing 
                action_type=ActionType.SCAN_ROOTFOLDER.value,
                storage_id=rootfolder.storage_id,  
                status=TaskStatus.PLANNED.value,
                max_execution_hours=48
            )
            session.add(task_scan_rootfolder)

            # 1) START_RETENTION_REVIEW - Day 0 of STARTING_RETENTION_REVIEW phase
            # Internal CleanupProgress Agent: call cleanup_cycle_startInitialize. Takes less than 5 minutes
            task_start_retention_review = CleanupTaskDTO(
                calendar_id=calendar.id,
                rootfolder_id=config.dto.rootfolder_id,
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
                rootfolder_id=config.dto.rootfolder_id,
                path=rootfolder.path,
                task_offset=0,
                action_type=ActionType.SEND_INITIAL_NOTIFICATION.value,
                storage_id=None,  # Internal action
                status=TaskStatus.PLANNED.value,
                max_execution_hours=1
            )
            session.add(task_send_initial_notification)

            # 3) SEND_FINAL_NOTIFICATION - About a week before end of RETENTION_REVIEW phase
            # Internal email agent: Notify stakeholders about the ongoing retention review. Takes less than a minute
            # Assuming RETENTION_REVIEW lasts for config.cleanupfrequency days, schedule 7 days before end
            retention_review_duration = config.dto.cleanupfrequency if config.dto.cleanupfrequency > 0. else 7.
            task_send_final_notification = CleanupTaskDTO(
                calendar_id=calendar.id,
                rootfolder_id=config.dto.rootfolder_id,
                path=rootfolder.path,
                task_offset=max(retention_review_duration,retention_review_duration - 7), # max due to testing
                action_type=ActionType.SEND_FINAL_NOTIFICATION.value,
                storage_id=None,  # Internal action
                status=TaskStatus.PLANNED.value,
                max_execution_hours=1
            )
            session.add(task_send_final_notification)

            # 5) CLEAN_ROOTFOLDER - 0 day into the CLEANING phase
            # Storage agent: clean marked simulations. Can take up to a day
            task_clean_rootfolder = CleanupTaskDTO(
                calendar_id=calendar.id,
                rootfolder_id=config.dto.rootfolder_id,
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
                rootfolder_id=config.dto.rootfolder_id,
                path=rootfolder.path,
                task_offset=max(retention_review_duration, retention_review_duration + 1),  # 1 day after cleaning starts
                action_type=ActionType.FINISH_CLEANUP_CYCLE.value,
                storage_id=None,  # Internal action
                status=TaskStatus.PLANNED.value,
                max_execution_hours=1
            )
            session.add(task_finish_cleanup_cycle)
            session.commit()
            return calendar

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
            active_calendars = session.exec( select(CleanupCalendarDTO).where(
                    CleanupCalendarDTO.status == CalendarStatus.ACTIVE ) ).all()
            
            calendars_completed = 0
            calendars_failed = 0
            tasks_activated = 0
            task_timeouts = 0
            
            for calendar in active_calendars:

                # Get all tasks for this calendar, ordered by days_offset then by ID
                tasks = session.exec(
                    select(CleanupTaskDTO)
                    .where(CleanupTaskDTO.calendar_id == calendar.id)
                    .order_by(CleanupTaskDTO.action_type)     #trying to pospone dependencies on dates til later in the process old order=>     .order_by(CleanupTaskDTO.task_offset, CleanupTaskDTO.id)
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
    def activate_planned_tasks(session: Session, calendar: CleanupCalendarDTO, ordered_tasks: list[CleanupTaskDTO]) -> int:
        tasks_activated = 0

        # Activate PLANNED tasks if ready
        # Logic: Activate tasks only when:
        #   1. The scheduled date (start_date + days_offset) has been reached or exceeded
        #   2. All previous tasks in the calendar are completed (sequential execution)
        planned_tasks = [t for t in ordered_tasks if t.status == TaskStatus.PLANNED.value]
        today = date.today()
        
        for task in planned_tasks:
            # Calculate the scheduled date for this task
            scheduled_date = calendar.start_date + timedelta(days=task.task_offset)
            
            # Check if the scheduled date has been reached
            if today < scheduled_date:
                # Not ready yet, skip this task
                continue
            
            # Check if all previous tasks are completed
            previous_tasks = ordered_tasks[:ordered_tasks.index(task)]
            all_previous_completed = all(
                t.status == TaskStatus.COMPLETED.value for t in previous_tasks
            )
            
            if not all_previous_completed:
                # Tasks must be done sequentially so break 
                break
            
            # All prerequisites met (date reached and previous tasks completed), activate the task
            task.status = TaskStatus.ACTIVATED.value
            session.add(task)
            tasks_activated += 1
            print(f"Activated task {task.id} for calendar {task}")

        return tasks_activated
    
    @staticmethod
    def deactivate_calendar(rootfolder_id:int) -> None:
        """Deactivate all active calendars and their tasks for a given rootfolder.
        
        This method will:
        1. Find all active calendars for the rootfolder
        2. Mark them as INTERRUPTED
        3. Find all non-terminal tasks (PLANNED, ACTIVATED, RESERVED, INPROGRESS)
        4. Mark them as FAILED with an appropriate message
        
        Args:
            rootfolder_id: The ID of the rootfolder whose calendars should be deactivated
        """
        with Session(Database.get_engine()) as session:
            # Get all active calendars for this rootfolder
            active_calendars = session.exec( select(CleanupCalendarDTO).where(
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
                            TaskStatus.PLANNED.value,
                            TaskStatus.ACTIVATED.value,
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
        
        return [
            {
                "action_type": ActionType.SCAN_ROOTFOLDER.value,
                "task_offset": 0,
                "needs_storage_id": True,
                "max_execution_hours": 48
            },
            {
                "action_type": ActionType.START_RETENTION_REVIEW.value,
                "task_offset": 0,
                "needs_storage_id": False,
                "max_execution_hours": 1
            },
            {
                "action_type": ActionType.SEND_INITIAL_NOTIFICATION.value,
                "task_offset": 0,
                "needs_storage_id": False,
                "max_execution_hours": 1
            },
            {
                "action_type": ActionType.SEND_FINAL_NOTIFICATION.value,
                "task_offset": max(retention_review_duration, retention_review_duration - 7),
                "needs_storage_id": False,
                "max_execution_hours": 1
            },
            {
                "action_type": ActionType.CLEAN_ROOTFOLDER.value,
                "task_offset": retention_review_duration,
                "needs_storage_id": True,
                "max_execution_hours": 48
            },
            {
                "action_type": ActionType.FINISH_CLEANUP_CYCLE.value,
                "task_offset": max(retention_review_duration, retention_review_duration + 1),
                "needs_storage_id": False,
                "max_execution_hours": 1
            }
        ]

    @staticmethod
    def create_calendars_for_cleanup_configuration_ready_to_start() -> str:
        # Fetch all cleanup configurations that are ready to start a new cleanup cycle.
        # Creates calendars but NO tasks upfront - tasks are created JIT.        
        # Returns:
        #     String message indicating how many calendars were generated

        today = date.today()
        
        with Session(Database.get_engine()) as session:
            configs = session.exec( select(dtos.CleanupConfigurationDTO).where(
                    (dtos.CleanupConfigurationDTO.cycletime > 0) &
                    (dtos.CleanupConfigurationDTO.cleanupfrequency > 0) &
                    (dtos.CleanupConfigurationDTO.cleanup_start_date != None) &
                    (dtos.CleanupConfigurationDTO.cleanup_start_date <= today) &
                    (dtos.CleanupConfigurationDTO.cleanup_progress.in_([
                        dtos.CleanupProgress.ProgressEnum.INACTIVE.value,
                        dtos.CleanupProgress.ProgressEnum.DONE.value
                    ]))
                )
            ).all()

            state_configs:list[cleanup_dtos.CleanupState] = [cleanup_dtos.CleanupState(config) for config in configs]
            calendars:list[scheduler_dtos.CleanupCalendarDTO] = []
            for config in state_configs:
                if config.can_start_cleanup_now():
                    if config.cleanup_progress == dtos.CleanupProgress.ProgressEnum.DONE.value:
                        config.dto.cleanup_start_date = datetime.now()
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
            from datamodel.dtos import RootFolderDTO
            rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == config.dto.rootfolder_id)).first()
            if not rootfolder:
                raise HTTPException(status_code=404, detail="RootFolder not found")

            # Create calendar only - NO tasks created here
            calendar: CleanupCalendarDTO = CleanupCalendarDTO(
                rootfolder_id=config.dto.rootfolder_id,
                start_date=config.dto.cleanup_start_date,
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
            active_calendars = session.exec( select(CleanupCalendarDTO).where(
                    CleanupCalendarDTO.status == CalendarStatus.ACTIVE ) ).all()
            
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
            retention_review_duration = config.cleanupfrequency if config.cleanupfrequency > 0 else 7.0
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
        retention_review_duration = config.cleanupfrequency if config.cleanupfrequency > 0 else 7.0
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
        today = date.today()
        if today < scheduled_date:
            return 0  # Not time yet
        
        # Create the task directly in ACTIVATED state
        new_task = CleanupTaskDTO(
            calendar_id=calendar.id,
            rootfolder_id=calendar.rootfolder_id,
            path=rootfolder.path,
            task_offset=next_task_def["task_offset"],
            action_type=next_task_def["action_type"],
            storage_id=rootfolder.storage_id if next_task_def["needs_storage_id"] else None,
            status=TaskStatus.ACTIVATED.value,  # JIT: Direct to ACTIVATED
            max_execution_hours=next_task_def["max_execution_hours"]
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