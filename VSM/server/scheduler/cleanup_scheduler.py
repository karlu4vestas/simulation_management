# The scheduler's job is to activate the tasks in the rootfolders calendar when their scheduled time comes.
# Agents pull ACTIVATED tasks from the central queue by trying to make a reservation of a task that matched their profile (ActionType,storage_id), 
# If a task is returned then it is reserved before return to the agent. The agent must execute it and report back to close the task 
# Agents are in that sense an anonymous asynchronous worker that pull and report on tasks over a REST API. 
# At this point Agents does even have to register with the central cleanup system.

# A rootfolder's calendar is generated based on the rootfolder cleanup configuration
# - in what cleanup phase must an action be carried out
# - how many days into the phase must the action be carried out
# - what action must be carried out

# Agents must run close to where the work needs to be done
# Storage agents: must run close to the storage that needs scanning in order to minimize latency when scanning and cleaning simulations
# Notification agents: can run anywhere as long as they can reach the email server
# Internal Agent: is an agents that runs internal operations such as starting and finishing a cleanup cycle.
#                 Even if this could be done in another way we have create them in order to make the scheduler agent relation consistent.
# Agents must be matched to the tasks they can perform based on the storage_id and action_type


# The schduler must run periodically to check all active calendars and their tasks
#  - for now a run every hour should be sufficient


from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum

from sqlmodel import SQLModel, Field
from sqlmodel import Session, func, select
from fastapi import FastAPI, Query, HTTPException
from datamodel.dtos import FolderNodeDTO
from db.database import Database
from db.db_api import insert_or_update_simulations_in_db, FileInfo, read_folders_marked_for_cleanup


class CalendarStatus(str, Enum):
    """Status of a calendar entry."""
    ACTIVE      = "active"          
    COMPLETED   = "completed"       # Action has been completed
    FAILED      = "failed"          # Action failed (terminal state) because one of the tasks failed to complete
    INTERRUPTED = "interrupted"     # The calendar was interrupted (terminal state) by the user changing the cleanup_configuration to INACTIVE


class TaskStatus(str, Enum):
    """Status of a task in the queue."""
    PLANNED      = "planned"         # Task is waiting to be picked up
    ACTIVATED    = "activated"       # Task has been activated and is ready for execution
    RESERVED     = "reserved"        # Task has been reserved by an agent
    COMPLETED    = "completed"       # Task finished successfully
    FAILED       = "failed"          # Task failed (terminal state)
    #CANCELLED    = "cancelled"       # Task was cancelled

# Agent can pickup the following types of tasks
class ActionType(str, Enum):
    """Types of actions that can be scheduled in the calendar."""
    START_RETENTION_REVIEW    = "start_retention_review"                    # Internal CleanupProgress Agent: call cleanup_cycle_startInitialize. Takes less than 5 minutes
    SEND_INITIAL_NOTIFICATION = "send_notification"                         # internal email agent: Notify stakeholders about the new retention review. 1 day into the RETENTION_REVIEW phase. Takes less than a minute
    SEND_FINAL_NOTIFICATION   = "send_notification"                         # internal email agent: Notify stakeholders about the ongoing retention review. About a week before end of RETENTION_REVIEW phase. Takes less than a minute
    SCAN_ROOTFOLDER           = "scan_rootfolder"                           # storage agent: scan the rootfolder for simulations. About 3 days before the end of the RETENTION_REVIEW phase. Can take upto one day
    CLEAN_ROOTFOLDER          = "clean_simulations"                         # storage agent: clean marked simulations. 0 day into the CLEANING phase. Can take upto a day
    FINISH_CLEANUP_CYCLE      = "finish_cleanup_cycle"                      # Internal CleanupProgress Agent: call cleanup_cycle_finishing to change the remaining marked retention to the next retention type.
    PREPARE_NEXT_CLEANUP_CYCLE= "prepare_next_cleanup_cycle"                # Internal CleanupProgress Agent: execute the last step in the cleanup cycle by calling prepare_next_cleanup_cycle.



class CleanupCalendarBase(SQLModel):
    """
    Base class for cleanup calendar tasks.

    All tasks for a cleanup round are preplanned in the calendar.
    The calendar defines scheduled actions for each rootfolder's cleanup cycle.
    Each entry specifies:
    - What ActionType to perform
    - What storage platform the rootfolder is on
    - When to perform it (phase + days_offset)
    - Maximum allowed execution time. Exceeding this marks the action and the calendar as FAILED
    
    
    The scheduler's job is to activate the tasks when their scheduled time comes.
    The agents pull tasks from the central queue by trying to make a reservation of a task that matched their profile (ActionType,storage_id), 
    If a reserved task is returned then the agent must execute it and report back to close the task 
    Agents are in that sense an anonymous asynchronous worker - it does not even have to register with the central cleanup system.

    """
    rootfolder_id: int     = Field(foreign_key="rootfolderdto.id", index=True)
    status: str            = Field(default=CalendarStatus.ACTIVE.value, description="Current status of the calendar entry")
    start_date: date       = Field(description="Start date of the cleanup phase for this calendar entry")

class CleanupCalendarDTO(CleanupCalendarBase, table=True):
    id: int | None         = Field(default=None, primary_key=True)
    

class CleanupTaskBase(SQLModel):
    """
    Base class for cleanup tasks in the calendar.
    
    Tasks are created by the scheduler based on calendar entries and executed by agents.
    Each task contains all the information needed by an agent to execute the action.
    """
    calendar_id: int | None             = Field(default=None, foreign_key="cleanupcalendardto.id", description="Reference to calendar entry that created this task")
    #information from the cleanup configuration
    rootfolder_id: int                  = Field(foreign_key="rootfolderdto.id", index=True)
    task_offset: int                    = Field(description="Days offset into the current cleanup phase when this action must be executed")

    # Info from an agent to match a task
    action_type: str                    = Field(description="Type of action to perform")
    storage_id: str                     = Field(description="Storage platform where the rootfolder is located")
 
    # Execution tracking
    status: str = Field(default=None, description="TaskStatus")
    reserved_by_agent_id: str | None    = Field(default=None, description="ID of agent that reserved this task")
    reserved_at: datetime | None        = Field(default=None, description="When the task was reserved")    
    completed_at: datetime | None       = Field(default=None, description="When execution completed")

    # Execution constraints
    max_execution_hours: int            = Field(default=24, description="Maximum hours allowed before marking as failed")
    
    # Results
    status_message: str | None          = Field(default=None, description="Result message from agent")
    

class CleanupTaskDTO(CleanupTaskBase, table=True):
    id: int | None                      = Field(default=None, primary_key=True)
    


@dataclass
class AgentInfo:
    """
    Represents the agent Information that is required to reserve and report on tasks.
    Agents pull tasks from the central scheduler based on their capabilities.
    """
    agent_id: str                            # must be a unique ID for the agent
    action_types: list[str]        # list of ActionTypes the agent can perform
    supported_storage_ids: list[str]|None    # storage platforms the agent can operate on
                                             # must be one of the predefined infrastructure storage IDs.                                       
                                             # None would be adequate for notification agents and internal agent that are used to change cleanup progress state
    def __repr__(self):
        return f"AgentInfo(agent_id={self.agent_id}, action_types={self.action_types}, supported_storage_ids={self.supported_storage_ids})"

    @staticmethod
    def reserve_task(agent: "AgentInfo") -> CleanupTaskDTO | None:
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
    def insert_or_update_simulations_in_db(task_id: int, rootfolder_id: int, simulations: list[FileInfo]) -> dict[str, str]:
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
        paths:list[str] = [folder.full_path for folder in simulations ]
        return paths


class CleanupScheduler:

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
        """
        Periodically check all active calendars and their tasks to:
        1. Activate PLANNED tasks that are ready for execution
        2. Check if RESERVED tasks have exceeded their max execution time and mark them as FAILED
        3. Mark calendars as COMPLETED when all their tasks are completed
        4. Mark calendars as FAILED if any of their tasks failed
        
        A task transitions from PLANNED to ACTIVATED only when both conditions are met:
        - The scheduled date has been reached or exceeded
        - All previous tasks in the calendar are completed

        Returns:
            Dictionary with summary of actions taken
        """
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
    def call_internal_agents() -> dict[str, any]:
        agents: list[AgentTemplate] = get_internal_agents()
        for agent in agents:
            agent.run()
        return {"message": "Internal agents called successfully"}


# ----------------- AgentTemplate -----------------
from abc import ABC, abstractmethod
class AgentTemplate(ABC):
    agent_info: AgentInfo
    task: CleanupTaskDTO | None
    error_message: str | None
    success_message: str | None

    def __init__(self, agent_id: str, action_types: list[str], supported_storage_ids: list[str]|None = None):
        self.agent_info = AgentInfo(agent_id, action_types, supported_storage_ids)

    def __repr__(self):
        return f"AgentTemplate(agent_info={self.agent_info})"

    def run(self):
        self.reserve_task()
        if self.task is not None:
            self.execute_task()

        pass

    def reserve_task(self):
        self.task = AgentInfo.reserve_task(self.agent_info)

    def complete_task(self ):
        if self.task is not None:
            if self.error_message is not None:
                AgentInfo.task_completion(self.task.id, TaskStatus.FAILED.value, self.error_message)
            else:
                AgentInfo.task_completion(self.task.id, TaskStatus.COMPLETED.value, "Task executed successfully")

    @abstractmethod
    def execute_task(self):
        """Subclasses must implement this method to execute their specific task logic."""
        pass

# ----------------- internal agents implementations -----------------
from db.cleanup_cycle import cleanup_cycle_start, cleanup_cycle_finishing, cleanup_cycle_prepare_next_cycle
class AgentCleanupCycleStart(AgentTemplate):

    def __init__(self):
        super().__init__("AgentCleanupCycleStart", [ActionType.START_RETENTION_REVIEW.value])

    def execute_task(self):
        cleanup_cycle_start(self.task.rootfolder_id)
        self.success_message = f"Cleanup cycle started for rootfolder {self.task.rootfolder_id}"

class AgentCleanupCycleFinishing(AgentTemplate):

    def __init__(self):
        super().__init__("AgentCleanupCycleFinishing", [ActionType.FINISH_CLEANUP_CYCLE.value])

    def execute_task(self):
        cleanup_cycle_finishing(self.task.rootfolder_id)
        self.success_message = f"Cleanup cycle finishing for rootfolder {self.task.rootfolder_id}"

class AgentCleanupCyclePrepareNext(AgentTemplate):
    def __init__(self):
        super().__init__("AgentCleanupCyclePrepareNext", [ActionType.PREPARE_NEXT_CLEANUP_CYCLE.value])

    def execute_task(self):
        cleanup_cycle_prepare_next_cycle(self.task.rootfolder_id)
        self.success_message = f"Next cleanup cycle prepared for rootfolder {self.task.rootfolder_id}"


class AgentNotification(AgentTemplate):
    def __init__(self):
        super().__init__("AgentNotification", [ActionType.SEND_INITIAL_NOTIFICATION.value, ActionType.SEND_FINAL_NOTIFICATION.value])
    
    def send_notification(self, message: str, receivers: list[str]) -> None:
        """
        Send email notification to the specified receivers.
        
        Args:
            message: The notification message to send
            receivers: List of email addresses to send the notification to


        Note: You may need to:

        Update the smtp_server and smtp_port values to match your actual email server
        Add authentication credentials if your SMTP server requires them (uncomment the starttls() and login() lines)
        Consider moving email configuration to environment variables or a config file
        Add the email configuration to your application settings for better maintainability
        The method will set self.error_message if sending fails, which will cause the task to be marked as FAILED when complete_task() is called.            
        """
        self.error_message = f"sending email + to {receivers} receivers.\nEmail content: {message} "    
        return 
    

        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        try:
            # Email configuration - these should ideally come from environment variables or config
            #smtp_server = "smtp.vestas.com"  # Update with actual SMTP server
            #smtp_port = 587  # or 465 for SSL
            #sender_email = "vsm-notifications@vestas.com"  # Update with actual sender

            smtp_server = "smtp.vestas.com"  # Update with actual SMTP server
            smtp_port = 587  # or 465 for SSL
            sender_email = "vsm-notifications@vestas.com"  # Update with actual sender
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = ", ".join(receivers)
            msg['Subject'] = "VSM Cleanup Notification"
            
            # Add message body
            msg.attach(MIMEText(message, 'plain'))
            
            # Send email
            # Note: This is a basic implementation. In production, you might want to:
            # 1. Use authentication if required
            # 2. Handle SSL/TLS properly
            # 3. Add retry logic
            # 4. Log the email sending activity
            
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                # Uncomment and configure if authentication is needed:
                # server.starttls()
                # server.login(username, password)
                
                server.send_message(msg)
            
            self.success_message = f"Notification sent successfully to {len(receivers)} recipient(s)"
            
        except Exception as e:
            self.error_message = f"Failed to send notification: {str(e)}"

    def execute_task(self):
        from db.database import Database
        from datamodel.dtos import RootFolderDTO, CleanupConfigurationDTO, CleanupProgress
        from sqlmodel import Session, func, select
        with Session(Database.get_engine()) as session:
            rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == self.task.rootfolder_id)).first()
            config: CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session) if rootfolder is not None else None
            if rootfolder is None or config is None:
                self.error_message = f"RootFolder with ID {self.task.rootfolder_id} not found."
            else:
                enddate_for_cleanup_cycle: date = config.cleanup_start_date + timedelta(days=config.cleanupfrequency-1)
                initial_message: str = f"The review has started. Use it to review and adjust the retention of your simulation in particular those marked for cleanup \n" + \
                                    f"You have configure the cleanup rutine as follow " + \
                                    f"Duration of the review periode is {config.cleanupfrequency_days} days; ending on {enddate_for_cleanup_cycle}." + \
                                    f"Simulations will be marked for cleanup {config.cycletime} days from last modification date unless otherwise specified by retention settings."
                final_message: str   = f"The retention review is about to end in {config.cleanupfrequency-self.task.task_offset-1} days."

                message: str = initial_message if self.task.action_type == ActionType.SEND_INITIAL_NOTIFICATION.value else final_message

                receivers: list[str] = [] if rootfolder.owner is None else [rootfolder.owner+f"@vestas.com"]
                if rootfolder.approvers is not None:
                    for approver in rootfolder.approvers:
                        receivers.append(approver+f"@vestas.com")
                if len(receivers) == 0:
                    self.error_message = f"No receivers found for RootFolder with ID {self.task.rootfolder_id}."                
                else:
                    self.send_notification(message, receivers)

def get_internal_agents() -> list[AgentTemplate]:
    return [
        AgentCleanupCycleStart(),
        AgentCleanupCycleFinishing(),
        AgentCleanupCyclePrepareNext(),
        AgentNotification()
    ]