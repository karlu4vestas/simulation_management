from enum import Enum
from datetime import date, datetime
from sqlmodel import SQLModel, Field, Column
from sqlalchemy import JSON
from pydantic import BaseModel


# ------------------cleanup calendar and tasks ------------------
# The scheduler runs periodically to activate the tasks in the rootfolders calendar when their scheduled time comes.
# Agents pull ACTIVATED tasks from the central queue by trying to make a reservation of a task that matched their profile (ActionType,storage_id), 
# If a task is returned then it is reserved before return to the agent. The agent must execute it and report back to close the task 
# Agents are in that sense an anonymous asynchronous worker that pull and report on tasks over a REST API. 
# At this point Agents does even have to register with the central cleanup system.

# A rootfolder's calendar is generated based on the rootfolder cleanup configuration and genrated by the cleanup configuration
# - in what cleanup phase must an action be carried out
# - how many days into the phase must the action be carried out
# - what action must be carried out

# Agents must run close to where the work needs to be done and their capabilities must match the work to be performed:
# Storage agents: must run close to the storage that needs scanning in order to minimize latency when scanning and cleaning simulations
# Notification agents: can run anywhere as long as they can reach the email server
# Internal Agent: is an agents that runs internal operations such as starting and finishing a cleanup cycle.
#                 Even if this could be done in another way we have create them in order to make the scheduler agent relation consistent.
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
    INPROGRESS   = "in_progress"     # Task is currently being executed
    COMPLETED    = "completed"       # Task finished successfully
    FAILED       = "failed"          # Task failed (terminal state)
    #CANCELLED    = "cancelled"       # Task was cancelled

# Agent can pickup the following types of tasks
class ActionType(str, Enum):
    """Types of actions that can be scheduled in the calendar."""
    #CLOSE_FINISHED_CALENDARS  = "0 - close_finished_calendars"                 # Internal CleanupProgress Agent: call generate_cleanup_calendar when the cleanup is ready to start. Takes less than 5 minutes
    CREATE_CLEANUP_CALENDAR     = "1 - create_cleanup_calendar"                   # Internal CleanupProgress Agent: call generate_cleanup_calendar when the cleanup is ready to start. Takes less than 5 minutes
    SCAN_ROOTFOLDER             = "2 - scan_rootfolder"                           # storage agent: scan the rootfolder for simulations. 0 day into START_RETENTION_REVIEW. Can take upto one day
    MARK_SIMULATIONS_FOR_REVIEW = "3 - mark_simulations_for_review"                    # Internal CleanupProgress Agent: call cleanup_cycle_startInitialize. Takes less than 5 minutes
    SEND_INITIAL_NOTIFICATION   = "4 - send_notification"                         # internal email agent: Notify stakeholders about the new retention review. 1 day into the RETENTION_REVIEW phase. Takes less than a minute
    SEND_FINAL_NOTIFICATION     = "5 - send_notification"                         # internal email agent: Notify stakeholders about the ongoing retention review. About a week before end of RETENTION_REVIEW phase. Takes less than a minute
    CLEAN_ROOTFOLDER            = "6 - clean_simulations"                         # storage agent: clean marked simulations. 0 day into the CLEANING phase. Can take upto a day
    UNMARK_SIMULATIONS_AFTER_REVIEW  = "7 - removed still maked simulation"                      # Internal CleanupProgress Agent: call cleanup_cycle_finishing to change the remaining marked retention to the next retention type.
    FINALISE_CLEANUP_CYCLE           = "8 - finalise_cleanup_cycle"                    # Internal CleanupProgress Agent: execute the last step in the cleanup cycle by calling prepare_next_cleanup_cycle.
    #STOP_AFTER_CLEANUP_CYCLE  = "9 - stop_after_cleanup_cycle"                 # Internal CleanupProgress Agent: execute the last step in the cleanup cycle by calling prepare_next_cleanup_cycle. Before exit set the start date to None and progress to INACTIVE

class AgentInfo(BaseModel):
    """Represents the agent Information that is required to reserve and report on tasks.
    
    Agents pull tasks from the central scheduler based on their capabilities.
    """
    agent_id: str                            # must be a unique ID for the agent
    action_types: list[str]                  # list of ActionTypes the agent can perform
    supported_storage_ids: list[str] | None = None  # storage platforms the agent can operate on
                                             # must be one of the predefined infrastructure storage IDs.
                                             # None is adequate for the notification agent and internal agents that are used to change cleanup progress state


class CleanupTaskBase(SQLModel):
    """
    Base class for cleanup tasks in the calendar.
    
    Tasks are created by the scheduler based on calendar entries and executed by agents.
    Each task contains all the information needed by an agent to execute the action.
    """
    calendar_id: int | None             = Field(default=None, foreign_key="cleanupcalendardto.id", description="Reference to calendar entry that created this task")
    #information from the cleanup configuration
    rootfolder_id: int                  = Field(foreign_key="rootfolderdto.id", index=True)
    path:str                            = Field(description="Full Path of the rootfolder to perform the action on")
    task_offset: int                    = Field(description="Days offset into the current cleanup phase when this action must be executed")

    # Info from an agent to match a task
    action_type: str                    = Field(description="Type of action to perform")
    storage_id: str | None              = Field(description="Storage platform where the rootfolder is located")
 
    # State management - NEW: Principled state transition support
    precondition_states: list[str]         = Field(default=[], sa_column=Column(JSON), description="Accepted CleanupProgress states at task reservation")
    target_state: str | None               = Field(default=None, description="CleanupProgress state to transition to for work execution")
    state_transition_on_reservation: bool  = Field(default=False, description="Whether to transition state when task is reserved")
    state_verification_on_completion: bool = Field(default=True, description="Whether to verify state matches target_state at completion")
 
    # Execution tracking
    status: str                         = Field(default=None, description="TaskStatus")
    reserved_by_agent_id: str | None    = Field(default=None, description="ID of agent that reserved this task")
    reserved_at: datetime | None        = Field(default=None, description="When the task was reserved")    
    completed_at: datetime | None       = Field(default=None, description="When execution completed")

    # Execution constraints
    max_execution_hours: int            = Field(default=24, description="Maximum hours allowed before marking as failed")
    
    # Results
    status_message: str | None          = Field(default=None, description="Result message from agent")
    
class CleanupTaskDTO(CleanupTaskBase, table=True):
    id: int | None                      = Field(default=None, primary_key=True)
    

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
