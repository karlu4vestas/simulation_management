from enum import Enum
from datetime import date
from sqlmodel import SQLModel, Field
from pydantic import BaseModel


#STARTING_RETENTION_REVIEW  is the only phase where the backend is allowed to mark simulation for cleanup.
#Simulations imported in this phase must postpone possible marked for cleanup to the next cleanup round
class CleanupProgress:
    class ProgressEnum(str, Enum):
        """Enumeration of cleanup round progress states."""
        INACTIVE = "inactive"    # No cleanup is active
        STARTING_RETENTION_REVIEW   = "starting_retention_review"      # This is the only phase where the backend is allowed to mark simulation for cleanup.
        RETENTION_REVIEW            = "retention_review"      # Markup phase - users can adjust what simulations will be cleaned. 
        CLEANING = "cleaning"    # Actual cleaning is happening
        FINISHING = "finish_cleanup_round"    # finish the cleanup round
        DONE = "cleanup_is_done"    # Cleanup round is complete, waiting for next round

    # Define valid state transitions
    valid_transitions: dict["CleanupProgress.ProgressEnum", list["CleanupProgress.ProgressEnum"]] = {
        ProgressEnum.INACTIVE: [ProgressEnum.STARTING_RETENTION_REVIEW],
        ProgressEnum.STARTING_RETENTION_REVIEW: [ProgressEnum.RETENTION_REVIEW],
        ProgressEnum.RETENTION_REVIEW: [ProgressEnum.CLEANING, ProgressEnum.INACTIVE],
        ProgressEnum.CLEANING: [ProgressEnum.FINISHING, ProgressEnum.INACTIVE],
        ProgressEnum.FINISHING: [ProgressEnum.DONE, ProgressEnum.INACTIVE],
        ProgressEnum.DONE: [ProgressEnum.INACTIVE, ProgressEnum.STARTING_RETENTION_REVIEW],
    }
    
    # Define the natural progression through cleanup states
    next_natural_state: dict["CleanupProgress.ProgressEnum", "CleanupProgress.ProgressEnum"] = {
        ProgressEnum.INACTIVE: ProgressEnum.STARTING_RETENTION_REVIEW,
        ProgressEnum.STARTING_RETENTION_REVIEW: ProgressEnum.RETENTION_REVIEW,
        ProgressEnum.RETENTION_REVIEW: ProgressEnum.CLEANING,
        ProgressEnum.CLEANING: ProgressEnum.FINISHING,
        ProgressEnum.FINISHING: ProgressEnum.DONE,
        ProgressEnum.DONE: ProgressEnum.STARTING_RETENTION_REVIEW,
    }

# The configuration can be used as follow:
#   a) deactivating cleanup is done by setting cleanupfrequency to None
#   b) activating a cleanup round requires that cleanupfrequency is set and that the cycletime is > 0. 
#        If cleanup_round_start_date is not set then we assume today
#   c) cycletime: is minimum number of days from last modification of a simulation til it can be cleaned
#        It can be set with cleanup is inactive cleanupfrequency is None
#   d) cleanup_progress to describe where the rootfolder is in the cleanup round: 
#      - inactive: going from an activate state to inactive will set the cleanup_start_date to None. 
#                  If inactivate state and cleanupfrequency, cycletime and cleanup_start_date will start the cleanup when the cleanup_start_date is reached.
#      - started:  the markup phase starts then cleanup round starts so that the user can adjust what simulations will be cleaned
#      - cleaning: this is the last phase in which the actual cleaning happens
#      - finished: the cleanup round is finished and we wait for the next round
class CleanupConfigurationBase(SQLModel):
    """Base class for cleanup configuration."""
    rootfolder_id: int              = Field(default=None, foreign_key="rootfolderdto.id")
    cycletime: int                  = Field(default=0)
    cleanupfrequency: int           = Field(default=0)  # days to next cleanup round
    cleanup_start_date: date | None = Field(default=None)
    cleanup_progress: str           = Field(default=CleanupProgress.ProgressEnum.INACTIVE.value)

class CleanupConfigurationDTO(CleanupConfigurationBase, table=True):
    """Cleanup configuration as separate table."""
    id: int | None = Field(default=None, primary_key=True)
    
    # Relationship
    #rootfolder: "RootFolderDTO" = Relationship(back_populates="cleanup_config")
    
    def __eq__(self, other):
        if not isinstance(other, CleanupConfigurationDTO):
            return False
        return (self.cycletime == other.cycletime and 
                self.cleanupfrequency == other.cleanupfrequency and 
                self.cleanup_start_date == other.cleanup_start_date and
                self.cleanup_progress == other.cleanup_progress)
   
    def is_valid(self) -> bool:
        # has cleanup_frequency and cycle_time been set. 
        # If cleanup_start_date is None then cleanup_progress must be INACTIVE
        is_valid: bool = (self.cleanupfrequency is not None and self.cleanupfrequency > 0) and \
                         (self.cycletime is not None and self.cycletime > 0) and \
                         ((self.cleanup_progress == CleanupProgress.ProgressEnum.INACTIVE.value) \
                          or self.cleanup_start_date is not None)
        return is_valid
    
    def can_start_cleanup_now(self) -> bool:
        # Return true if 
        # cleanup is ready to start at some point
        # and the cleanup_start_date is today or in the past
        # and self.cleanup_progress is INACTIVE or FINISHED
        
        # has valid configuration 
        has_valid_configuration = self.is_valid() and self.cleanup_start_date is not None and self.cleanup_start_date <= date.today() 
        if not has_valid_configuration:
            return False

        has_valid_progress = self.cleanup_progress in [CleanupProgress.ProgressEnum.INACTIVE.value, CleanupProgress.ProgressEnum.DONE.value]
        return has_valid_progress
    
    def is_in_cleanup_round(self) -> bool:
        return self.cleanup_progress in [CleanupProgress.ProgressEnum.RETENTION_REVIEW.value, CleanupProgress.ProgressEnum.CLEANING.value, CleanupProgress.ProgressEnum.FINISHING.value]
    
    def is_starting_cleanup_round(self) -> bool:
        return self.cleanup_progress in [CleanupProgress.ProgressEnum.STARTING_RETENTION_REVIEW.value]


    def can_transition_to(self, new_state: CleanupProgress.ProgressEnum) -> bool:
        """Check if transition to new_state is valid from current state."""

        # transitions require a valid configuration or that the new state is INACTIVE
        if not (self.is_valid() or new_state == CleanupProgress.ProgressEnum.INACTIVE):
            return False

        current = CleanupProgress.ProgressEnum(self.cleanup_progress)
        
        if new_state not in CleanupProgress.valid_transitions.get(current, []):
            return False

        return True
    
    def transition_to(self, new_state: CleanupProgress.ProgressEnum) -> bool:
        """
        Transition to a new cleanup progress state.
        
        Args:
            new_state: The state to transition to
            
        Returns:
            tuple[bool, str]: (success, message) - True if transition succeeded, False otherwise
        """
        can_transition = self.can_transition_to(new_state)
        
        if can_transition:
            self.cleanup_progress = new_state.value
            return True
        
        return False
    
    def transition_to_next(self) -> bool:
        """
        Transition to the next default state in the cleanup workflow.
        Follows the primary path: INACTIVE -> STARTED -> CLEANING -> FINISHED -> INACTIVE
        
        Returns:
            tuple[bool, str]: (success, message) - True if transition succeeded, False otherwise
        """
        current = CleanupProgress.ProgressEnum(self.cleanup_progress)
        next_state = CleanupProgress.next_natural_state.get(current, None)
        if next_state is None:
            return False
        
        return self.transition_to(next_state)
    
    #There must be a scheduler that generates the calendar when a cleanupconfiguration is ready to start
    def generate_calendar(self) -> bool:
        calendar: CleanupCalendarDTO = None
        if self.can_start_cleanup_now():
            calendar = CleanupCalendarDTO.generate_calendarClean(self)
        return calendar is not None


# ------------------cleanup calendar and tasks ------------------
from datetime import date, datetime, timedelta, timezone

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
    COMPLETED    = "completed"       # Task finished successfully
    FAILED       = "failed"          # Task failed (terminal state)
    #CANCELLED    = "cancelled"       # Task was cancelled

# Agent can pickup the following types of tasks
class ActionType(str, Enum):
    """Types of actions that can be scheduled in the calendar."""
    CREATE_CLEANUP_CALENDAR   = "create_cleanup_calendar"                   # Internal CleanupProgress Agent: call generate_cleanup_calendar when the cleanup is ready to start. Takes less than 5 minutes
    START_RETENTION_REVIEW    = "start_retention_review"                    # Internal CleanupProgress Agent: call cleanup_cycle_startInitialize. Takes less than 5 minutes
    SEND_INITIAL_NOTIFICATION = "send_notification"                         # internal email agent: Notify stakeholders about the new retention review. 1 day into the RETENTION_REVIEW phase. Takes less than a minute
    SEND_FINAL_NOTIFICATION   = "send_notification"                         # internal email agent: Notify stakeholders about the ongoing retention review. About a week before end of RETENTION_REVIEW phase. Takes less than a minute
    SCAN_ROOTFOLDER           = "scan_rootfolder"                           # storage agent: scan the rootfolder for simulations. About 3 days before the end of the RETENTION_REVIEW phase. Can take upto one day
    CLEAN_ROOTFOLDER          = "clean_simulations"                         # storage agent: clean marked simulations. 0 day into the CLEANING phase. Can take upto a day
    FINISH_CLEANUP_CYCLE      = "finish_cleanup_cycle"                      # Internal CleanupProgress Agent: call cleanup_cycle_finishing to change the remaining marked retention to the next retention type.
    PREPARE_NEXT_CLEANUP_CYCLE= "prepare_next_cleanup_cycle"                # Internal CleanupProgress Agent: execute the last step in the cleanup cycle by calling prepare_next_cleanup_cycle.


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
    task_offset: int                    = Field(description="Days offset into the current cleanup phase when this action must be executed")

    # Info from an agent to match a task
    action_type: str                    = Field(description="Type of action to perform")
    storage_id: str | None              = Field(description="Storage platform where the rootfolder is located")
 
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

