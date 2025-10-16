from sqlmodel import Field, SQLModel
from typing import Optional
from datetime import date
from typing import Optional
from dataclasses import dataclass
from enum import Enum


# see values in vts_create_meta_data
class SimulationDomainDTO(SQLModel, table=True):
    id: int | None  = Field(default=None, primary_key=True)
    name: str       = Field(default="")

class FolderTypeEnum(str, Enum):
    #Enumeration of legal folder type names for simulation domains.
    #'innernode' must exist for all domains and will be applied to all folders that are not simulations.
    INNERNODE       = "innernode"
    VTS_SIMULATION  = "vts_simulation"

class ExternalRetentionTypes(str, Enum):
    Unknown = None
    Issue = "Issue"
    Clean = "Clean"
    Missing = "Missing"

class CleanupProgressEnum(str, Enum):
    """Enumeration of cleanup round progress states."""
    INACTIVE = "inactive"    # No cleanup is active
    STARTED = "started"      # Markup phase - users can adjust what simulations will be cleaned
    CLEANING = "cleaning"    # Actual cleaning is happening
    FINISHED = "finished"    # Cleanup round is complete, waiting for next round

# see values in vts_create_meta_data
# FolderTypeEnum.INNERNODE must exist of all domains and will be applied to all folders that are not simulations
class FolderTypeBase(SQLModel):
    simulationdomain_id: int        = Field(foreign_key="simulationdomaindto.id") 
    name: str                       = Field(default="")

class FolderTypeDTO(FolderTypeBase, table=True):
    id: int | None                  = Field(default=None, primary_key=True)


# see values in vts_create_meta_data
class RetentionTypeBase(SQLModel):
    simulationdomain_id: int        = Field(foreign_key="simulationdomaindto.id") 
    name: str                       = Field(default="")
    days_to_cleanup: Optional[int]  = None  # days until the simulation can be cleaned. Can be null for path_retention "clean" and "issue"
    is_endstage: bool               = Field(default=False) #end stage is clean, issue or missing
    display_rank: int               = Field(default=0)

#retention types must be order by increasing days_to_cleanup and then by display_rank
class RetentionTypeDTO(RetentionTypeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)


# time from initialization of the simulation til cleanup of the simulation
class CleanupFrequencyBase(SQLModel):
    simulationdomain_id: int  = Field(foreign_key="simulationdomaindto.id") 
    name: str                 = Field(default="")
    days: int                 = Field(default=0)

class CleanupFrequencyDTO(CleanupFrequencyBase, table=True):
    id: int | None            = Field(default=None, primary_key=True)


# how long time does the engineer require to analyse a simulation before it expires and can be cleaned
# see values in vts_create_meta_data
class CycleTimeBase(SQLModel):
    simulationdomain_id: int  = Field(foreign_key="simulationdomaindto.id") 
    name: str                 = Field(default="")
    days: int                 = Field(default=0)

class CycleTimeDTO(CycleTimeBase, table=True):
    id: int | None            = Field(default=None, primary_key=True)


# The configuration can be used as follow:
#   a) deactivating cleanup is done by setting cleanupfrequency to None
#   b) activating a cleanup round requires that cleanupfrequency is set and that the cycletime is > 0. If cleanup_round_start_date is not set then we assume today
#   c) cycletime can be set with cleanup is inactive cleanupfrequency is None
#   d) cleanup_progress to describe where the rootfolder is in the cleanup round: 
#      - inactive
#      - started: the markup phase starts then cleanup round starts so that the user can adjust what simulations will be cleaned
#      - cleaning: this is the last phase in which the actual cleaning happens
#      - finished: the cleanup round is finished and we wait for the next round

@dataclass # these parameters are needed together often
class CleanupConfiguration:
    cycletime: int                                          # days from initialization of the simulations til it can be cleaned
    cleanupfrequency: int                                   # number of days between cleanup rounds
    cleanup_start_date: date | None = None                  # at what date did the current cleanup round start. If not set then we assume today
    cleanup_progress: CleanupProgressEnum = CleanupProgressEnum.INACTIVE  # current state of the cleanup round
    

    def __eq__(self, other):
        if not isinstance(other, CleanupConfiguration):
            return False
        return (self.cycletime == other.cycletime and 
                self.cleanupfrequency == other.cleanupfrequency and 
                self.cleanup_start_date == other.cleanup_start_date and
                self.cleanup_progress == other.cleanup_progress)
    
    def is_valid(self) -> tuple[bool, str]:
        # if cleanupfrequency is set then cycletime must also be set
        is_valid: bool = True if self.cleanupfrequency is None else (self.cycletime is not None and self.cycletime > 0)
        
        if not is_valid:
            return (False, "error: cycletime must be set if cleanupfrequency is set")
        
        # Validate progress state consistency with configuration
        if self.cleanup_progress != CleanupProgressEnum.INACTIVE:
            if self.cleanupfrequency is None:
                return (False, "error: cleanup_progress must be INACTIVE when cleanupfrequency is None")
            if self.cycletime is None or self.cycletime <= 0:
                return (False, "error: cleanup_progress must be INACTIVE when cycletime is not properly set")
        
        return (True, "ok")

    #return true if cleanup can be started with this configuration
    def can_start_cleanup(self) -> bool:
        return self.is_valid()[0] and (self.cleanupfrequency is not None)
    
    def can_transition_to(self, new_state: CleanupProgressEnum) -> tuple[bool, str]:
        """Check if transition to new_state is valid from current state."""
        current = self.cleanup_progress
        
        # Define valid state transitions
        valid_transitions = {
            CleanupProgressEnum.INACTIVE: [CleanupProgressEnum.STARTED],
            CleanupProgressEnum.STARTED: [CleanupProgressEnum.CLEANING, CleanupProgressEnum.INACTIVE],
            CleanupProgressEnum.CLEANING: [CleanupProgressEnum.FINISHED, CleanupProgressEnum.INACTIVE],
            CleanupProgressEnum.FINISHED: [CleanupProgressEnum.INACTIVE, CleanupProgressEnum.STARTED],
        }
        
        # Check if cleanup can be started for non-inactive states
        if new_state != CleanupProgressEnum.INACTIVE and not self.can_start_cleanup():
            return (False, "error: cleanup configuration is not valid or cleanupfrequency is not set")
        
        if new_state not in valid_transitions.get(current, []):
            return (False, f"error: cannot transition from {current.value} to {new_state.value}")
        
        return (True, "ok")
    
    def get_valid_next_states(self) -> list[CleanupProgressEnum]:
        """Returns list of valid states that can be transitioned to from current state."""
        valid_transitions = {
            CleanupProgressEnum.INACTIVE: [CleanupProgressEnum.STARTED] if self.can_start_cleanup() else [],
            CleanupProgressEnum.STARTED: [CleanupProgressEnum.CLEANING, CleanupProgressEnum.INACTIVE],
            CleanupProgressEnum.CLEANING: [CleanupProgressEnum.FINISHED, CleanupProgressEnum.INACTIVE],
            CleanupProgressEnum.FINISHED: [CleanupProgressEnum.INACTIVE, CleanupProgressEnum.STARTED] if self.can_start_cleanup() else [CleanupProgressEnum.INACTIVE],
        }
        
        return valid_transitions.get(self.cleanup_progress, [])
    
    def transition_to(self, new_state: CleanupProgressEnum) -> tuple[bool, str]:
        """
        Transition to a new cleanup progress state.
        
        Args:
            new_state: The state to transition to
            
        Returns:
            tuple[bool, str]: (success, message) - True if transition succeeded, False otherwise
        """
        can_transition, message = self.can_transition_to(new_state)
        
        if can_transition:
            self.cleanup_progress = new_state
            return (True, f"Transitioned to {new_state.value}")
        
        return (False, message)
    
    def transition_to_next(self) -> tuple[bool, str]:
        """
        Transition to the next default state in the cleanup workflow.
        Follows the primary path: INACTIVE -> STARTED -> CLEANING -> FINISHED -> INACTIVE
        
        Returns:
            tuple[bool, str]: (success, message) - True if transition succeeded, False otherwise
        """
        # Define the natural progression through cleanup states
        next_state_map = {
            CleanupProgressEnum.INACTIVE: CleanupProgressEnum.STARTED,
            CleanupProgressEnum.STARTED: CleanupProgressEnum.CLEANING,
            CleanupProgressEnum.CLEANING: CleanupProgressEnum.FINISHED,
            CleanupProgressEnum.FINISHED: CleanupProgressEnum.INACTIVE,
        }
        
        next_state = next_state_map.get(self.cleanup_progress)
        
        if next_state is None:
            return (False, f"error: no default next state defined for {self.cleanup_progress.value}")
        
        return self.transition_to(next_state)


class RootFolderBase(SQLModel):
    simulationdomain_id: int              = Field(foreign_key="simulationdomaindto.id") 
    folder_id: int | None                 = Field(default=None, foreign_key="foldernodedto.id") 
    owner: str                            = Field(default="")
    approvers: str                        = Field(default="")     # comma separated approvers
    path: str                             = Field(default="")     # fullpath including the domain. Maybe only the domains because folder_id points to the foldername
    cycletime: int                        = Field(default=0)      # cycletime for a simulation: from last modified data til initialization of the simulations til it can be cleaned. 0 means not set
    cleanupfrequency: int                 = Field(default=0)      # number of days between cleanup rounds. 0 means not set
    cleanup_round_start_date: date | None = Field(default=None)   # at what date have the user set the cleanup to start start. Can be set into the furture
    cleanup_progress: str                 = Field(default=CleanupProgressEnum.INACTIVE.value)  # current state of the cleanup round
    
    def get_cleanup_configuration(self) -> CleanupConfiguration:
        return CleanupConfiguration(
            self.cycletime, 
            self.cleanupfrequency, 
            self.cleanup_round_start_date,
            CleanupProgressEnum(self.cleanup_progress)
        )
    
    def set_cleanup_configuration(self, cleanup: CleanupConfiguration):
        self.cycletime = cleanup.cycletime
        self.cleanupfrequency = cleanup.cleanupfrequency
        self.cleanup_round_start_date = cleanup.cleanup_start_date
        self.cleanup_progress = cleanup.cleanup_progress.value

class RootFolderDTO(RootFolderBase, table=True):
    id: int | None = Field(default=None, primary_key=True)



@dataclass # these parameters must always be in sync
class Retention:
    retention_id: int
    pathprotection_id: int | None = None
    expiration_date: date | None = None

class FolderNodeBase(SQLModel):
    rootfolder_id: int                    = Field(foreign_key="rootfolderdto.id")
    parent_id: int                        = Field(default=0)  # 0 means no parent
    name: str                             = Field(default="")
    path: str                             = Field(default="")  # full path
    path_ids: str                         = Field(default="")  # full path represented as 0/1/2/3 where each number is the folder id and 0 means root
    nodetype_id: int                      = Field(foreign_key="foldertypedto.id")
    modified_date: date | None            = None # must actually be mandatory but lets wait until the test data is fixed 
    retention_id: int | None              = Field(default=None, foreign_key="retentiontypedto.id")
    pathprotection_id: int | None         = Field(default=None, foreign_key="pathprotectiondto.id")
    expiration_date: date | None          = None

    # we should gravitate towards using the Retention dataclass to enforce consistency between retention_id and pathprotection_id nad possibly expiration_date
    def get_retention(self) -> Retention:
        return Retention(self.retention_id, self.pathprotection_id, self.expiration_date)
    def set_retention(self, retention: Retention):
        self.retention_id = retention.retention_id
        self.pathprotection_id = retention.pathprotection_id
        self.expiration_date = retention.expiration_date

class FolderNodeDTO(FolderNodeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)



# path protection for a specific path in a rootfolder
# the question is whether we need a foreigne key to the folder id 
class PathProtectionBase(SQLModel):
    rootfolder_id: int   = Field(foreign_key="rootfolderdto.id")
    folder_id: int       = Field(foreign_key="foldernodedto.id")
    path: str            = Field(default="")

class PathProtectionDTO(PathProtectionBase, table=True):
    id: int | None       = Field(default=None, primary_key=True)


class RetentionUpdateDTO(SQLModel):
    folder_id: int                  = Field(foreign_key="foldernodedto.id")
    retention_id: int               = Field(foreign_key="retentiontypedto.id")
    pathprotection_id: int          = Field(default=0, foreign_key="pathprotectiondto.id")
    expiration_date: date | None    = None  # calculated expiration date for this folder
    def get_retention(self) -> Retention:
        return Retention(self.retention_id, self.pathprotection_id, self.expiration_date)
    def set_retention(self, retention: Retention):
        self.retention_id = retention.retention_id
        self.pathprotection_id = retention.pathprotection_id
        self.expiration_date = retention.expiration_date
