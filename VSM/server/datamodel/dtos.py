from sqlmodel import Field, SQLModel, Relationship, Session
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

#STARTING_RETENTION_REVIEW  is the only phase where the backend is allowed to mark simulation for cleanup.
#Simulations imported in this phase must postpone possible marked for cleanup to the next cleanup round
class CleanupProgress:
    class ProgressEnum(str, Enum):
        """Enumeration of cleanup round progress states."""
        INACTIVE = "inactive"    # No cleanup is active
        STARTING_RETENTION_REVIEW   = "starting_retention_review"      # This is the only phase where the backend is allowed to mark simulation for cleanup.
        RETENTION_REVIEW            = "retention_review"      # Markup phase - users can adjust what simulations will be cleaned. 
        CLEANING = "cleaning"    # Actual cleaning is happening
        FINISHED = "finished"    # Cleanup round is complete, waiting for next round

    # Define valid state transitions
    valid_transitions: dict["CleanupProgress.ProgressEnum", list["CleanupProgress.ProgressEnum"]] = {
        ProgressEnum.INACTIVE: [ProgressEnum.STARTING_RETENTION_REVIEW],
        ProgressEnum.STARTING_RETENTION_REVIEW: [ProgressEnum.RETENTION_REVIEW],
        ProgressEnum.RETENTION_REVIEW: [ProgressEnum.CLEANING, ProgressEnum.INACTIVE],
        ProgressEnum.CLEANING: [ProgressEnum.FINISHED, ProgressEnum.INACTIVE],
        ProgressEnum.FINISHED: [ProgressEnum.INACTIVE, ProgressEnum.STARTING_RETENTION_REVIEW],
    }
    
    # Define the natural progression through cleanup states
    next_natural_state: dict["CleanupProgress.ProgressEnum", "CleanupProgress.ProgressEnum"] = {
        ProgressEnum.INACTIVE: ProgressEnum.STARTING_RETENTION_REVIEW,
        ProgressEnum.STARTING_RETENTION_REVIEW: ProgressEnum.RETENTION_REVIEW,
        ProgressEnum.RETENTION_REVIEW: ProgressEnum.CLEANING,
        ProgressEnum.CLEANING: ProgressEnum.FINISHED,
        ProgressEnum.FINISHED: ProgressEnum.INACTIVE,
    }

# The configuration can be used as follow:
#   a) deactivating cleanup is done by setting cleanupfrequency to None
#   b) activating a cleanup round requires that cleanupfrequency is set and that the cycletime is > 0. If cleanup_round_start_date is not set then we assume today
#   c) cycletime can be set with cleanup is inactive cleanupfrequency is None
#   d) cleanup_progress to describe where the rootfolder is in the cleanup round: 
#      - inactive
#      - started: the markup phase starts then cleanup round starts so that the user can adjust what simulations will be cleaned
#      - cleaning: this is the last phase in which the actual cleaning happens
#      - finished: the cleanup round is finished and we wait for the next round
class CleanupConfigurationBase(SQLModel):
    """Base class for cleanup configuration."""
    rootfolder_id: int              = Field(default=None, foreign_key="rootfolderdto.id")
    cycletime: int                  = Field(default=0)
    cleanupfrequency: int           = Field(default=0)
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
                         ((self.cleanup_progress == CleanupProgress.ProgressEnum.INACTIVE.value and self.cleanup_start_date is None) \
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

        has_valid_progress = self.cleanup_progress in [CleanupProgress.ProgressEnum.INACTIVE.value, CleanupProgress.ProgressEnum.FINISHED.value]
        return has_valid_progress
    
    def is_in_cleanup_round(self) -> bool:
        return self.cleanup_progress in [CleanupProgress.ProgressEnum.RETENTION_REVIEW.value, CleanupProgress.ProgressEnum.CLEANING.value]
    
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


class RootFolderBase(SQLModel):
    simulationdomain_id: int              = Field(foreign_key="simulationdomaindto.id") 
    folder_id: int | None                 = Field(default=None, foreign_key="foldernodedto.id") 
    owner: str                            = Field(default="")
    approvers: str                        = Field(default="")     # comma separated approvers
    path: str                             = Field(default="")     # fullpath including the domain. Maybe only the domains because folder_id points to the foldername
    cleanup_config_id: int | None         = Field(default=None, foreign_key="cleanupconfigurationdto.id") 

    def get_cleanup_configuration(self, session: Session) -> CleanupConfigurationDTO:
        """Get or create cleanup configuration."""
        if self.cleanup_config_id is not None:
            cleanup = session.get(CleanupConfigurationDTO, self.cleanup_config_id)
            if cleanup is not None:
                return cleanup
        
        # Create new cleanup configuration if none exists or if it was deleted
        new_cleanup = CleanupConfigurationDTO(rootfolder_id=self.id)
        session.add(new_cleanup)
        session.commit()
        session.refresh(new_cleanup)
        self.cleanup_config_id = new_cleanup.id
        session.add(self)
        session.commit()
        return new_cleanup

class RootFolderDTO(RootFolderBase, table=True):
    id: int | None = Field(default=None, primary_key=True)
    
    # Relationship to CleanupConfigurationDTO (one-to-one)
    # lazy="noload" prevents automatic loading when accessing this attribute,
    # avoiding DetachedInstanceError when session is closed
    #cleanup_config: Optional["CleanupConfigurationDTO"] = Relationship(
    #    back_populates="rootfolder",
    #    sa_relationship_kwargs={"uselist": False, "lazy": "noload"}
    #)
    
    #def ensure_cleanup_config(self, session) -> "CleanupConfigurationDTO":
    #    """Get or create cleanup configuration."""
    #    if self.cleanup_config is None:
    #        self.cleanup_config = CleanupConfigurationDTO(rootfolder_id=self.id)
    #        session.add(self.cleanup_config)
    #    return self.cleanup_config



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
