from __future__ import annotations
from typing import TYPE_CHECKING
from fastapi import HTTPException
from sqlmodel import Field, SQLModel, Session
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum

if TYPE_CHECKING:
    from datamodel.retentions import ExternalRetentionTypes, RetentionTypeDTO, Retention

# see values in vts_create_meta_data
class SimulationDomainDTO(SQLModel, table=True):
    id: int | None  = Field(default=None, primary_key=True)
    name: str       = Field(default="")

class FolderTypeEnum(str, Enum):
    #Enumeration of legal folder type names for simulation domains.
    #'innernode' must exist for all domains and will be applied to all folders that are not simulations.
    INNERNODE   = "innernode"
    SIMULATION  = "simulation"



# see values in vts_create_meta_data
# FolderTypeEnum.INNERNODE must exist of all domains and will be applied to all folders that are not simulations
class FolderTypeBase(SQLModel):
    simulationdomain_id: int        = Field(foreign_key="simulationdomaindto.id") 
    name: str                       = Field(default="")

class FolderTypeDTO(FolderTypeBase, table=True):
    id: int | None                  = Field(default=None, primary_key=True)


# time from initialization of the simulation til cleanup of the simulation
class CleanupFrequencyBase(SQLModel):
    simulationdomain_id: int  = Field(foreign_key="simulationdomaindto.id") 
    name: str                 = Field(default="")
    days: int                 = Field(default=0)

class CleanupFrequencyDTO(CleanupFrequencyBase, table=True):
    id: int | None            = Field(default=None, primary_key=True)


# how long time does the engineer require to analyse a simulation before it expires and can be cleaned
# see values in vts_create_meta_data
class LeadTimeBase(SQLModel):
    simulationdomain_id: int  = Field(foreign_key="simulationdomaindto.id") 
    name: str                 = Field(default="")
    days: int                 = Field(default=0)

class LeadTimeDTO(LeadTimeBase, table=True):
    id: int | None            = Field(default=None, primary_key=True)

# The configuration can be used as follow:
#   a) deactivating cleanup is done by setting frequency to None
#   b) activating a cleanup round requires that frequency is set and that the leadtime is > 0. If cleanup_round_start_date is not set then we assume today
#   c) leadtime can be set with cleanup is inactive frequency is None
#   d) progress to describe where the rootfolder is in the cleanup round: 
#      - inactive
#      - started: the markup phase starts then cleanup round starts so that the user can adjust what simulations will be cleaned
#      - cleaning: this is the last phase in which the actual cleaning happens
#      - finished: the cleanup round is finished and we wait for the next round

#STARTING_RETENTION_REVIEW  is the only phase where the backend is allowed to mark simulation for cleanup.
#Simulations imported in this phase must postpone possible marked for cleanup to the next cleanup round
class CleanupProgress:
    class Progress(str, Enum):
        """Enumeration of cleanup round progress states."""
        INACTIVE                      = "inactive"                      # No cleanup is active
        SCANNING                      = "scanning"                      # This is the only phase where the backend is allowed to mark simulation for cleanup.
        MARKING_FOR_RETENTION_REVIEW  = "marking_for_retention_review"  # This is the only phase where the backend is allowed to mark simulation for cleanup.
        RETENTION_REVIEW              = "retention_review"              # Markup phase - users can adjust what simulations will be cleaned. 
        CLEANING                      = "cleaning"                      # Actual cleaning is happening
        UNMARKING_AFTER_REVIEW        = "unmarking_after_review"        # setting the retention of still marked simulation to a retention after "marked" -that would probably be +7d(next)
        DONE                          = "cleanup_is_done"               # Cleanup round is complete, waiting for next round

    # Define valid state transitions
    valid_transitions: dict["CleanupProgress.Progress", list["CleanupProgress.Progress"]] = {
        Progress.INACTIVE:                     [Progress.SCANNING],
        Progress.SCANNING:                     [Progress.MARKING_FOR_RETENTION_REVIEW, Progress.INACTIVE],
        Progress.MARKING_FOR_RETENTION_REVIEW: [Progress.RETENTION_REVIEW, Progress.INACTIVE],
        Progress.RETENTION_REVIEW:             [Progress.CLEANING, Progress.INACTIVE],
        Progress.CLEANING:                     [Progress.UNMARKING_AFTER_REVIEW, Progress.INACTIVE],
        Progress.UNMARKING_AFTER_REVIEW:       [Progress.DONE, Progress.INACTIVE],
        Progress.DONE:                         [Progress.SCANNING, Progress.INACTIVE]
    }
    
    # Define the natural progression through cleanup states
    next_natural_state: dict["CleanupProgress.Progress", "CleanupProgress.Progress"] = {
        Progress.INACTIVE:                      Progress.SCANNING,
        Progress.SCANNING:                      Progress.MARKING_FOR_RETENTION_REVIEW,
        Progress.MARKING_FOR_RETENTION_REVIEW:  Progress.RETENTION_REVIEW,
        Progress.RETENTION_REVIEW:              Progress.CLEANING,
        Progress.CLEANING:                      Progress.UNMARKING_AFTER_REVIEW,
        Progress.UNMARKING_AFTER_REVIEW:        Progress.DONE,
        Progress.DONE:                          Progress.SCANNING
    }

# The configuration can be used as follow:
#   a) deactivating cleanup is done by setting frequency to None
#   b) activating a cleanup round requires that frequency is set and that the leadtime is > 0. 
#        If cleanup_round_start_date is not set then we assume today
#   c) leadtime: is minimum number of days from last modification of a simulation til it can be cleaned
#        It can be set with cleanup is inactive frequency is None
#   d) progress to describe where the rootfolder is in the cleanup round: 
#      - inactive: going from an activate state to inactive will set the start_date to None. 
#                  If inactivate state and frequency, leadtime and start_date will start the cleanup when the start_date is reached.
#      - started:  the markup phase starts then cleanup round starts so that the user can adjust what simulations will be cleaned
#      - cleaning: this is the last phase in which the actual cleaning happens
#      - finished: the cleanup round is finished and we wait for the next round
class CleanupConfigurationBase(SQLModel):
    """Base class for cleanup configuration."""
    rootfolder_id: int      = Field(default=None, foreign_key="rootfolderdto.id")
    leadtime: int           = Field(default=0)  # days a simulation must be available before cleanup can start. 
    frequency: float        = Field(default=0)  # days to next cleanup round. we use float because automatic testing may require setting it to 1 second like 1/(24*60*60) of a day
    start_date: date | None = Field(default=None)
    progress: str           = Field(default=CleanupProgress.Progress.INACTIVE.value)

class CleanupConfigurationDTO(CleanupConfigurationBase, table=True):
    """Cleanup configuration as separate table."""
    id: int | None = Field(default=None, primary_key=True)

    def is_valid(self) -> bool:
        # has cleanup_frequency and leadtime been set. 
        # If start_date is None then progress must be INACTIVE
        is_valid: bool = (self.frequency is not None and self.frequency > 0) and \
                         (self.leadtime is not None and self.leadtime > 0) and \
                         ( self.progress == CleanupProgress.Progress.INACTIVE.value  or self.start_date is not None )
        return is_valid
    
# path protection for a specific path in a rootfolder
# the question is whether we need a foreigne key to the folder id 
class PathProtectionBase(SQLModel):
    rootfolder_id: int   = Field(foreign_key="rootfolderdto.id")
    folder_id: int       = Field(foreign_key="foldernodedto.id")
    path: str            = Field(default="")

class PathProtectionDTO(PathProtectionBase, table=True):
    id: int | None       = Field(default=None, primary_key=True)



#storage_id:  @TODO the default = "local" must be fixed when moving to remote storage platforms
class RootFolderBase(SQLModel):
    simulationdomain_id: int              = Field(foreign_key="simulationdomaindto.id") 
    folder_id: int | None                 = Field(default=None, foreign_key="foldernodedto.id") 
    storage_id: str                       = Field(default="local")     # storage identifier that will be used by the scan and cleanup agents to pick tasks for their local system.
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

    def save_cleanup_configuration(self, session: Session, cleanup_configuration: CleanupConfigurationDTO) -> CleanupConfigurationDTO:
        #@TODO we should use insert_cleanup_configuration(input.rootfolder.id, cleanup_config_dto)
        if self.cleanup_config_id is not None:
            config = session.get(CleanupConfigurationDTO, self.cleanup_config_id)
            if config is None:
                # Create new cleanup configuration if none exists or if it was deleted
                config = CleanupConfigurationDTO(rootfolder_id=self.id)
                session.add(config)
                session.commit()
                session.refresh(config)
                self.cleanup_config_id = config.id

        if config is None:
            raise HTTPException(status_code=404, detail="unable to save cleanup configuration")

        if config is not None:
            config.leadtime   = cleanup_configuration.leadtime
            config.frequency  = cleanup_configuration.frequency
            config.start_date = cleanup_configuration.start_date
            config.progress   = cleanup_configuration.progress if cleanup_configuration.progress is None else cleanup_configuration.progress

        session.add(self)
        session.commit()
        return config


class RootFolderDTO(RootFolderBase, table=True):
    id: int | None = Field(default=None, primary_key=True)
    

@dataclass
class FileInfo:
    filepath: str
    modified_date: datetime
    nodetype: FolderTypeEnum
    external_retention: "ExternalRetentionTypes"
    id: int = None   # will be used during updates




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
    def get_retention(self) -> "Retention":
        from datamodel.retentions import Retention
        return Retention(self.retention_id, self.pathprotection_id, self.expiration_date)
    
    def set_retention(self, retention: "Retention"):
        self.retention_id = retention.retention_id
        self.pathprotection_id = retention.pathprotection_id
        self.expiration_date = retention.expiration_date
    
    def get_fileinfo(self, nodetype_dict: dict[int, FolderTypeDTO], retention_dict: dict[int, "RetentionTypeDTO"]) -> FileInfo:
        from datamodel.retentions import RetentionTypeDTO
        # Convert FolderNodeBase to FileInfo with all fields populated except id.        
        # Args:
        #     nodetype_dict: Dictionary mapping nodetype_id to FolderTypeDTO
        #     retention_dict: Dictionary mapping retention_id to RetentionTypeDTO
        
        # Get nodetype enum from nodetype_id
        nodetype_dto = nodetype_dict.get(self.nodetype_id)
        if nodetype_dto is None:
            raise ValueError(f"Unknown nodetype_id: {self.nodetype_id}")
        
        # Convert nodetype name to FolderTypeEnum
        try:
            nodetype = FolderTypeEnum(nodetype_dto.name)
        except ValueError:
            # Default to INNERNODE if unknown type
            nodetype = FolderTypeEnum.INNERNODE
        
        # Get retention type and convert to external retention
        retention_dto = retention_dict.get(self.retention_id)
        external_retention = retention_dto.get_external_retention_type() 
        
        return FileInfo(
            filepath=self.path,
            modified_date=self.modified_date,
            nodetype=nodetype,
            external_retention=external_retention,
        )

class FolderNodeDTO(FolderNodeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)
