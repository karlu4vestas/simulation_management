from fastapi import HTTPException
from sqlmodel import Field, SQLModel, Relationship, Session
from typing import Optional
from datetime import date
from typing import Optional
from dataclasses import dataclass
from enum import Enum
from cleanup_cycle.cleanup_dtos import CleanupConfigurationDTO

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
    UNDEFINED = None
    Issue = "Issue"
    Clean = "Clean"
    Missing = "Missing"



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
    storage_id: str                       = Field(default="")     # storage identifier that will be used by the scan and cleanup agents to pick tasks for their local system 
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
            config.cycletime          = cleanup_configuration.cycletime
            config.cleanupfrequency   = cleanup_configuration.cleanupfrequency
            config.cleanup_start_date = cleanup_configuration.cleanup_start_date
            config.cleanup_progress   = cleanup_configuration.cleanup_progress if cleanup_configuration.cleanup_progress is None else cleanup_configuration.cleanup_progress

        session.add(self)
        session.commit()
        return config


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



@dataclass
class Retention:
    """Core retention data structure for folder retention information."""
    retention_id: int
    pathprotection_id: int | None = None
    expiration_date: date | None = None

@dataclass
class FolderRetention(Retention):
    """
    DTO for updating folder retention from client.
    Inherits all retention fields and adds folder_id for API routing.
    Since this IS-A Retention, it can be passed directly to functions expecting Retention objects.
    """
    folder_id: int = 0
    
    def update_retention_fields(self, retention: Retention) -> None:
        """Update the retention fields from a Retention object."""
        self.retention_id = retention.retention_id
        self.pathprotection_id = retention.pathprotection_id
        self.expiration_date = retention.expiration_date


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


@dataclass
class FileInfo:
    filepath: str
    modified_date: date
    nodetype: FolderTypeEnum
    external_retention: ExternalRetentionTypes
    id: int = None   # will be used during updates
