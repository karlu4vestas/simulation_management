from sqlmodel import Field, SQLModel
from typing import Optional
from datetime import date
from typing import NamedTuple, Literal, Optional
from dataclasses import dataclass



# see values in vts_create_meta_data
class SimulationDomainDTO(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(default="")


# see values in vts_create_meta_data
# "innernode" must exist of all domains and will be applied to all folders that are not simulations
class FolderTypeBase(SQLModel):
    simulation_domain_id: int | None = Field(default=None, foreign_key="simulationdomaindto.id") 
    name: str = Field(default="InnerNode")

class FolderTypeDTO(FolderTypeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

# see values in vts_create_meta_data
class RetentionTypeBase(SQLModel):
    simulation_domain_id: int | None = Field(default=None, foreign_key="simulationdomaindto.id") 
    name: str = Field(default="")
    days_to_cleanup: Optional[int] = None  # days until the simulation can be cleaned. Can be null for path_retention "clean" and "issue"
    is_system_managed: bool = Field(default=False)
    display_rank: int = Field(default=0)

#retention types must be order by increasing days_to_cleanup and then by display_rank
class RetentionTypeDTO(RetentionTypeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

# time from initialization of the simulation til cleanup of the simulation
class CleanupFrequencyBase(SQLModel):
    simulation_domain_id: int | None = Field(default=None, foreign_key="simulationdomaindto.id") 
    name: str = Field(default="InnerNode")
    days: int = Field(default=0)

class CleanupFrequencyDTO(CleanupFrequencyBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

# how long time does the engineer require to analyse a simulation before it expires and can be cleaned
# see values in vts_create_meta_data
class CycleTimeBase(SQLModel):
    simulation_domain_id: int | None = Field(default=None, foreign_key="simulationdomaindto.id") 
    name: str = Field(default="InnerNode")
    days: int = Field(default=0)

class CycleTimeDTO(CycleTimeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)


# The configuration can be used as follow:
#   a) deactivating cleanup is done by setting cleanup_frequency to None
#   b) activating a cleanup round requires that cleanup_frequency is set and that the cycletime is > 0. If cleanup_round_start_date is not set then we assume today
#   c) cycletime can be set with cleanup is inactive cleanup_frequency is None
@dataclass # these parameters are needed together often
class CleanupConfiguration:
    cycletime: int | None                        # days from initialization of the simulations til it can be cleaned
    cleanup_frequency: int | None                # number of days between cleanup rounds
    cleanup_round_start_date: date | None = None # at what date did the current cleanup round start. If not set then we assume today
    def is_valid(self):
        # if cleanup_frequency is set then cycletime must also be set
        is_valid:bool = True if self.cleanup_frequency is None else (self.cycletime is not None and self.cycletime > 0)

        return (is_valid,"ok") if is_valid else (is_valid,"error: cycletime must be set if cleanup_frequency is set")

    #return true if cleanup can be started with this configuration
    def can_start_cleanup(self) -> bool:
        return self.is_valid()[0] and (self.cleanup_frequency is not None)

class RootFolderBase(SQLModel):
    simulation_domain_id: int | None      = Field(default=None, foreign_key="simulationdomaindto.id") 
    folder_id: int | None                 = Field(default=None, foreign_key="foldernodedto.id") 
    owner: str | None                     = None
    approvers: str | None                 = Field(default=None)  # comma separated approvers
    path: str | None                      = None   # fullpath including the domain. Maybe only the domains because folder_id points to the foldername
    cycletime: int | None                 = None   # days from initialization of the simulations til it can be cleaned
    cleanup_frequency: int | None         = None   # number of days between cleanup rounds
    cleanup_round_start_date: date | None = None   # at what date did the current cleanup round start
    def get_cleanup_configuration(self) -> CleanupConfiguration:
        return CleanupConfiguration(self.cycletime, self.cleanup_frequency, self.cleanup_round_start_date)
    def set_cleanup_configuration(self, cleanup: CleanupConfiguration):
        self.cycletime = cleanup.cycletime
        self.cleanup_frequency = cleanup.cleanup_frequency
        self.cleanup_round_start_date = cleanup.cleanup_round_start_date

class RootFolderDTO(RootFolderBase, table=True):
    id: int | None = Field(default=None, primary_key=True)



@dataclass # these parameters must always be in sync
class Retention:
    retention_id: int
    pathprotection_id: int = 0
    expiration_date: date = None

class FolderNodeBase(SQLModel):
    rootfolder_id: int                    = Field(default=None, foreign_key="rootfolderdto.id")
    parent_id: int                        = Field(default=0)  # 0 means no parent
    name: str                             = Field(default="")
    path: str                             = Field(default="")  # full path
    path_ids: str                         = Field(default="")  # full path represented as 0/1/2/3 where each number is the folder id and 0 means root
    nodetype_id: int | None               = Field(default=None, foreign_key="foldertypedto.id")
    modified_date: str | None             = None
    retention_id: int | None              = Field(default=None, foreign_key="retentiontypedto.id")
    pathprotection_id: int | None         = Field(default=None, foreign_key="pathprotectiondto.id")
    expiration_date: str | None           = None
    
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
    rootfolder_id: int   = Field(default=None, foreign_key="rootfolderdto.id")
    folder_id: int       = Field(default=None, foreign_key="foldernodedto.id")
    path: str            = Field(default="")

class PathProtectionDTO(PathProtectionBase, table=True):
    id: int | None = Field(default=None, primary_key=True)


class RetentionUpdateDTO(SQLModel):
    folder_id: int                  = Field(default=None, foreign_key="foldernodedto.id")
    retention_id: int | None        = Field(default=None, foreign_key="retentiontypedto.id")
    pathprotection_id: int | None   = Field(default=None, foreign_key="pathprotectiondto.id")
    expiration_date: date | None    = None  # calculated expiration date for this folder
    def get_retention(self) -> Retention:
        return Retention(self.retention_id, self.pathprotection_id, self.expiration_date)
    def set_retention(self, retention: Retention):
        self.retention_id = retention.retention_id
        self.pathprotection_id = retention.pathprotection_id
        self.expiration_date = retention.expiration_date
