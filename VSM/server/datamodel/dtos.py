import string
from sqlmodel import Field, SQLModel
from typing import Optional
from datetime import date



# see values in vts_create_meta_data
class SimulationDomainDTO(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(default="")


# see values in vts_create_meta_data
class FolderTypeBase(SQLModel):
    simulation_domain_id: int | None = Field(default=None, foreign_key="simulationdomaindto.id") 
    name: str = Field(default="InnerNode")

class FolderTypeDTO(FolderTypeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

# how of shal the system clean up simulations that have expired
# see values in vts_create_meta_data
class CleanupFrequencyBase(SQLModel):
    simulation_domain_id: int | None = Field(default=None, foreign_key="simulationdomaindto.id") 
    name: str = Field(default="InnerNode")
    days: int = Field(default=0)

class CleanupFrequencyDTO(CleanupFrequencyBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

# how long time does the engineer require to analyse a simulation before it expires and can be cleaned
# see values in vts_create_meta_data
class DaysToAnalyseBase(SQLModel):
    simulation_domain_id: int | None = Field(default=None, foreign_key="simulationdomaindto.id") 
    name: str = Field(default="InnerNode")
    days: int = Field(default=0)

class DaysToAnalyseDTO(DaysToAnalyseBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

# see values in vts_create_meta_data
class RetentionTypeBase(SQLModel):
    simulation_domain_id: int | None = Field(default=None, foreign_key="simulationdomaindto.id") 
    name: str = Field(default="")
    days_to_cleanup: Optional[int] = None  # null means path protected or clean or issue
    is_system_managed: bool = Field(default=False)
    display_rank: int = Field(default=0)

#used to CRUD retention types
class RetentionTypeDTO(RetentionTypeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)




# VSM.Datamodel namespace - Python DTOs translated from C#
#we assume that -1 means unassigned for the db
class RootFolderBase(SQLModel):
    simulation_domain_id: int | None   = Field(default=None, foreign_key="simulationdomaindto.id") 
    folder_id: int | None              = Field(default=None, foreign_key="foldernodedto.id") 
    owner: str | None                  = None
    approvers: str | None              = Field(default=None)  # comma separated approvers
    path: str | None                   = None   # fullpath including the domain. Maybe only the domains because folder_id points to the foldername
    days_to_analyse: int | None        = None   # how many days to keep simulations in analyse state. It starts at simulations modified date
    cleanup_status_date: str | None    = None   # at what date did the current cleanup round start
    cleanup_frequency_days: int | None = None

class RootFolderDTO(RootFolderBase, table=True):
    id: int | None = Field(default=None, primary_key=True)



class FolderNodeBase(SQLModel):
    rootfolder_id: int                    = Field(default=None, foreign_key="rootfolderdto.id")
    parent_id: int                        = Field(default=0)  # 0 means no parent
    name: str                             = Field(default="")
    path: str                             = Field(default="")  # full path
    path_ids: str                         = Field(default="")  # full path represented as 0/1/2/3 where each number is the folder id and 0 means root
    type_id: int | None                   = Field(default=None, foreign_key="foldertypedto.id")
    retention_id: int | None              = Field(default=None, foreign_key="retentiontypedto.id")
    pathprotection_id: int | None         = Field(default=None, foreign_key="pathprotectiondto.id")
    modified_date: str | None             = None
    expiration_date: str | None           = None

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


class CleanupFrequencyUpdate(SQLModel):
    cleanup_frequency: str


class RetentionUpdateDTO(SQLModel):
    folder_id: int                  = Field(default=None, foreign_key="foldernodedto.id")
    retention_id: int | None        = Field(default=None, foreign_key="retentiontypedto.id")
    pathprotection_id: int | None   = Field(default=None, foreign_key="pathprotectiondto.id")
    expiration_date: date | None    = None  # calculated expiration date for this folder
