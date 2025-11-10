from fastapi import HTTPException
from sqlmodel import Field, SQLModel, Session
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from cleanup_cycle.cleanup_dtos import CleanupConfigurationDTO
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





# The configuration can be used as follow:
#   a) deactivating cleanup is done by setting cleanupfrequency to None
#   b) activating a cleanup round requires that cleanupfrequency is set and that the cycletime is > 0. If cleanup_round_start_date is not set then we assume today
#   c) cycletime can be set with cleanup is inactive cleanupfrequency is None
#   d) cleanup_progress to describe where the rootfolder is in the cleanup round: 
#      - inactive
#      - started: the markup phase starts then cleanup round starts so that the user can adjust what simulations will be cleaned
#      - cleaning: this is the last phase in which the actual cleaning happens
#      - finished: the cleanup round is finished and we wait for the next round


#storage_id:  @TODO the default = "local" must eb fixed when moving to other remote platofrms
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
            config.cycletime          = cleanup_configuration.cycletime
            config.cleanupfrequency   = cleanup_configuration.cleanupfrequency
            config.cleanup_start_date = cleanup_configuration.cleanup_start_date
            config.cleanup_progress   = cleanup_configuration.cleanup_progress if cleanup_configuration.cleanup_progress is None else cleanup_configuration.cleanup_progress

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
    external_retention: ExternalRetentionTypes
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
    def get_retention(self) -> Retention:
        return Retention(self.retention_id, self.pathprotection_id, self.expiration_date)
    def set_retention(self, retention: Retention):
        self.retention_id = retention.retention_id
        self.pathprotection_id = retention.pathprotection_id
        self.expiration_date = retention.expiration_date
    
    def get_fileinfo(self, nodetype_dict: dict[int, FolderTypeDTO], retention_dict: dict[int, RetentionTypeDTO]) -> FileInfo:
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
