from sqlmodel import Field, SQLModel
from typing import Optional
from datetime import date

# VSM.Datamodel namespace - Python DTOs translated from C#

class RootFolderDTO(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    path: str
    folder_id: int
    owner: str
    approvers: str
    active_cleanup: bool


class FolderNodeDTO(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    parent_id: int = Field(default=0)
    name: str = Field(default="")
    type_id: int = Field(default=0)
    node_attributes: int


class NodeAttributesDTO(SQLModel, table=True):
    node_id: int | None = Field(default=None, primary_key=True)
    retention_id: int = Field(default=0)
    retention_date: str | None = None
    modified: str | None = None


class FolderTypeDTO(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(default="InnerNode")


class RetentionDTO(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(default="")
    is_system_managed: str = Field(default="")
    display_rank: int = Field(default=0)