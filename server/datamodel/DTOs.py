from sqlmodel import Field, SQLModel
from typing import Optional
from datetime import date

# VSM.Datamodel namespace - Python DTOs translated from C#

class RootFolderDTO(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    path: str
    folder_id: int | None = Field(default=None, foreign_key="foldernodedto.id")
    owner: str
    approvers: str | None = Field(default=None)  # comma separated approvers
    active_cleanup: bool

class FolderNodeDTO(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    parent_id: int = Field(default=0)  # 0 means no parent
    name: str = Field(default="")
    type_id: int | None = Field(default=None, foreign_key="foldertypedto.id")
    modified: str | None = None
    retention_date: str | None = None
    retention_id: int | None = Field(default=None, foreign_key="retentiondto.id")

class FolderTypeDTO(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(default="InnerNode")


class RetentionDTO(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(default="")
    is_system_managed: str = Field(default="")
    display_rank: int = Field(default=0)