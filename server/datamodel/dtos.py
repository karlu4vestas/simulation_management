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
    retention_id: int | None = Field(default=None, foreign_key="retentiontypedto.id")

class FolderTypeDTO(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(default="InnerNode")

# The retention type for folders is basically an enumeration of the choice that the user or the system can make. 
# name: The name of the retention type that the user will see on the client.
# is_system_managed: If only the system can change the value then it is considered system-managed.
# display_rank: is uses to determine the order in which retention types are presented to the user on the client side
# We will use the following values for the name field in test and production:
# Cleaned
# MarkedForCleanup 
# CleanupIssue    
# New
# +1Next
# +Q1
# +Q3
# +Q6
# +1Y
# +2Y
# +3Y
# longterm
# path protected

class RetentionTypeBase(SQLModel):
    name: str = Field(default="")
    is_system_managed: bool = Field(default=False)
    display_rank: int = Field(default=0)

#used to CRUD retention types
class RetentionTypeDTO(RetentionTypeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

#Use by the endpoint to only read the retention types
class RetentionTypePublic(RetentionTypeBase):
    id: int
