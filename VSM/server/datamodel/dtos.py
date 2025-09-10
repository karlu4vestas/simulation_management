import string
from sqlmodel import Field, SQLModel
from typing import Optional
from datetime import date

# VSM.Datamodel namespace - Python DTOs translated from C#
#we assume that -1 means unassigned for the db
class RootFolderBase(SQLModel):
    owner: str
    approvers: str | None = Field(default=None)  # comma separated approvers
    cleanup_frequency: str | None = Field(default="inactive") 
    path: str                                    #fullpath including the domain. Maybe only the domains because folder_id points to the foldername
    folder_id: int | None = Field(default=None, foreign_key="foldernodedto.id") 

class RootFolderDTO(RootFolderBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

class RootFolderCreate(RootFolderBase):
    pass

class RootFolderPublic(RootFolderBase):
    id: int


class FolderNodeBase(SQLModel):
    rootfolder_id: int   = Field(default=None, foreign_key="rootfolderdto.id")
    parent_id: int       = Field(default=0)  # 0 means no parent
    name: str            = Field(default="")
    type_id: int | None  = Field(default=None, foreign_key="foldertypedto.id")
    retention_id: int | None = Field(default=None, foreign_key="retentiontypedto.id")
    retention_date: str | None = None
    modified: str | None = None

class FolderNodeDTO(FolderNodeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

class FolderNodePublic(FolderNodeBase):
    id: int | None = Field(default=None, primary_key=True)

class FolderNodeCreate(FolderNodeBase):
    pass


# Values so far:
# InnerNode
# VTS
class FolderTypeBase(SQLModel):
    name: str = Field(default="InnerNode")

class FolderTypeDTO(FolderTypeBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

class FolderTypeCreate(FolderTypeBase):
    pass

#Use for read-only
class FolderTypePublic(FolderTypeBase):
    id: int

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

class RetentionTypeBaseCreate(RetentionTypeBase):
    pass

#Use for read-only
class RetentionTypePublic(RetentionTypeBase):
    id: int


# path protection for a specific path in a rootfolder
# the question is whether we need a foreigne key to the folder id 
class PathProtectionBase(SQLModel):
    rootfolder_id: int   = Field(default=None, foreign_key="rootfolderdto.id")
    folder_id: int       = Field(default=None, foreign_key="foldernodedto.id")
    path: str            = Field(default="")

class PathProtectionDTO(PathProtectionBase, table=True):
    id: int | None = Field(default=None, primary_key=True)

class PathProtectionPublic(PathProtectionBase):
    id: int | None = Field(default=None, primary_key=True)

class PathProtectionCreate(PathProtectionBase):
    pass

class CleanupFrequencyUpdate(SQLModel):
    cleanup_frequency: str
