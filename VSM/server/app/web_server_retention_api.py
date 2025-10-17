from dataclasses import dataclass
from typing import Optional
from datetime import date, timedelta
from bisect import bisect_left
from fastapi import HTTPException
from sqlmodel import Session, select
from datamodel.dtos import FolderTypeDTO, FolderTypeEnum, RetentionTypeDTO, RootFolderDTO, FolderNodeDTO, RetentionUpdateDTO
from server.db.database import Database
from sqlalchemy import func, case
from app.web_api import app, read_folder_type_dict_pr_domain_id, read_pathprotections, read_rootfolder_retentiontypes_dict, read_rootfolder_numeric_retentiontypes_dict 
from datamodel.dtos import PathProtectionDTO, Retention
