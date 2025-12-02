import os
from datetime import datetime
from typing import Literal, Optional
from sqlalchemy import func
from sqlmodel import Session, func, select
from fastapi import Query, HTTPException
from db.database import Database
from datamodel import dtos


 
#-----------------start retrieval of metadata for a simulation domain -------------------
simulation_domain_name: Literal["vts"]
simulation_domain_names = ["vts"]  # Define the allowed domain names

def read_simulation_domains() -> list[dtos.SimulationDomainDTO]:
    with Session(Database.get_engine()) as session:
        simulation_domains = session.exec(select(dtos.SimulationDomainDTO)).all()
        if not simulation_domains:
            raise HTTPException(status_code=404, detail="SimulationDomain not found")
        return simulation_domains

def read_simulation_domains_dict():
    return {domain.name.lower(): domain for domain in read_simulation_domains()}


def read_simulation_domain_by_name(domain_name: str):
    with Session(Database.get_engine()) as session:
        simulation_domain = session.exec(select(dtos.SimulationDomainDTO).where(dtos.SimulationDomainDTO.name == domain_name)).first()
        if not simulation_domain:
            raise HTTPException(status_code=404, detail=f"SimulationDomain for {domain_name}not found")
        return simulation_domain

def read_retentiontypes_by_domain_id(simulationdomain_id: int):
    with Session(Database.get_engine()) as session:
        retention_types = session.exec(select(dtos.RetentionTypeDTO).where(dtos.RetentionTypeDTO.simulationdomain_id == simulationdomain_id)).all()
        if not retention_types or len(retention_types) == 0:
            raise HTTPException(status_code=404, detail="retentiontypes not found")
        return retention_types

def read_retentiontypes_dict_by_domain_id(simulationdomain_id: int) -> dict[str, dtos.RetentionTypeDTO]:
    return {retention.name.lower(): retention for retention in read_retentiontypes_by_domain_id(simulationdomain_id)}

def read_frequency_by_domain_id(simulationdomain_id: int):
    with Session(Database.get_engine()) as session:
        frequency = session.exec(select(dtos.CleanupFrequencyDTO).where(dtos.CleanupFrequencyDTO.simulationdomain_id == simulationdomain_id)).all()
        if not frequency or len(frequency) == 0:
            raise HTTPException(status_code=404, detail="Frequency not found")
        return frequency

def read_frequency_name_dict_by_domain_id(simulationdomain_id: int):
    return {cleanup.name.lower(): cleanup for cleanup in read_frequency_by_domain_id(simulationdomain_id)}


def read_folder_types_pr_domain_id(simulationdomain_id: int):    
    with Session(Database.get_engine()) as session:
        folder_types = session.exec(select(dtos.FolderTypeDTO).where(dtos.FolderTypeDTO.simulationdomain_id == simulationdomain_id)).all()
        if not folder_types or len(folder_types) == 0:
            raise HTTPException(status_code=404, detail="foldertypes not found")
        return folder_types

def read_folder_type_dict_pr_domain_id(simulationdomain_id: int) -> dict[str, dtos.FolderTypeDTO]:
    return {folder_type.name.lower(): folder_type for folder_type in read_folder_types_pr_domain_id(simulationdomain_id)}

def read_cycle_time_by_domain_id(simulationdomain_id: int):
    with Session(Database.get_engine()) as session:
        cycle_time = session.exec(select(dtos.LeadTimeDTO).where(dtos.LeadTimeDTO.simulationdomain_id == simulationdomain_id)).all()
        if not cycle_time or len(cycle_time) == 0:
            raise HTTPException(status_code=404, detail="LeadTime not found")
        return cycle_time

def read_cycle_time_dict_by_domain_id(simulationdomain_id: int):
    return {cycle.name.lower(): cycle for cycle in read_cycle_time_by_domain_id(simulationdomain_id)}
#-----------------end retrieval of metadata for a simulation domain -------------------


#-----------------start maintenance of rootfolders and information under it -------------------
def read_rootfolders(simulationdomain_id: int, initials: Optional[str] = Query(default="")):
    if initials is None or simulationdomain_id is None or simulationdomain_id == 0:
        raise HTTPException(status_code=404, detail="root_folders not found. you must provide simulation domain and initials")
    rootfolders = read_rootfolders_by_domain_and_initials(simulationdomain_id, initials)
    return rootfolders

def read_rootfolders_by_domain_and_initials(simulationdomain_id: int, initials: str= None)->list[dtos.RootFolderDTO]:
    if simulationdomain_id is None or simulationdomain_id == 0:
        raise HTTPException(status_code=404, detail="root_folders not found. you must provide simulation domain and initials")

    with Session(Database.get_engine()) as session:
        if type(initials) == str and initials is not None:
            rootfolders = session.exec(
                select(dtos.RootFolderDTO).where( (dtos.RootFolderDTO.simulationdomain_id == simulationdomain_id) &
                    ((dtos.RootFolderDTO.owner == initials) | (dtos.RootFolderDTO.approvers.like(f"%{initials}%")))
                )
            ).all()
        else:
            rootfolders = session.exec(
                select(dtos.RootFolderDTO).where( (dtos.RootFolderDTO.simulationdomain_id == simulationdomain_id) )
            ).all()

        return rootfolders

def exist_rootfolder(rootfolder:dtos.RootFolderDTO):
    if (rootfolder is None) or (rootfolder.simulationdomain_id is None) or (rootfolder.simulationdomain_id == 0):
        raise HTTPException(status_code=404, detail="You must provide a valid simulationdomain_id to create a rootfolder")

    with Session(Database.get_engine()) as session:
        #verify if the rootfolder already exists        
        existing_rootfolder:dtos.RootFolderDTO = session.exec(select(dtos.RootFolderDTO).where(
                (dtos.RootFolderDTO.simulationdomain_id == rootfolder.simulationdomain_id) & 
                (dtos.RootFolderDTO.path == rootfolder.path)
            )).first()
        return existing_rootfolder is not None

def insert_rootfolder(rootfolder:dtos.RootFolderDTO):
    if (rootfolder is None) or (rootfolder.simulationdomain_id is None) or (rootfolder.simulationdomain_id == 0):
        raise HTTPException(status_code=404, detail="You must provide a valid simulationdomain_id to create a rootfolder")

    with Session(Database.get_engine()) as session:
        #verify if the rootfolder already exists
        existing_rootfolder:dtos.RootFolderDTO = session.exec(select(dtos.RootFolderDTO).where(
                (dtos.RootFolderDTO.simulationdomain_id == rootfolder.simulationdomain_id) & 
                (dtos.RootFolderDTO.path == rootfolder.path)
            )).first()
        if existing_rootfolder:
            return existing_rootfolder

        session.add(rootfolder)
        session.commit()
        session.refresh(rootfolder)

        if (rootfolder.id is None) or (rootfolder.id == 0):
            raise HTTPException(status_code=404, detail=f"Failed to provide {rootfolder.path} with an id")
        return rootfolder
    
def read_rootfolder_by_id(rootfolder_id: int):
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")
        return rootfolder

def read_rootfolder_retentiontypes(rootfolder_id: int):
    retention_types:list[dtos.RetentionTypeDTO] = list(read_rootfolder_retentiontypes_dict(rootfolder_id).values())
    return retention_types

def read_rootfolder_retentiontypes_dict(rootfolder_id: int)-> dict[str, dtos.RetentionTypeDTO]:
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")

        retention_types:dict[str,dtos.RetentionTypeDTO] = read_retentiontypes_dict_by_domain_id(rootfolder.simulationdomain_id)
        if not retention_types:
            raise HTTPException(status_code=404, detail="retentiontypes not found")
        
        if not retention_types.get("path", None):
            raise HTTPException(status_code=500, detail=f"Unable to retrieve node_type_id=vts_simulation for {rootfolder.id}")
        
        return retention_types

def read_rootfolder_numeric_retentiontypes_dict(rootfolder_id: int) -> dict[str, dtos.RetentionTypeDTO]:
    retention_types_dict:dict[str, dtos.RetentionTypeDTO] = read_rootfolder_retentiontypes_dict(rootfolder_id)
    #filter to keep only retentions with at lead_time
    return {key:retention for key,retention in retention_types_dict.items() if retention.days_to_cleanup is not None}

def read_folders( rootfolder_id: int ):
    with Session(Database.get_engine()) as session:
        folders = session.exec(select(dtos.FolderNodeDTO).where(dtos.FolderNodeDTO.rootfolder_id == rootfolder_id)).all()
        return folders



from datetime import datetime, timezone
import zoneinfo

def to_utc(dt, default_timezone="UTC"):
    if dt is None:
        return None
    elif dt.tzinfo is None:
        # Assign a timezone to naive datetime
        tz = zoneinfo.ZoneInfo(default_timezone)
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(timezone.utc)

def get_cleanup_configuration_by_rootfolder_id(rootfolder_id: int)-> dtos.CleanupConfigurationDTO:
    with Session(Database.get_engine()) as session:
        rootfolder:dtos.RootFolderDTO = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")

        cleanup_configuration = rootfolder.get_cleanup_configuration(session)
        if not cleanup_configuration:
            raise HTTPException(status_code=404, detail="cleanup_configuration not found")
        cleanup_configuration.start_date = cleanup_configuration.start_date
    return cleanup_configuration

#insert the cleanup configuration for a rootfolder and update the rootfolder to point to the cleanup configuration
def insert_or_update_cleanup_configuration(rootfolder_id:int, cleanup_config: dtos.CleanupConfigurationDTO):
    if (rootfolder_id is None) or (rootfolder_id == 0):
        raise HTTPException(status_code=404, detail="You must provide a valid rootfolder_id to create a cleanup configuration")
    with Session(Database.get_engine()) as session:
        #verify if the rootfolder already exists
        rootfolder:dtos.RootFolderDTO = session.exec(select(dtos.RootFolderDTO).where((dtos.RootFolderDTO.id == rootfolder_id))).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail=f"Failed to find rootfolder with id {rootfolder_id} to create a cleanup configuration")

        existing_cleanup_cfg:dtos.CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session)
        if existing_cleanup_cfg is None:
            raise HTTPException(status_code=404, detail=f"Failed to find existing cleanup configuration for rootfolder with id {rootfolder_id} to update")
        
        existing_cleanup_cfg.lead_time  = cleanup_config.lead_time
        existing_cleanup_cfg.frequency  = cleanup_config.frequency
        existing_cleanup_cfg.start_date = None if cleanup_config.start_date is None else datetime.fromisoformat(cleanup_config.start_date.replace("Z", "+00:00")) #datetime.fromisoformat(ts.replace("Z", "+00:00")) #cleanup_config.start_date #fastapi/pydantics handles the conversion 
        existing_cleanup_cfg.progress   = dtos.CleanupProgress.Progress.INACTIVE # any change to the cleanupå config resets progress to INACTIVE


        # Import at runtime to avoid circular dependency
        from cleanup.scheduler import CleanupScheduler
        CleanupScheduler.deactivate_calendar(cleanup_config.rootfolder_id)
        
        session.add(existing_cleanup_cfg)
        session.commit()
        session.refresh(existing_cleanup_cfg)

        cleanup_config.id       = existing_cleanup_cfg.id if existing_cleanup_cfg else None
        cleanup_config.progress = existing_cleanup_cfg.progress        
        return cleanup_config
    
# def update_cleanup_configuration_by_rootfolder_id(rootfolder_id: int, cleanup_configuration: dtos.CleanupConfigurationDTO):
#     #is_valid = cleanup_configuration.is_valid()
#     #if not is_valid:
#     #    raise HTTPException(status_code=404, detail=f"for rootfolder {rootfolder_id}: update of cleanup_configuration failed")

#     #now the configuration is consistent
#     with Session(Database.get_engine()) as session:
#         rootfolder:dtos.RootFolderDTO = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
#         if not rootfolder:
#             raise HTTPException(status_code=404, detail="rootfolder not found")
      
#         cleanup_config: dtos.CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session)
#         # Update the DTO with values from the incoming dataclass
#         # Any change will reset the progress to INACTIVE and deactivate all active calender and tasks 
#         cleanup_config.lead_time   = cleanup_configuration.lead_time
#         cleanup_config.frequency  = cleanup_configuration.frequency
#         cleanup_config.start_date = cleanup_configuration.start_date
#         cleanup_config.progress   = dtos.CleanupProgress.Progress.INACTIVE 
#         rootfolder.save_cleanup_configuration(session, cleanup_config)

#         return {"message": f"for rootfolder {rootfolder_id}: update of cleanup configuration {cleanup_config.id} "}


# # --------------- start not in use now - will be used for optimization -------------------

# import io
# import csv
# import csv
# from zstandard import ZstdCompressor

# # read folders into csv for efficient transmission
# def read_folders_csv(rootfolder_id:int):
#     def generate_csv():
#         engine = Database.get_engine()
#         conn = engine.raw_connection()
#         if conn is None:
#             return io.StringIO()
#         else :
#             cursor = conn.cursor()
            
#             # Get column names
#             cursor.execute("PRAGMA table_info(foldernodedto)")
#             columns = [row[1] for row in cursor.fetchall()]
            
#             # Create CSV header
#             output = io.StringIO()
#             writer = csv.writer(output)
#             writer.writerow(columns)
#             yield output.getvalue()
            
#             # Stream data in chunks
#             chunk_size = 10000
#             offset = 0
            
#             while True:
#                 cursor.execute(f"SELECT * FROM foldernodedto LIMIT {chunk_size} OFFSET {offset}")
#                 rows = cursor.fetchall()
                
#                 if not rows:
#                     conn.close()
#                     conn=None
#                     break
                
#                 # Write chunk to CSV
#                 output = io.StringIO()
#                 writer = csv.writer(output)
#                 writer.writerows(rows)
#                 yield output.getvalue()
                
#                 offset += chunk_size
        
#     return StreamingResponse(
#         generate_csv(), 
#         media_type="text/csv", 
#         headers={"Content-Disposition": "attachment; filename=folders.csv"}
#     )
# def read_folders_csv_zstd(rootfolder_id: int):
#     def generate_compressed_csv():
#         compressor = ZstdCompressor(level=3)
#         engine = Database.get_engine()
        
#         conn = engine.raw_connection()
#         if conn is None:
#             return io.StringIO()
#         else :
#             cursor = conn.cursor()
            
#             # Get column names and create header
#             cursor.execute("PRAGMA table_info(foldernodedto)")
#             columns = [row[1] for row in cursor.fetchall()]
            
#             output = io.StringIO()
#             writer = csv.writer(output)
#             writer.writerow(columns)
#             yield compressor.compress(output.getvalue().encode('utf-8'))
            
#             # Stream data in chunks
#             chunk_size = 10000
#             offset = 0
            
#             while True:
#                 cursor.execute(f"SELECT * FROM foldernodedto LIMIT {chunk_size} OFFSET {offset}")
#                 rows = cursor.fetchall()
                
#                 if not rows:
#                     conn.close()
#                     conn=None
#                     break
                
#                 # Write chunk to CSV and compress
#                 output = io.StringIO()
#                 writer = csv.writer(output)
#                 writer.writerows(rows)
#                 yield compressor.compress(output.getvalue().encode('utf-8'))
                
#                 offset += chunk_size
        
#     return StreamingResponse(
#         generate_compressed_csv(), 
#         media_type="application/zstd",
#         headers={
#             "Content-Disposition": "attachment; filename=folders.csv.zst",
#             "Content-Encoding": "zstd"
#         }
#     )
# # --------------- end start not in use now - will be used for optimization -------------------

def read_pathprotections( rootfolder_id: int )-> list[dtos.PathProtectionDTO]:
    with Session(Database.get_engine()) as session:
        paths = session.exec(select(dtos.PathProtectionDTO).where(dtos.PathProtectionDTO.rootfolder_id == rootfolder_id)).all()
        return paths

# @TODO we should consider to enforce the changed path protection on existing folders
# at present it is the clients responsibility to so and communicate it in "def change_retentions"
def add_pathprotection(rootfolder_id:int, path_protection:dtos.PathProtectionDTO):
    #print(f"Adding path protection {path_protection}")
    with Session(Database.get_engine()) as session:
        # Check if path protection already exists for this path in this rootfolder
        existing_protection = session.exec(
            select(dtos.PathProtectionDTO).where(
                (dtos.PathProtectionDTO.rootfolder_id == rootfolder_id) & 
                (dtos.PathProtectionDTO.folder_id == path_protection.folder_id)
            )
        ).first()
        
        if existing_protection:
            raise HTTPException(status_code=409, detail="Path protection already exists for this path")
        
        # Create new path protection
        new_protection = dtos.PathProtectionDTO(
            rootfolder_id = rootfolder_id,
            folder_id     = path_protection.folder_id,
            path          = path_protection.path
        )
        
        session.add(new_protection)
        session.commit()
        session.refresh(new_protection)
        path_protection.id = new_protection.id
        path_protection.rootfolder_id = rootfolder_id

        return path_protection

def add_pathprotection_by_paths(rootfolder_id:int, paths:list[str]):
    # step 1: find the folder nodes by path
    # step 2: create list[dtos.PathProtectionDTO] with rootfolder_id, folder_id, path
    # step 3: call add_path_protection for each dtos.PathProtectionDTO
    # Note: Uses case-insensitive path matching and raises exceptions for errors
    if not paths:
        raise HTTPException(status_code=400, detail="No paths provided")
    
    with Session(Database.get_engine()) as session:
        # Verify rootfolder exists
        rootfolder = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")
        
        # Step 1: Find the folder nodes by path (case-insensitive)
        lower_case_paths = [path.lower() for path in paths]
        existing_folders = session.exec(
            select(dtos.FolderNodeDTO).where(
                (dtos.FolderNodeDTO.rootfolder_id == rootfolder_id) &
                (func.lower(dtos.FolderNodeDTO.path).in_(lower_case_paths))
            )
        ).all()
        
        # Create a mapping from path to folder for fast lookup
        path_to_folder = {folder.path.lower(): folder for folder in existing_folders}
        
        # Track results
        added_protections = []
        failed_paths = []
        
        # Step 2 & 3: Create and add path protections
        for path in paths:
            folder = path_to_folder.get(path.lower())
            
            if not folder:
                failed_paths.append({"path": path, "reason": "Folder not found"})
                continue
            
            # Check if path protection already exists
            existing_protection = session.exec(
                select(dtos.PathProtectionDTO).where(
                    (dtos.PathProtectionDTO.rootfolder_id == rootfolder_id) & 
                    (dtos.PathProtectionDTO.folder_id == folder.id)
                )
            ).first()
            
            # some path_protections failed. Instead of raise an exception we return the number of failed_paths in the results
            if existing_protection:
                # Use existing protection instead of creating a new one
                added_protections.append({"path": folder.path, "folder_id": folder.id, "already_existed": True})
                continue
            
            # Create new path protection
            new_protection = dtos.PathProtectionDTO(
                rootfolder_id=rootfolder_id,
                folder_id=folder.id,
                path=folder.path
            )
            
            session.add(new_protection)
            added_protections.append({"path": folder.path, "folder_id": folder.id, "already_existed": False})

        
        # Commit all new protections
        session.commit()
        
        return {
            "message": f"Added {len(added_protections)} path protection(s) for rootfolder {rootfolder_id}",
            "added_count": len(added_protections),
            "added_protections": added_protections,
            "failed_paths": failed_paths
        }


def apply_pathprotections(rootfolder_id:int)-> dict[str, int]:
    # ensure that all existing path protections for the root folder has been applied to the folders
    # step 1: find the path retention id
    # step 2: get the simulation nodetype id (only simulation nodes get retention, not innernodes)
    # step 3: find all path protections for the rootfolder
    # step 4: sort them by path depth so the most specific path protections takes priority
    #         Sort by increasing number of segments and apply in that order
    #         Most specific protections are applied last, overriding higher-level path protections
    # step 5: Apply path protection by setting retention_id to path retention id and pathprotection_id
    # step 6: If there is any folder with path retention whose pathprotection_id is no longer valid (protection was deleted).
    #         Then their retention must be reset to undefined_retention_id
    # step 7: return number of folders modified
    
    with Session(Database.get_engine()) as session:
        # Verify rootfolder exists
        rootfolder = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")
        
        # Step 1: Find the path retention id
        path_retention_dict:dict[str, dtos.RetentionTypeDTO] = read_rootfolder_retentiontypes_dict(rootfolder_id)
        path_retention_id:int = path_retention_dict["path"].id if "path" in path_retention_dict else 0
        undefined_retention_id:int = path_retention_dict["?"].id if "?" in path_retention_dict else 0
        if path_retention_id == 0:
            raise HTTPException(status_code=500, detail=f"Path retention type not found for rootfolder {rootfolder_id}")
        
        # Step 2: Get the simulation nodetype id (only SIMULATION nodes, not INNERNODE)
        nodetype_simulation_id:int = read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)[dtos.FolderTypeEnum.SIMULATION].id
        
        # Step 3: Find all path protections for the rootfolder
        path_protections = session.exec( select(dtos.PathProtectionDTO).where( dtos.PathProtectionDTO.rootfolder_id == rootfolder_id) ).all()        
        if not path_protections:
            return {"message": f"No path protections found for rootfolder {rootfolder_id}", "folders_modified": 0}
        
        # Step 4: Sort by path depth (number of segments) - least specific first, most specific last
        sorted_protections = sorted(path_protections, key=lambda p: p.path.count('/'))
        
        # Step 5: Apply path protections to all SIMULATION folders under each protected path
        folders_modified = 0
        
        for protection in sorted_protections:
            # Find all SIMULATION folders that start with this path (case-insensitive)
            # Using LIKE with wildcards to match the path and all subpaths
            matching_folders = session.exec(
                select(dtos.FolderNodeDTO).where(
                    (dtos.FolderNodeDTO.rootfolder_id == rootfolder_id) &
                    (dtos.FolderNodeDTO.nodetype_id == nodetype_simulation_id) &
                    ((func.lower(dtos.FolderNodeDTO.path) == protection.path.lower()) |
                     (func.lower(dtos.FolderNodeDTO.path).like(f"{protection.path.lower()}/%")))
                )
            ).all()
            
            # Apply protection to matching folders
            for folder in matching_folders:
                folder.retention_id = path_retention_id
                folder.path_protection_id = protection.id
                session.add(folder)
                folders_modified += 1
        
        # Commit all changes
        session.commit()

        # Step 6: Find folders with path retention where their pathprotection_id is not in the current list of path protections
        #         Set their retention to undefined_retention_id
        folders_reset = 0
        
        # Get the list of valid path protection IDs
        valid_protection_ids = [p.id for p in path_protections]
        
        # Find all folders with path retention whose pathprotection_id is not in the valid list
        # These are folders that were protected but their protection has been removed
        folders_to_reset = session.exec(
            select(dtos.FolderNodeDTO).where(
                (dtos.FolderNodeDTO.rootfolder_id == rootfolder_id) &
                (dtos.FolderNodeDTO.retention_id == path_retention_id) &
                (dtos.FolderNodeDTO.path_protection_id.notin_(valid_protection_ids))
            )
        ).all()
        
        # Reset these folders to undefined retention
        for folder in folders_to_reset:
            folder.retention_id = undefined_retention_id
            folder.path_protection_id = None
            session.add(folder)
            folders_reset += 1
        
        session.commit()

        # Step 7: Return number of folders modified
        message = f"Applied {len(path_protections)} path protection(s) to {folders_modified} folder(s) in rootfolder {rootfolder_id}"
        if folders_reset > 0:
            message += f", reset {folders_reset} folder(s) no longer covered by protections"
        
        return {
            "message": message,
            "protections_applied": len(path_protections),
            "folders_modified": folders_modified,
            "folders_reset": folders_reset
        }


# @TODO we should consider to enforce the changed path protection on existing folders
# at present it is the clients responsibility to so and communicate it in "def change_retentions"
def delete_pathprotection(rootfolder_id: int, protection_id: int):
    with Session(Database.get_engine()) as session:
        # Find the path protection by ID and rootfolder_id
        protection = session.exec( select(dtos.PathProtectionDTO).where((dtos.PathProtectionDTO.id == protection_id) & (dtos.PathProtectionDTO.rootfolder_id == rootfolder_id)) ).first()
        if not protection:
            raise HTTPException(status_code=404, detail="Path protection not found")
        
        session.delete(protection)
        session.commit()
        return {"message": f"Path protection {protection_id} deleted"}

def read_simulations_by_retention_type(rootfolder_id: int, retention_type: dtos.RetentionTypeEnum, require_pathprotection: bool = False) -> list[dtos.FolderNodeDTO]:
    """
    Read all simulation folders with a specific retention type.
    
    Args:
        rootfolder_id: ID of the root folder to query
        retention_type: The retention type enum to filter by (e.g., PATH, MARKED, CLEAN, etc.)
        require_pathprotection: If True, only return folders with non-null pathprotection_id (for PATH retention)
    
    Returns:
        List of FolderNodeDTO matching the criteria
    """
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        # Get the retention type ID for the specified enum value
        retention_type_dict = read_rootfolder_retentiontypes_dict(rootfolder.id)
        if retention_type.value not in retention_type_dict:
            raise HTTPException(status_code=404, detail=f"Retention type '{retention_type.value}' not found for rootfolder {rootfolder_id}")
        
        retention_id: int = retention_type_dict[retention_type.value].id
        leaf_nodetype_id: int = read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)[dtos.FolderTypeEnum.SIMULATION].id

        # Build query with optional pathprotection filter
        query = select(dtos.FolderNodeDTO).where(
            (dtos.FolderNodeDTO.rootfolder_id == rootfolder_id) &
            (dtos.FolderNodeDTO.retention_id == retention_id) &
            (dtos.FolderNodeDTO.nodetype_id == leaf_nodetype_id)
        )
        
        if require_pathprotection:
            query = query.where(dtos.FolderNodeDTO.path_protection_id.isnot(None))
        
        folders = session.exec(query).all()
        return folders

def change_retentions(rootfolder_id: int, retentions: list[dtos.FolderRetention]) -> list[dtos.FolderRetention]:
    from datamodel.retentions import RetentionCalculator    
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        # Get retention types for calculations
        #retention_calculator: RetentionCalculator = RetentionCalculator(read_rootfolder_retentiontypes_dict(rootfolder_id), cleanup_config) 
        retention_calculator: RetentionCalculator = RetentionCalculator(rootfolder.id, rootfolder.cleanup_config_id, session)

        # Update expiration dates. 
        # Since RetentionUpdateDTO IS-A Retention, we can pass it directly to the calculator
        for retention in retentions:
            retention.update_retention_fields(
                retention_calculator.adjust_expiration_date_from_cleanup_configuration_and_retentiontype(retention.getRetention())
            )
           
        # Prepare bulk update data - much more efficient than Python loops
        # update with the retention information and reset the days_to_cleanup to 0 
        bulk_updates = [
            {
                "id": retention.folder_id,
                "retention_id": retention.retention_id,
                "pathprotection_id": retention.path_protection_id,
                "expiration_date": retention.expiration_date
            }
            for retention in retentions
        ]
        session.bulk_update_mappings(dtos.FolderNodeDTO, bulk_updates)
        session.commit()

        #get all FolderNodeDTO that were updated
        #updated_folders:list[dtos.FolderNodeDTO] = session.exec(
        #    select(dtos.FolderNodeDTO).where(dtos.FolderNodeDTO.id.in_([retention.folder_id for retention in retentions]))
        #).all()

        #retentions: list[dtos.FolderRetention] = [dtos.FolderRetention.from_folder_node_dto(folder) for folder in updated_folders]    
        
        return retentions

# -------------------------- db operation related to cleanup_cycle action ---------
def read_folders_marked_for_cleanup(rootfolder_id: int) -> list[dtos.FolderNodeDTO]:
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        marked_retention_id:int = read_rootfolder_retentiontypes_dict(rootfolder.id)["marked"].id
        leaf_nodetype_id:int = read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)[dtos.FolderTypeEnum.SIMULATION].id

        # Get all folders marked for cleanup. FolderNodeDTO.nodetype_id == leaf_nodetype_id is not required but
        # should we in the future handle hierarchies of simulation then we must refactor and test any way
        folders = session.exec(select(dtos.FolderNodeDTO).where(
            (dtos.FolderNodeDTO.rootfolder_id == rootfolder_id) &
            (dtos.FolderNodeDTO.retention_id == marked_retention_id) &
            (dtos.FolderNodeDTO.nodetype_id == leaf_nodetype_id)
        )).all()

        return folders

# used for testing
def read_folder( folder_id: int ) -> dtos.FolderNodeDTO:
    with Session(Database.get_engine()) as session:
        folder = session.exec(select(dtos.FolderNodeDTO).where(dtos.FolderNodeDTO.id == folder_id)).first()
        if not folder:
            raise HTTPException(status_code=404, detail="Folder not found")
        return folder


# -------------------------- insertion of simulation by agents ---------



# -------------------------- all calculations for expiration dates, retentions are done in below functions ---------
# The consistency of these calculations is highly critical to the system working correctly


# This function will insert new simulations and update existing simulations
#  - I can be called with all simulations modified or not since last scan, because the list of simulations will be reduced to simulations that have changed or are new
#  - in all cases the caller's change to modify, retention_id (not None retention_id) will be applied and path protection will be applied and takes priority
#  - the expiration date and numeric retentions will:
#       if cleanup is active then recalculate them 
#       if cleanup is inactive then ignore them 
def insert_or_update_simulations_in_db(rootfolder_id: int, simulations: list[dtos.FileInfo]) -> dict[str, str]:
    #here we can remove all existing simulation if nothing changes for them
    ret1:dict[str, str] = insert_or_update_simulation_in_db_internal(rootfolder_id, simulations)
    ret2:dict[str, str] = apply_pathprotections(rootfolder_id) # this should no be necessary but
    return {**ret1, **ret2}



#the function is slow so before calling this function remove all simulation that does not provide new information. new simulation, new modified date, new retention
def insert_or_update_simulation_in_db_internal(rootfolder_id: int, simulations: list[dtos.FileInfo]) -> dict[str, str]:
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(dtos.RootFolderDTO).where(dtos.RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        # and the filepaths from the list of simulations
        existing_folders_query = session.exec(
            select(dtos.FolderNodeDTO).where(
                (dtos.FolderNodeDTO.rootfolder_id == rootfolder_id) &
                (dtos.FolderNodeDTO.path.in_([sim.filepath.lower() for sim in simulations]))
            )
        ).all()
        
        # Create a mapping from filepath to existing folder for fast lookup
        existing_folders: set[str]               = set([folder.path.lower() for folder in existing_folders_query])
        insert_simulations: list[dtos.FileInfo]  = [sim for sim in simulations if sim.filepath.lower() not in existing_folders]
        existing_folders                         = None # to preserve memory

        insertion_results: dict[str, int]   = insert_simulations_in_db(rootfolder, insert_simulations)
        insert_simulations = None

        #print(insertion_results)
        if insertion_results.get("failed_path_count", 0) > 0:
            print(f"Failed to insert some simulations for rootfolder {rootfolder_id}: {insertion_results.get('failed_paths', [])}")
            raise HTTPException(status_code=500, detail=f"Failed to insert some new paths: {insertion_results.get('failed_paths', [])}")

        update_results: dict[str, int] = update_simulation_attributes_in_db_internal(session, rootfolder, simulations)

        # Commit all changes
        session.commit()

        return {"message": f"For rootfolder {rootfolder_id}: inserted or changed simulations: {len(simulations)}"}


# This function update the attributes of existing simulations
# The function is slow so before calling this function remove all simulation that does not provide new information. new simulation, new modified date, new retention
# The update implements the attribute changes described in insert_or_update_simulation_in_db
def update_simulation_attributes_in_db_internal(session: Session, rootfolder: dtos.RootFolderDTO, simulations: list[dtos.FileInfo]):
    from datamodel.retentions import RetentionCalculator

    # retrieve the simulations in the rootfolder and ensure that the order is the same as in the simulations list 
    # This is important for the subsequent update operation to maintain consistency.
    lower_case_filepaths = [sim.filepath.lower() for sim in simulations]
    # Replace the query + ordering block with:
    rows = session.exec(
        select(dtos.FolderNodeDTO).where(
            (dtos.FolderNodeDTO.rootfolder_id == rootfolder.id) &
            (func.lower(dtos.FolderNodeDTO.path).in_(lower_case_filepaths))
        ) ).all()

    # Execute the ordered query to get results in the same order as simulations
    if len(rows) != len(simulations):
        raise HTTPException(status_code=500, detail=f"Mismatch in existing folders ({len(rows)}) and simulations ({len(simulations)}) for rootfolder {rootfolder.id}")

    #order as simulations
    existing_map = {f.path.lower(): f for f in rows}
    try:
        existing_folders = [existing_map[p] for p in lower_case_filepaths]
    except KeyError:
            raise HTTPException(status_code=500, detail="Mismatch in existing folders and simulations")
    
    #verify ordering as i am a little uncertain about the behaviour of the query
    equals_ordering = all(folder.path.lower() == sim.filepath.lower() for folder, sim in zip(existing_folders, simulations))
    if not equals_ordering:
        raise HTTPException(status_code=500, detail=f"Ordering of existing folders does not match simulations for rootfolder {rootfolder.id}")


    #Prepare calculation of retention: non-numeric including pathprotection and numeric dtos.  
    retention_calculator: RetentionCalculator = RetentionCalculator(rootfolder.id, rootfolder.cleanup_config_id, session)

    # Prepare bulk update data for existing folders
    bulk_updates = []
    for db_folder, sim in zip(existing_folders, simulations):
        # Calculate retention using consolidated logic in RetentionCalculator
        new_retention, new_modified_date = retention_calculator.calculate_retention_from_scan(
            db_retention=db_folder.get_retention(),
            db_modified_date=db_folder.modified_date,
            sim_external_retention=sim.external_retention,
            sim_modified_date=sim.modified_date,
            folder_path=db_folder.path
        )

        bulk_updates.append({
            "id": db_folder.id,
            "modified_date": new_modified_date,
            "expiration_date": new_retention.expiration_date,
            "retention_id": new_retention.retention_id,
            "pathprotection_id": new_retention.path_protection_id
        })
    
    # Execute bulk update
    session.bulk_update_mappings(dtos.FolderNodeDTO, bulk_updates)

    return {"updated_count": len(bulk_updates)}


# Helper functions for hierarchical insert
def normalize_path(filepath: str) -> list[str]:
    """
    Normalize path to forward slashes and split into segments.
    Handles edge cases like empty paths, root paths, trailing slashes.
    
    Examples:
    - "/root/child/grandchild" -> ["root", "child", "grandchild"]
    - "\\root\\child\\" -> ["root", "child"]
    - "root/child/" -> ["root", "child"]
    - "" -> []
    """
    if not filepath or filepath.strip() == "":
        return []
    
    # Normalize to forward slashes
    normalized = filepath.replace("\\", "/")
    return normalized


def find_existing_node(session: Session, rootfolder_id: int, parent_id: int, name: str) -> dtos.FolderNodeDTO | None:
    """
    Find existing node by parent_id and name (case-insensitive) within a rootfolder.
    
    Args:
        session: Database session
        rootfolder_id: ID of the root folder
        parent_id: ID of the parent node (0 for root level)
        name: Name of the node to find (case-insensitive)
    
    Returns:
        FolderNodeDTO if found, None otherwise
    """
    return session.exec(
        select(dtos.FolderNodeDTO).where(
            (dtos.FolderNodeDTO.rootfolder_id == rootfolder_id) &
            (dtos.FolderNodeDTO.parent_id == parent_id) &
            (func.lower(dtos.FolderNodeDTO.name) == name.lower())
        )
    ).first()

def generate_path_ids(parent_path_ids: str, node_id: int) -> str:
    """
    Generate path_ids for a new node based on parent information.
    Format: parent_ids + "/" + node_id (e.g., "0/1/2")
    For root nodes (parent_id=0), just return the node_id as string.
    
    Args:
        parent_path_ids: The path_ids of the parent node
        node_id: The ID of the current node
    
    Returns:
        String representation of the path_ids
    """
    if not parent_path_ids or parent_path_ids == "0":  # is this the root node?
        return str(node_id)
    else:
        return f"{parent_path_ids}/{node_id}"

#todo implement hierarchical inserts
def insert_simulations_in_db(rootfolder: dtos.RootFolderDTO, simulations: list[dtos.FileInfo]):
    # Insert missing hierarchy for all simulation filepaths.
    # This function only creates the folder structure, attributes will be updated separately.
    if not simulations:
        return {"inserted_hierarchy_count": 0, "failed_path_count": 0, "failed_paths": []}

    #print(f"start insert_simulations rootfolder_id {rootfolder.id} inserting hierarchy for {len(simulations)} folders")

    nodetypes:dict[str,dtos.FolderTypeDTO] = read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)
    #innernode_type_id =nodetypes.get(FolderTypeEnum.INNERNODE, None).id
    #if not innernode_type_id:
    #    raise HTTPException(status_code=500, detail=f"Unable to retrieve node_type_id=innernode for {rootfolder.id}")

    with Session(Database.get_engine()) as session:
        inserted_count = 0
        failed_paths = []
        
        for sim in simulations:
            try:
                # Insert hierarchy for this filepath (creates missing nodes)
                leaf_node_id = insert_hierarchy_for_one_filepath(session, rootfolder, sim, nodetypes)
                inserted_count += 1
                
            except Exception as e:
                print(f"Error inserting hierarchy for path '{sim.filepath}': {e}")
                failed_paths.append(sim.filepath)
                # Continue with other paths rather than failing completely
                continue
        
        # Commit all insertions
        session.commit()
        count = session.exec(select(func.count()).select_from(dtos.FolderNodeDTO).where(dtos.FolderNodeDTO.rootfolder_id == rootfolder.id )).first()
        #print(f"Total records in DB for rootfolder {rootfolder.id}: {count}")
        #print(f"end insert_simulations rootfolder_id {rootfolder.id} - successfully inserted hierarchy for {inserted_count}/{len(simulations)} paths, {len(failed_paths)} failed")
        
    return {"inserted_hierarchy_count": inserted_count, "failed_path_count": len(failed_paths), "failed_paths": failed_paths}


def insert_hierarchy_for_one_filepath(session: Session, rootfolder: dtos.RootFolderDTO, simulation: dtos.FileInfo, nodetypes:dict[str,dtos.FolderTypeDTO]) -> int:
    #    Insert missing hierarchy for a single filepath and return the leaf node ID.
    rootfolder_id = rootfolder.id
    if rootfolder_id is None or rootfolder_id <= 0:
        raise ValueError(f"Invalid rootfolder_id: {rootfolder_id}")
    
 
    # Normalize path and split into segments
    normalized:str = os.path.normpath(normalize_path(simulation.filepath).rstrip("/"))
    rootfolder_head:str = os.path.normpath(rootfolder.path)
    
    if not normalized.startswith(rootfolder_head)  :
        raise ValueError(f"Filepath '{simulation.filepath}' does not start with rootfolder head '{rootfolder_head}'")
    
    # the first segment must be the rootfolder head. The rest can be split using "/"
    segments = [rootfolder_head]
    remaining_path = normalized[len(rootfolder_head):]
    if remaining_path and remaining_path.startswith("/"):
        remaining_path = remaining_path[1:]  # Remove leading slash after rootfolder_head
    if remaining_path:
        segments.extend([segment for segment in remaining_path.split("/") if segment.strip()])
    
    if not segments:
        raise ValueError(f"Invalid or empty filepath: {simulation.filepath}")
    
    current_parent_id = 0  # Start from root level
    current_parent_path_ids = "0"
    current_path_segments = []

    for index, segment in enumerate(segments):
        current_path_segments.append(segment)
        
        # Build the path, adding leading / if original path was absolute
        current_path = "/".join(current_path_segments)
        
        # Check if node already exists at this level
        existing_node = find_existing_node(session, rootfolder_id, current_parent_id, segment)
        
        if existing_node:
            # Node exists, move to next level
            current_parent_id = existing_node.id
            current_parent_path_ids = existing_node.path_ids
        else:
            # Node doesn't exist, create it
            if index < len(segments) - 1:
                new_node = dtos.FolderNodeDTO(
                    rootfolder_id=rootfolder_id,
                    parent_id=current_parent_id,
                    name=segment,
                    nodetype_id=nodetypes[dtos.FolderTypeEnum.INNERNODE].id,
                    path=current_path,  # Full path up to this segment
                )
            else:
                new_node = dtos.FolderNodeDTO(
                    rootfolder_id=rootfolder_id,
                    parent_id=current_parent_id,
                    name=segment,
                    nodetype_id=nodetypes[dtos.FolderTypeEnum.SIMULATION].id,
                    path=current_path,  # Full path up to this segment
                )            
            try:
                session.add(new_node)
                session.flush()  # Flush to get the ID without committing

                # Verify we got an ID
                if new_node.id is None:
                    raise HTTPException(status_code=500, detail=f"Failed to generate ID for new node: {segment}")
                
                # Now set the path_ids using the generated ID
                new_node.path_ids = generate_path_ids(current_parent_path_ids, new_node.id)
                
                # Move to next level
                current_parent_id = new_node.id
                current_parent_path_ids = new_node.path_ids
                
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Database error creating node '{segment}': {str(e)}")
    
    return current_parent_id  # Return the ID of the leaf node
