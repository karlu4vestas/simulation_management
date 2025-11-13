from __future__ import annotations
from typing import Literal, Optional, TYPE_CHECKING
from sqlalchemy import func
from sqlmodel import Session, func, select
from fastapi import Query, HTTPException
from db.database import Database

if TYPE_CHECKING:
    from datamodel.dtos import FolderTypeDTO, FolderNodeDTO, RootFolderDTO, SimulationDomainDTO, FolderTypeEnum, FileInfo
    from datamodel.retentions import RetentionTypeDTO, PathProtectionDTO, RetentionTypeEnum, FolderRetention 
 
#-----------------start retrieval of metadata for a simulation domain -------------------
simulation_domain_name: Literal["vts"]
simulation_domain_names = ["vts"]  # Define the allowed domain names

def read_simulation_domains() -> list["SimulationDomainDTO"]:
    from datamodel.dtos import SimulationDomainDTO
    with Session(Database.get_engine()) as session:
        simulation_domains = session.exec(select(SimulationDomainDTO)).all()
        if not simulation_domains:
            raise HTTPException(status_code=404, detail="SimulationDomain not found")
        return simulation_domains
#@app.get("/v1/simulationdomains/dict", response_model=dict[str,SimulationDomainDTO]) #disable webapi untill we need it
def read_simulation_domains_dict():
    return {domain.name.lower(): domain for domain in read_simulation_domains()}


def read_simulation_domain_by_name(domain_name: str):
    from datamodel.dtos import SimulationDomainDTO
    with Session(Database.get_engine()) as session:
        simulation_domain = session.exec(select(SimulationDomainDTO).where(SimulationDomainDTO.name == domain_name)).first()
        if not simulation_domain:
            raise HTTPException(status_code=404, detail=f"SimulationDomain for {domain_name}not found")
        return simulation_domain

def read_retentiontypes_by_domain_id(simulationdomain_id: int):
    from datamodel.retentions import RetentionTypeDTO, PathProtectionDTO, RetentionTypeEnum, FolderRetention
    with Session(Database.get_engine()) as session:
        retention_types = session.exec(select(RetentionTypeDTO).where(RetentionTypeDTO.simulationdomain_id == simulationdomain_id)).all()
        if not retention_types or len(retention_types) == 0:
            raise HTTPException(status_code=404, detail="retentiontypes not found")
        return retention_types
#@app.get("/simulationdomain/{simulationdomain_id}/retentiontypes/dict", response_model=dict[str,RetentionTypeDTO]) do not expose before needed
def read_retentiontypes_dict_by_domain_id(simulationdomain_id: int) -> dict[str, "RetentionTypeDTO"]:
    from datamodel.retentions import RetentionTypeDTO
    from datamodel.retentions import RetentionTypeDTO, PathProtectionDTO, RetentionTypeEnum, FolderRetention
    return {retention.name.lower(): retention for retention in read_retentiontypes_by_domain_id(simulationdomain_id)}

def read_cleanupfrequency_by_domain_id(simulationdomain_id: int):
    from cleanup_cycle.cleanup_dtos import CleanupFrequencyDTO, CycleTimeDTO
    with Session(Database.get_engine()) as session:
        cleanupfrequency = session.exec(select(CleanupFrequencyDTO).where(CleanupFrequencyDTO.simulationdomain_id == simulationdomain_id)).all()
        if not cleanupfrequency or len(cleanupfrequency) == 0:
            raise HTTPException(status_code=404, detail="CleanupFrequency not found")
        return cleanupfrequency
#@app.get("/simulationdomain/{simulationdomain_id}/cleanupfrequencies/dict", response_model=dict[str,CleanupFrequencyDTO]) # do not expose before needed
def read_cleanupfrequency_name_dict_by_domain_id(simulationdomain_id: int):
    return {cleanup.name.lower(): cleanup for cleanup in read_cleanupfrequency_by_domain_id(simulationdomain_id)}


def read_folder_types_pr_domain_id(simulationdomain_id: int):
    from datamodel.dtos import FolderTypeDTO
    with Session(Database.get_engine()) as session:
        folder_types = session.exec(select(FolderTypeDTO).where(FolderTypeDTO.simulationdomain_id == simulationdomain_id)).all()
        if not folder_types or len(folder_types) == 0:
            raise HTTPException(status_code=404, detail="foldertypes not found")
        return folder_types
#@app.get("/foldertypes/dict", response_model=dict[str,FolderTypeDTO]) #isadble webapi untill we need it
def read_folder_type_dict_pr_domain_id(simulationdomain_id: int):
    return {folder_type.name.lower(): folder_type for folder_type in read_folder_types_pr_domain_id(simulationdomain_id)}

def read_cycle_time_by_domain_id(simulationdomain_id: int):
    from cleanup_cycle.cleanup_dtos import CleanupFrequencyDTO, CycleTimeDTO
    with Session(Database.get_engine()) as session:
        cycle_time = session.exec(select(CycleTimeDTO).where(CycleTimeDTO.simulationdomain_id == simulationdomain_id)).all()
        if not cycle_time or len(cycle_time) == 0:
            raise HTTPException(status_code=404, detail="CycleTime not found")
        return cycle_time
#@app.get("/simulationdomain/{simulationdomain_id}/cycletimes/dict", response_model=dict[str,CycleTimeDTO]) # do not expose before needed
def read_cycle_time_dict_by_domain_id(simulationdomain_id: int):
    return {cycle.name.lower(): cycle for cycle in read_cycle_time_by_domain_id(simulationdomain_id)}

#-----------------end retrieval of metadata for a simulation domain -------------------


#-----------------start maintenance of rootfolders and information under it -------------------
def read_rootfolders(simulationdomain_id: int, initials: Optional[str] = Query(default="")):
    if initials is None or simulationdomain_id is None or simulationdomain_id == 0:
        raise HTTPException(status_code=404, detail="root_folders not found. you must provide simulation domain and initials")
    rootfolders = read_rootfolders_by_domain_and_initials(simulationdomain_id, initials)
    return rootfolders

def read_rootfolders_by_domain_and_initials(simulationdomain_id: int, initials: str= None)->list[RootFolderDTO]:
    from datamodel.dtos import RootFolderDTO
    if simulationdomain_id is None or simulationdomain_id == 0:
        raise HTTPException(status_code=404, detail="root_folders not found. you must provide simulation domain and initials")

    with Session(Database.get_engine()) as session:
        if type(initials) == str and initials is not None:
            rootfolders = session.exec(
                select(RootFolderDTO).where( (RootFolderDTO.simulationdomain_id == simulationdomain_id) &
                    ((RootFolderDTO.owner == initials) | (RootFolderDTO.approvers.like(f"%{initials}%")))
                )
            ).all()
        else:
            rootfolders = session.exec(
                select(RootFolderDTO).where( (RootFolderDTO.simulationdomain_id == simulationdomain_id) )
            ).all()

        return rootfolders


def exist_rootfolder(rootfolder:RootFolderDTO):
    from datamodel.dtos import RootFolderDTO
    if (rootfolder is None) or (rootfolder.simulationdomain_id is None) or (rootfolder.simulationdomain_id == 0):
        raise HTTPException(status_code=404, detail="You must provide a valid simulationdomain_id to create a rootfolder")

    with Session(Database.get_engine()) as session:
        #verify if the rootfolder already exists        
        existing_rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(
                (RootFolderDTO.simulationdomain_id == rootfolder.simulationdomain_id) & 
                (RootFolderDTO.path == rootfolder.path)
            )).first()
        return existing_rootfolder is not None

def insert_rootfolder(rootfolder:RootFolderDTO):
    from datamodel.dtos import RootFolderDTO
    if (rootfolder is None) or (rootfolder.simulationdomain_id is None) or (rootfolder.simulationdomain_id == 0):
        raise HTTPException(status_code=404, detail="You must provide a valid simulationdomain_id to create a rootfolder")

    with Session(Database.get_engine()) as session:
        #verify if the rootfolder already exists
        existing_rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(
                (RootFolderDTO.simulationdomain_id == rootfolder.simulationdomain_id) & 
                (RootFolderDTO.path == rootfolder.path)
            )).first()
        if existing_rootfolder:
            return existing_rootfolder

        session.add(rootfolder)
        session.commit()
        #session.refresh(rootfolder)

        if (rootfolder.id is None) or (rootfolder.id == 0):
            raise HTTPException(status_code=404, detail=f"Failed to provide {rootfolder.path} with an id")
        return rootfolder


def read_rootfolder_retentiontypes(rootfolder_id: int):
    #from datamodel.retentions import RetentionTypeDTO, PathProtectionDTO, RetentionTypeEnum, FolderRetention
    retention_types:list[RetentionTypeDTO] = list(read_rootfolder_retentiontypes_dict(rootfolder_id).values())
    return retention_types
#@app.get("/v1/rootfolders/{rootfolder_id}/retentiontypes/dict", response_model=dict[str, RetentionTypeDTO]) #do not expose untill needed
def read_rootfolder_retentiontypes_dict(rootfolder_id: int)-> dict[str, "RetentionTypeDTO"]:
    #from datamodel.retentions import RetentionTypeDTO, PathProtectionDTO, RetentionTypeEnum, FolderRetention
    from datamodel.dtos import RootFolderDTO
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")

        retention_types:dict[str,RetentionTypeDTO] = read_retentiontypes_dict_by_domain_id(rootfolder.simulationdomain_id)
        if not retention_types:
            raise HTTPException(status_code=404, detail="retentiontypes not found")
        
        if not retention_types.get("path", None):
            raise HTTPException(status_code=500, detail=f"Unable to retrieve node_type_id=vts_simulation for {rootfolder.id}")
        
        return retention_types

def read_rootfolder_numeric_retentiontypes_dict(rootfolder_id: int) -> dict[str, "RetentionTypeDTO"]:
    #from datamodel.retentions import RetentionTypeDTO, PathProtectionDTO, RetentionTypeEnum, FolderRetention
    retention_types_dict:dict[str, RetentionTypeDTO] = read_rootfolder_retentiontypes_dict(rootfolder_id)
    #filter to keep only retentions with at cycletime
    return {key:retention for key,retention in retention_types_dict.items() if retention.days_to_cleanup is not None}

def read_folders( rootfolder_id: int ):
    from datamodel.dtos import FolderNodeDTO
    with Session(Database.get_engine()) as session:
        folders = session.exec(select(FolderNodeDTO).where(FolderNodeDTO.rootfolder_id == rootfolder_id)).all()
        return folders

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

def read_pathprotections( rootfolder_id: int )-> list["PathProtectionDTO"]:
    from datamodel.retentions import RetentionTypeDTO, PathProtectionDTO, RetentionTypeEnum, FolderRetention
    with Session(Database.get_engine()) as session:
        paths = session.exec(select(PathProtectionDTO).where(PathProtectionDTO.rootfolder_id == rootfolder_id)).all()
        return paths

# @TODO we should consider to enforce the changed path protection on existing folders
# at present it is the clients responsibility to so and communicate it in "def change_retentions"
def add_pathprotection(rootfolder_id:int, path_protection:"PathProtectionDTO"):
    from datamodel.retentions import RetentionTypeDTO, PathProtectionDTO, RetentionTypeEnum, FolderRetention
    #print(f"Adding path protection {path_protection}")
    with Session(Database.get_engine()) as session:
        # Check if path protection already exists for this path in this rootfolder
        existing_protection = session.exec(
            select(PathProtectionDTO).where(
                (PathProtectionDTO.rootfolder_id == rootfolder_id) & 
                (PathProtectionDTO.folder_id == path_protection.folder_id)
            )
        ).first()
        
        if existing_protection:
            raise HTTPException(status_code=409, detail="Path protection already exists for this path")
        
        # Create new path protection
        new_protection = PathProtectionDTO(
            rootfolder_id = rootfolder_id,
            folder_id     = path_protection.folder_id,
            path          = path_protection.path
        )
        
        session.add(new_protection)
        session.commit()
        return {"id": new_protection.id,
            "message": f"Path protection added id '{new_protection.id}' for path '{new_protection.path}'"}

def add_pathprotection_by_paths(rootfolder_id:int, paths:list[str]):
    from datamodel.retentions import RetentionTypeDTO, PathProtectionDTO, RetentionTypeEnum, FolderRetention
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO
    # step 1: find the folder nodes by path
    # step 2: create list[PathProtectionDTO] with rootfolder_id, folder_id, path
    # step 3: call add_path_protection for each PathProtectionDTO
    # Note: Uses case-insensitive path matching and raises exceptions for errors
    if not paths:
        raise HTTPException(status_code=400, detail="No paths provided")
    
    with Session(Database.get_engine()) as session:
        # Verify rootfolder exists
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")
        
        # Step 1: Find the folder nodes by path (case-insensitive)
        lower_case_paths = [path.lower() for path in paths]
        existing_folders = session.exec(
            select(FolderNodeDTO).where(
                (FolderNodeDTO.rootfolder_id == rootfolder_id) &
                (func.lower(FolderNodeDTO.path).in_(lower_case_paths))
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
                select(PathProtectionDTO).where(
                    (PathProtectionDTO.rootfolder_id == rootfolder_id) & 
                    (PathProtectionDTO.folder_id == folder.id)
                )
            ).first()
            
            # some path_protections failed. Instead of raise an exception we return the number of failed_paths in the results
            if existing_protection:
                # Use existing protection instead of creating a new one
                added_protections.append({"path": folder.path, "folder_id": folder.id, "already_existed": True})
                continue
            
            # Create new path protection
            new_protection = PathProtectionDTO(
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
    from datamodel.retentions import RetentionTypeDTO, PathProtectionDTO, RetentionTypeEnum, FolderRetention
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeEnum
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
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")
        
        # Step 1: Find the path retention id
        path_retention_dict:dict[str, RetentionTypeDTO] = read_rootfolder_retentiontypes_dict(rootfolder_id)
        path_retention_id:int = path_retention_dict["path"].id if "path" in path_retention_dict else 0
        undefined_retention_id:int = path_retention_dict["?"].id if "?" in path_retention_dict else 0
        if path_retention_id == 0:
            raise HTTPException(status_code=500, detail=f"Path retention type not found for rootfolder {rootfolder_id}")
        
        # Step 2: Get the simulation nodetype id (only SIMULATION nodes, not INNERNODE)
        nodetype_simulation_id:int = read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)[FolderTypeEnum.SIMULATION].id
        
        # Step 3: Find all path protections for the rootfolder
        path_protections = session.exec( select(PathProtectionDTO).where( PathProtectionDTO.rootfolder_id == rootfolder_id) ).all()        
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
                select(FolderNodeDTO).where(
                    (FolderNodeDTO.rootfolder_id == rootfolder_id) &
                    (FolderNodeDTO.nodetype_id == nodetype_simulation_id) &
                    ((func.lower(FolderNodeDTO.path) == protection.path.lower()) |
                     (func.lower(FolderNodeDTO.path).like(f"{protection.path.lower()}/%")))
                )
            ).all()
            
            # Apply protection to matching folders
            for folder in matching_folders:
                folder.retention_id = path_retention_id
                folder.pathprotection_id = protection.id
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
            select(FolderNodeDTO).where(
                (FolderNodeDTO.rootfolder_id == rootfolder_id) &
                (FolderNodeDTO.retention_id == path_retention_id) &
                (FolderNodeDTO.pathprotection_id.notin_(valid_protection_ids))
            )
        ).all()
        
        # Reset these folders to undefined retention
        for folder in folders_to_reset:
            folder.retention_id = undefined_retention_id
            folder.pathprotection_id = None
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
    from datamodel.retentions import RetentionTypeDTO, PathProtectionDTO, RetentionTypeEnum, FolderRetention
    with Session(Database.get_engine()) as session:
        # Find the path protection by ID and rootfolder_id
        protection = session.exec( select(PathProtectionDTO).where((PathProtectionDTO.id == protection_id) & (PathProtectionDTO.rootfolder_id == rootfolder_id)) ).first()
        if not protection:
            raise HTTPException(status_code=404, detail="Path protection not found")
        
        session.delete(protection)
        session.commit()
        return {"message": f"Path protection {protection_id} deleted"}

def read_simulations_by_retention_type(rootfolder_id: int, retention_type: RetentionTypeEnum, require_pathprotection: bool = False) -> list[FolderNodeDTO]:
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeEnum
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
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        # Get the retention type ID for the specified enum value
        retention_type_dict = read_rootfolder_retentiontypes_dict(rootfolder.id)
        if retention_type.value not in retention_type_dict:
            raise HTTPException(status_code=404, detail=f"Retention type '{retention_type.value}' not found for rootfolder {rootfolder_id}")
        
        retention_id: int = retention_type_dict[retention_type.value].id
        leaf_nodetype_id: int = read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)[FolderTypeEnum.SIMULATION].id

        # Build query with optional pathprotection filter
        query = select(FolderNodeDTO).where(
            (FolderNodeDTO.rootfolder_id == rootfolder_id) &
            (FolderNodeDTO.retention_id == retention_id) &
            (FolderNodeDTO.nodetype_id == leaf_nodetype_id)
        )
        
        if require_pathprotection:
            query = query.where(FolderNodeDTO.pathprotection_id.isnot(None))
        
        folders = session.exec(query).all()
        return folders

def change_retentions(rootfolder_id: int, retentions: list["FolderRetention"]):
    from datamodel.retentions import RetentionCalculator
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO
    #print(f"start change_retention_category rootfolder_id{rootfolder_id} changing number of retention {len(retentions)}")
    
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        #cleanup_config: CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session)
        # the cleanup_round_start_date must be set for calculation of retention.expiration_date. It could make sens to set path retentions before the first cleanup round. 
        # However, when a cleanup round is started they have time at "rootfolder.cycletime" to adjust retention
        #if not cleanup_config.is_valid():
        #    raise HTTPException(status_code=400, detail="The rootFolder's CleanupConfiguration is is missing cleanupfrequency, cleanup_round_start_date or cycletime ")

        # Get retention types for calculations
        #retention_calculator: RetentionCalculator = RetentionCalculator(read_rootfolder_retentiontypes_dict(rootfolder_id), cleanup_config) 
        retention_calculator: RetentionCalculator = RetentionCalculator(rootfolder.id, rootfolder.cleanup_config_id, session)

        # Update expiration dates. 
        # Since RetentionUpdateDTO IS-A Retention, we can pass it directly to the calculator
        for retention in retentions:
            retention.update_retention_fields(
                retention_calculator.adjust_expiration_date_from_cleanup_configuration_and_retentiontype(retention)
            )
           
        # Prepare bulk update data - much more efficient than Python loops
        # update with the retention information and reset the days_to_cleanup to 0 
        bulk_updates = [
            {
                "id": retention.folder_id,
                "retention_id": retention.retention_id,
                "pathprotection_id": retention.pathprotection_id,
                "expiration_date": retention.expiration_date
            }
            for retention in retentions
        ]
        session.bulk_update_mappings(FolderNodeDTO, bulk_updates)
        session.commit()
        
        #print(f"end changeretentions rootfolder_id{rootfolder_id} changing number of retention {len(retentions)}")
        return {"message": f"Updated rootfolder {rootfolder_id} with {len(retentions)} retentions"}

# -------------------------- db operation related to cleanup_cycle action ---------
def read_folders_marked_for_cleanup(rootfolder_id: int) -> list[FolderNodeDTO]:
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeEnum

    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        marked_retention_id:int = read_rootfolder_retentiontypes_dict(rootfolder.id)["marked"].id
        leaf_nodetype_id:int = read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)[FolderTypeEnum.SIMULATION].id

        # Get all folders marked for cleanup. FolderNodeDTO.nodetype_id == leaf_nodetype_id is not required but
        # should we in the future handle hierarchies of simulation then we must refactor and test any way
        folders = session.exec(select(FolderNodeDTO).where(
            (FolderNodeDTO.rootfolder_id == rootfolder_id) &
            (FolderNodeDTO.retention_id == marked_retention_id) &
            (FolderNodeDTO.nodetype_id == leaf_nodetype_id)
        )).all()

        return folders

# used for testing
def read_folder( folder_id: int ) -> FolderNodeDTO:
    from datamodel.dtos import FolderNodeDTO
    with Session(Database.get_engine()) as session:
        folder = session.exec(select(FolderNodeDTO).where(FolderNodeDTO.id == folder_id)).first()
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
def insert_or_update_simulations_in_db(rootfolder_id: int, simulations: list[FileInfo]) -> dict[str, str]:
    #here we can remove all existing simulation if nothing changes for them
    ret1:dict[str, str] = insert_or_update_simulation_in_db_internal(rootfolder_id, simulations)
    ret2:dict[str, str] = apply_pathprotections(rootfolder_id) # this should no be necessary but
    return {**ret1, **ret2}



#the function is slow so before calling this function remove all simulation that does not provide new information. new simulation, new modified date, new retention
def insert_or_update_simulation_in_db_internal(rootfolder_id: int, simulations: list["FileInfo"]) -> dict[str, str]:
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FileInfo
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        # and the filepaths from the list of simulations
        existing_folders_query = session.exec(
            select(FolderNodeDTO).where(
                (FolderNodeDTO.rootfolder_id == rootfolder_id) &
                (FolderNodeDTO.path.in_([sim.filepath.lower() for sim in simulations]))
            )
        ).all()
        
        # Create a mapping from filepath to existing folder for fast lookup
        existing_folders: set[str]          = set([folder.path.lower() for folder in existing_folders_query])
        insert_simulations: list[FileInfo]  = [sim for sim in simulations if sim.filepath.lower() not in existing_folders]
        existing_folders                    = None # to preserve memory

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
def update_simulation_attributes_in_db_internal(session: Session, rootfolder: "RootFolderDTO", simulations: list["FileInfo"]):
    from datamodel.retentions import RetentionCalculator
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FileInfo
    # retrieve the simulations in the rootfolder and ensure that the order is the same as in the simulations list 
    # This is important for the subsequent update operation to maintain consistency.
    lower_case_filepaths = [sim.filepath.lower() for sim in simulations]
    # Replace the query + ordering block with:
    rows = session.exec(
        select(FolderNodeDTO).where(
            (FolderNodeDTO.rootfolder_id == rootfolder.id) &
            (func.lower(FolderNodeDTO.path).in_(lower_case_filepaths))
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


    #Prepare calculation of retention: non-numeric including pathprotection and numeric retentions.  
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
            "pathprotection_id": new_retention.pathprotection_id
        })
    
    # Execute bulk update
    session.bulk_update_mappings(FolderNodeDTO, bulk_updates)

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


def find_existing_node(session: Session, rootfolder_id: int, parent_id: int, name: str) -> "FolderNodeDTO | None":
    from datamodel.dtos import FolderNodeDTO
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
        select(FolderNodeDTO).where(
            (FolderNodeDTO.rootfolder_id == rootfolder_id) &
            (FolderNodeDTO.parent_id == parent_id) &
            (func.lower(FolderNodeDTO.name) == name.lower())
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
def insert_simulations_in_db(rootfolder: "RootFolderDTO", simulations: list["FileInfo"]):
    from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeDTO, FileInfo
    # Insert missing hierarchy for all simulation filepaths.
    # This function only creates the folder structure, attributes will be updated separately.
    if not simulations:
        return {"inserted_hierarchy_count": 0, "failed_path_count": 0, "failed_paths": []}
        
    #print(f"start insert_simulations rootfolder_id {rootfolder.id} inserting hierarchy for {len(simulations)} folders")

    nodetypes:dict[str,FolderTypeDTO] = read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)
    #innernode_type_id =nodetypes.get(FolderTypeEnum.INNERNODE, None).id
    #if not innernode_type_id:
    #    raise HTTPException(status_code=500, detail=f"Unable to retrieve node_type_id=innernode for {rootfolder.id}")

    with Session(Database.get_engine()) as session:
        inserted_count = 0
        failed_paths = []
        
        for sim in simulations:
            try:
                # Insert hierarchy for this filepath (creates missing nodes)
                leaf_node_id = insert_hierarchy_for_one_filepath(session, rootfolder.id, sim, nodetypes)
                inserted_count += 1
                
            except Exception as e:
                print(f"Error inserting hierarchy for path '{sim.filepath}': {e}")
                failed_paths.append(sim.filepath)
                # Continue with other paths rather than failing completely
                continue
        
        # Commit all insertions
        session.commit()
        count = session.exec(select(func.count()).select_from(FolderNodeDTO).where(
            FolderNodeDTO.rootfolder_id == rootfolder.id
        )).first()
        #print(f"Total records in DB for rootfolder {rootfolder.id}: {count}")
        #print(f"end insert_simulations rootfolder_id {rootfolder.id} - successfully inserted hierarchy for {inserted_count}/{len(simulations)} paths, {len(failed_paths)} failed")
        
    return {"inserted_hierarchy_count": inserted_count, "failed_path_count": len(failed_paths), "failed_paths": failed_paths}


def insert_hierarchy_for_one_filepath(session: Session, rootfolder_id: int, simulation: "FileInfo", nodetypes:dict[str,"FolderTypeDTO"]) -> int:
    from datamodel.dtos import FolderNodeDTO, FolderTypeDTO, FileInfo, FolderTypeEnum
    #    Insert missing hierarchy for a single filepath and return the leaf node ID.
    if rootfolder_id is None or rootfolder_id <= 0:
        raise ValueError(f"Invalid rootfolder_id: {rootfolder_id}")

    # Normalize path and split into segments
    normalized = normalize_path(simulation.filepath).rstrip("/")
    
    # Check if path is absolute (starts with /)
    if normalized.startswith("/") and len(normalized) > 1:
        segments = [segment for segment in normalized.split("/") if segment.strip()]
        if len(segments) > 0:
            segments[0]= "/" + segments[0]   # restore leading slash to first segment
    else:
        segments = [segment for segment in normalized.split("/") if segment.strip()]
    
    if not segments:
        raise ValueError(f"Invalid or empty filepath: {simulation.filepath}")
    
    current_parent_id = 0  # Start from root level
    current_parent_path_ids = "0"
    current_path_segments = []

    for index, segment in enumerate(segments):
        # Validate segment name
        if not segment or segment.strip() == "":
            raise ValueError(f"Invalid empty segment in filepath: {simulation.filepath}")
            
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
                new_node = FolderNodeDTO(
                    rootfolder_id=rootfolder_id,
                    parent_id=current_parent_id,
                    name=segment,
                    nodetype_id=nodetypes[FolderTypeEnum.INNERNODE].id,
                    path=current_path,  # Full path up to this segment
                )
            else:
                new_node = FolderNodeDTO(
                    rootfolder_id=rootfolder_id,
                    parent_id=current_parent_id,
                    name=segment,
                    nodetype_id=nodetypes[FolderTypeEnum.SIMULATION].id,
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
