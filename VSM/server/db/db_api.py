from datetime import date
from typing import Literal, Optional
from dataclasses import dataclass
from sqlalchemy import func
from sqlmodel import Session, func, select
from fastapi import Query, HTTPException
from db.database import Database
from datamodel.dtos import CleanupConfigurationDTO, CleanupFrequencyDTO, CycleTimeDTO, RetentionTypeDTO, FolderTypeDTO, FolderNodeDTO
from datamodel.dtos import RootFolderDTO, PathProtectionDTO, SimulationDomainDTO, RetentionUpdateDTO, FolderTypeEnum, Retention, FileInfo 
from datamodel.retention_validators import ExternalToInternalRetentionTypeConverter, RetentionCalculator, PathProtectionEngine
 
#-----------------start retrieval of metadata for a simulation domain -------------------
simulation_domain_name: Literal["vts"]
simulation_domain_names = ["vts"]  # Define the allowed domain names

def read_simulation_domains():
    with Session(Database.get_engine()) as session:
        simulation_domains = session.exec(select(SimulationDomainDTO)).all()
        if not simulation_domains:
            raise HTTPException(status_code=404, detail="SimulationDomain not found")
        return simulation_domains
#@app.get("/v1/simulationdomains/dict", response_model=dict[str,SimulationDomainDTO]) #disable webapi untill we need it
def read_simulation_domains_dict():
    return {domain.name.lower(): domain for domain in read_simulation_domains()}


def read_simulation_domain_by_name(domain_name: str):
    with Session(Database.get_engine()) as session:
        simulation_domain = session.exec(select(SimulationDomainDTO).where(SimulationDomainDTO.name == domain_name)).first()
        if not simulation_domain:
            raise HTTPException(status_code=404, detail=f"SimulationDomain for {domain_name}not found")
        return simulation_domain

def read_retentiontypes_by_domain_id(simulationdomain_id: int):
    with Session(Database.get_engine()) as session:
        retention_types = session.exec(select(RetentionTypeDTO).where(RetentionTypeDTO.simulationdomain_id == simulationdomain_id)).all()
        if not retention_types or len(retention_types) == 0:
            raise HTTPException(status_code=404, detail="retentiontypes not found")
        return retention_types
#@app.get("/simulationdomain/{simulationdomain_id}/retentiontypes/dict", response_model=dict[str,RetentionTypeDTO]) do not expose before needed
def read_retentiontypes_dict_by_domain_id(simulationdomain_id: int) -> dict[str, RetentionTypeDTO]:
    return {retention.name.lower(): retention for retention in read_retentiontypes_by_domain_id(simulationdomain_id)}

def read_cleanupfrequency_by_domain_id(simulationdomain_id: int):
    with Session(Database.get_engine()) as session:
        cleanupfrequency = session.exec(select(CleanupFrequencyDTO).where(CleanupFrequencyDTO.simulationdomain_id == simulationdomain_id)).all()
        if not cleanupfrequency or len(cleanupfrequency) == 0:
            raise HTTPException(status_code=404, detail="CleanupFrequency not found")
        return cleanupfrequency
#@app.get("/simulationdomain/{simulationdomain_id}/cleanupfrequencies/dict", response_model=dict[str,CleanupFrequencyDTO]) # do not expose before needed
def read_cleanupfrequency_name_dict_by_domain_id(simulationdomain_id: int):
    return {cleanup.name.lower(): cleanup for cleanup in read_cleanupfrequency_by_domain_id(simulationdomain_id)}


def read_folder_types_pr_domain_id(simulationdomain_id: int):
    with Session(Database.get_engine()) as session:
        folder_types = session.exec(select(FolderTypeDTO).where(FolderTypeDTO.simulationdomain_id == simulationdomain_id)).all()
        if not folder_types or len(folder_types) == 0:
            raise HTTPException(status_code=404, detail="foldertypes not found")
        return folder_types
#@app.get("/foldertypes/dict", response_model=dict[str,FolderTypeDTO]) #isadble webapi untill we need it
def read_folder_type_dict_pr_domain_id(simulationdomain_id: int):
    return {folder_type.name.lower(): folder_type for folder_type in read_folder_types_pr_domain_id(simulationdomain_id)}

def read_cycle_time_by_domain_id(simulationdomain_id: int):
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


def insert_rootfolder(rootfolder:RootFolderDTO):
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

def update_rootfolder_cleanup_configuration(rootfolder_id: int, cleanup_configuration: CleanupConfigurationDTO):
    is_valid = cleanup_configuration.is_valid()
    if not is_valid:
        raise HTTPException(status_code=404, detail=f"for rootfolder {rootfolder_id}: update of cleanup_configuration failed")

    #now the configuration is consistent
    with Session(Database.get_engine()) as session:
        rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")
      
        # NEW: Use ensure_cleanup_config to get or create CleanupConfigurationDTO
        config_dto = rootfolder.get_cleanup_configuration(session)
        # Update the DTO with values from the incoming dataclass
        config_dto.cycletime          = cleanup_configuration.cycletime
        config_dto.cleanupfrequency   = cleanup_configuration.cleanupfrequency
        config_dto.cleanup_start_date = cleanup_configuration.cleanup_start_date
        config_dto.cleanup_progress   = cleanup_configuration.cleanup_progress if cleanup_configuration.cleanup_progress is None else cleanup_configuration.cleanup_progress 
        rootfolder.save_cleanup_configuration(session, config_dto)

        #if cleanup_configuration.can_start_cleanup():
        #    print(f"Starting cleanup for rootfolder {rootfolder_id} with configuration {cleanup_configuration}")
        #    #from app.web_server_retention_api import start_new_cleanup_cycle  #avoid circular import
        #    #start_new_cleanup_cycle(rootfolder_id)
        return {"message": f"for rootfolder {rootfolder_id}: update of cleanup configuration {config_dto.id} "}

def get_cleanup_configuration_by_rootfolder_id(rootfolder_id: int)-> CleanupConfigurationDTO:
    with Session(Database.get_engine()) as session:
        rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")

        cleanup_configuration = rootfolder.get_cleanup_configuration()
        if not cleanup_configuration:
            raise HTTPException(status_code=404, detail="cleanup_configuration not found")

    return cleanup_configuration

#insert the cleanup configuration for a rootfolder and update the rootfolder to point to the cleanup configuration
def insert_cleanup_configuration(rootfolder_id:int, cleanup_config: CleanupConfigurationDTO):
    if (rootfolder_id is None) or (rootfolder_id == 0):
        raise HTTPException(status_code=404, detail="You must provide a valid rootfolder_id to create a cleanup configuration")
    
    with Session(Database.get_engine()) as session:
        #verify if the rootfolder already exists
        existing_rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(
                (RootFolderDTO.id == rootfolder_id)
            )).first()
        if not existing_rootfolder:
            raise HTTPException(status_code=404, detail=f"Failed to find rootfolder with id {rootfolder_id} to create a cleanup configuration")

        #verify if a cleanup configuration already exists for this rootfolder
        existing_cleanup_config:CleanupConfigurationDTO = session.exec(select(CleanupConfigurationDTO).where(
                (CleanupConfigurationDTO.rootfolder_id == rootfolder_id)
            )).first()
        if existing_cleanup_config:
            return existing_cleanup_config
        
        cleanup_config.rootfolder_id = rootfolder_id
        session.add(cleanup_config)
        session.commit()
        session.refresh(cleanup_config)
        existing_rootfolder.cleanup_config_id = cleanup_config.id
        session.add(existing_rootfolder)
        session.commit()

        if (cleanup_config.id is None) or (cleanup_config.id == 0):
            raise HTTPException(status_code=404, detail=f"Failed to provide cleanup configuration for rootfolder_id {rootfolder_id} with an id")
        return cleanup_config   



def read_rootfolder_retentiontypes(rootfolder_id: int):
    retention_types:list[RetentionTypeDTO] = list(read_rootfolder_retentiontypes_dict(rootfolder_id).values())
    return retention_types
#@app.get("/v1/rootfolders/{rootfolder_id}/retentiontypes/dict", response_model=dict[str, RetentionTypeDTO]) #do not expose untill needed
def read_rootfolder_retentiontypes_dict(rootfolder_id: int)-> dict[str, RetentionTypeDTO]:
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")

        retention_types:dict[str,RetentionTypeDTO] = read_retentiontypes_dict_by_domain_id(rootfolder.simulationdomain_id)
        if not retention_types:
            raise HTTPException(status_code=404, detail="retentiontypes not found")
        
        if not retention_types.get("+next",None):
            raise HTTPException(status_code=404, detail="retentiontypes does not contain 'next' retention type")

        if not retention_types.get("path", None):
            raise HTTPException(status_code=500, detail=f"Unable to retrieve node_type_id=vts_simulation for {rootfolder.id}")
        
        return retention_types

def read_rootfolder_numeric_retentiontypes_dict(rootfolder_id: int) -> dict[str, RetentionTypeDTO]:
    retention_types_dict:dict[str, RetentionTypeDTO] = read_rootfolder_retentiontypes_dict(rootfolder_id)
    #filter to keep only retentions with at cycletime
    return {key:retention for key,retention in retention_types_dict.items() if retention.days_to_cleanup is not None}

def read_folders( rootfolder_id: int ):
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

def read_pathprotections( rootfolder_id: int ):
    with Session(Database.get_engine()) as session:
        paths = session.exec(select(PathProtectionDTO).where(PathProtectionDTO.rootfolder_id == rootfolder_id)).all()
        return paths

def add_path_protection(rootfolder_id:int, path_protection:PathProtectionDTO):
    print(f"Adding path protection {path_protection}")
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
    
def delete_path_protection(rootfolder_id: int, protection_id: int):
    with Session(Database.get_engine()) as session:
        # Find the path protection by ID and rootfolder_id
        protection = session.exec( select(PathProtectionDTO).where((PathProtectionDTO.id == protection_id) & (PathProtectionDTO.rootfolder_id == rootfolder_id)) ).first()
        if not protection:
            raise HTTPException(status_code=404, detail="Path protection not found")
        
        session.delete(protection)
        session.commit()
        return {"message": f"Path protection {protection_id} deleted"}

def change_retentions(rootfolder_id: int, retentions: list[RetentionUpdateDTO]):
    print(f"start change_retention_category rootfolder_id{rootfolder_id} changing number of retention {len(retentions)}")
    
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        cleanup_config: CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session)
        # the cleanup_round_start_date must be set for calculation of retention.expiration_date. It could make sens to set path retentions before the first cleanup round. 
        # However, when a cleanup round is started they have time at "rootfolder.cycletime" to adjust retention
        if not cleanup_config.is_valid():
            raise HTTPException(status_code=400, detail="The rootFolder's CleanupConfiguration is is missing cleanupfrequency, cleanup_round_start_date or cycletime ")

        # Get retention types for calculations
        retention_calculator: RetentionCalculator = RetentionCalculator(read_rootfolder_retentiontypes_dict(rootfolder_id), cleanup_config) 

        # Update expiration dates. 
        for retention in retentions:
            retention.set_retention( retention_calculator.adjust_expiration_date_from_cleanup_configuration_and_retentiontype(retention.get_retention()) )
           
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
        
        print(f"end changeretentions rootfolder_id{rootfolder_id} changing number of retention {len(retentions)}")
        return {"message": f"Updated rootfolder {rootfolder_id} with {len(retentions)} retentions"}

# -------------------------- db operation related to cleanup_cycle action ---------
def read_folders_marked_for_cleanup(rootfolder_id: int) -> list[FolderNodeDTO]:

    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        marked_retention_id:int = read_rootfolder_retentiontypes_dict(rootfolder.id)["marked"].id
        leaf_nodetype_id:int = read_folder_type_dict_pr_domain_id(rootfolder.simulationdomain_id)[FolderTypeEnum.VTS_SIMULATION].id

        # Get all folders marked for cleanup. FolderNodeDTO.nodetype_id == leaf_nodetype_id is not required but
        # should we in the future handle hierarchies of simulation then we must refactor and test any way
        folders = session.exec(select(FolderNodeDTO).where(
            (FolderNodeDTO.rootfolder_id == rootfolder_id) &
            (FolderNodeDTO.retention_id == marked_retention_id) &
            (FolderNodeDTO.nodetype_id == leaf_nodetype_id)
        )).all()

        return folders




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
    return insert_or_update_simulation_in_db_internal(rootfolder_id, simulations)



#the function is slow so before calling this function remove all simulation that does not provide new information. new simulation, new modified date, new retention
def insert_or_update_simulation_in_db_internal(rootfolder_id: int, simulations: list[FileInfo]) -> dict[str, str]:
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

        print(insertion_results)
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
def update_simulation_attributes_in_db_internal(session: Session, rootfolder: RootFolderDTO, simulations: list[FileInfo]):
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


    # prepare the path_protection_engine. it returns the id to the PathProtectionDTO and prioritizes the most specific path (most segments) if any.
    path_retention_dict:dict[str, RetentionTypeDTO] = read_rootfolder_retentiontypes_dict(rootfolder.id)
    path_matcher:PathProtectionEngine = PathProtectionEngine(read_pathprotections(rootfolder.id), path_retention_dict["path"].id)

    #Prepare calculation of numeric retention_id.  
    config:CleanupConfigurationDTO = rootfolder.get_cleanup_configuration(session)
    retention_calculator = RetentionCalculator( read_rootfolder_retentiontypes_dict(rootfolder.id), config )
    external_to_internal_retention_converter = ExternalToInternalRetentionTypeConverter(read_rootfolder_retentiontypes_dict(rootfolder.id))

    # Prepare bulk update data for existing folders
    bulk_updates = []
    for db_folder, sim in zip(existing_folders, simulations):
        db_modified_date:date    = db_folder.modified_date   # initialise with existing
        db_retention:Retention   = db_folder.get_retention() # initialise with existing
        sim_retention:RetentionTypeDTO  = external_to_internal_retention_converter.to_internal(sim.external_retention)
        path_retention:Retention = path_matcher.match(db_folder.path)


        # path retention takes priority over other retentions
        if path_retention is not None:
            db_retention = path_retention
        elif sim_retention is not None: #then it must be an endstage retention (clean, issue or missing) so we apply it
            db_retention = Retention(sim_retention.id)
        elif db_retention.retention_id is not None and retention_calculator.is_endstage(db_retention.retention_id): 
            # The retention (possibly set by the user) must not overwrite unless the db_retention is in endstage

            # sim_retention is None and the db_retention is in endstage. This means that the newly scanned simulation must be in another stage than endstage. 
            # We must therefore reset it to None so that the retention_calculator can calculate the correct retention if cleanup is active 
            db_retention = Retention(retention_id=None) 
        else:
            pass # keep existing retention

        if db_modified_date != sim.modified_date:
            db_modified_date = sim.modified_date

        # Evaluate the retention state if retention_calculator exist (valid cleanconfiguration)
        db_retention = retention_calculator.adjust_from_cleanup_configuration_and_modified_date(db_retention, db_modified_date)
        if db_retention.retention_id is None:
            db_retention = retention_calculator.adjust_from_cleanup_configuration_and_modified_date(db_retention, db_modified_date)
            print(f"Folder {db_folder.id} has no retention ID")

        bulk_updates.append({
            "id": db_folder.id,
            "modified_date": db_modified_date,
            "expiration_date": db_retention.expiration_date,
            "retention_id": db_retention.retention_id,
            "pathprotection_id": db_retention.pathprotection_id
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
    
def normalize_and_split_path(filepath: str) -> list[str]:
    normalized:str = normalize_path(filepath)
    # Remove leading and trailing slashes, then split
    segments = [segment for segment in normalized.strip("/").split("/") if segment.strip()]
    
    return segments


def find_existing_node(session: Session, rootfolder_id: int, parent_id: int, name: str) -> FolderNodeDTO | None:
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
def insert_simulations_in_db(rootfolder: RootFolderDTO, simulations: list[FileInfo]):
    """
    Insert missing hierarchy for all simulation filepaths.
    This function only creates the folder structure, attributes will be updated separately.
    """
    if not simulations:
        return {"inserted_hierarchy_count": 0, "failed_path_count": 0, "failed_paths": []}
        
    print(f"start insert_simulations rootfolder_id {rootfolder.id} inserting hierarchy for {len(simulations)} folders")

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
        str =f"Total records in DB for rootfolder {rootfolder.id}: {count}"
        print(str)

        print(f"end insert_simulations rootfolder_id {rootfolder.id} - successfully inserted hierarchy for {inserted_count}/{len(simulations)} paths, {len(failed_paths)} failed")
        
    return {"inserted_hierarchy_count": inserted_count, "failed_path_count": len(failed_paths), "failed_paths": failed_paths}


def insert_hierarchy_for_one_filepath(session: Session, rootfolder_id: int, simulation: FileInfo, nodetypes:dict[str,FolderTypeDTO]) -> int:
    #    Insert missing hierarchy for a single filepath and return the leaf node ID.
    if rootfolder_id is None or rootfolder_id <= 0:
        raise ValueError(f"Invalid rootfolder_id: {rootfolder_id}")

    segments = normalize_and_split_path(simulation.filepath) #can probably be optimized to: pathlib.Path(filepath).as_posix().split('/') because the domains part of the folder should be in the rootfolder  
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
                    path="/".join(current_path_segments),  # Full path up to this segment
                )
            else:
                new_node = FolderNodeDTO(
                    rootfolder_id=rootfolder_id,
                    parent_id=current_parent_id,
                    name=segment,
                    nodetype_id=nodetypes[FolderTypeEnum.VTS_SIMULATION].id,
                    path="/".join(current_path_segments),  # Full path up to this segment
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
