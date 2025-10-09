import io
import csv
from contextlib import asynccontextmanager
from typing import Literal, Optional
from zstandard import ZstdCompressor
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlmodel import Session, func, select
from datamodel.dtos import CleanupConfiguration, CleanupFrequencyDTO, CycleTimeDTO, RetentionTypeDTO, FolderTypeDTO
from datamodel.dtos import RootFolderDTO, FolderNodeDTO, PathProtectionDTO, SimulationDomainDTO, RetentionUpdateDTO 
from db.database import Database
from app.app_config import AppConfig
from datamodel.vts_create_meta_data import insert_vts_metadata_in_db

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    db = Database.get_db()

    if db.is_empty() :
        engine = db.get_engine()
        db.create_db_and_tables()

        if AppConfig.is_client_test():
            with Session(engine) as session:
                insert_vts_metadata_in_db(session)
                insert_test_folder_hierarchy_in_db(session)
    #else:
    #    db.clear_all_tables_and_schemas()
    
    yield
    # Shutdown (if needed in the future)

app = FastAPI(lifespan=lifespan)

origins = [
    "http://localhost:5173",
    "localhost:5173",
    "http://127.0.0.1:5173",
    "127.0.0.1:5173"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Handle favicon requests to avoid 404 logs
@app.get("/favicon.ico")
async def favicon():
    from fastapi.responses import Response
    return Response(status_code=204)

# Get the current test mode configuration.
@app.get("/", tags=["root"])
async def get_current_test_mode() -> dict:
    return {
        "is_unit_test": AppConfig.is_unit_test(),
        "is_client_test": AppConfig.is_client_test(),
        "is_production": AppConfig.is_production()
    }

#-----------------start retrieval of metadata for a simulation domain -------------------
simulation_domain_name: Literal["vts"]
simulation_domain_names = ["vts"]  # Define the allowed domain names

@app.get("/v1/simulationdomains/", response_model=list[SimulationDomainDTO])
def read_simulation_domains():
    with Session(Database.get_engine()) as session:
        simulation_domains = session.exec(select(SimulationDomainDTO)).all()
        if not simulation_domains:
            raise HTTPException(status_code=404, detail="SimulationDomain not found")
        return simulation_domains
#@app.get("/v1/simulationdomains/dict", response_model=dict[str,SimulationDomainDTO]) #disable webapi untill we need it
def read_simulation_domains_dict():
    return {domain.name.lower(): domain for domain in read_simulation_domains()}

@app.get("/v1/simulationdomains/{domain_name}", response_model=SimulationDomainDTO)
def read_simulation_domain_by_name(domain_name: str):
    with Session(Database.get_engine()) as session:
        simulation_domain = session.exec(select(SimulationDomainDTO).where(SimulationDomainDTO.name == domain_name)).first()
        if not simulation_domain:
            raise HTTPException(status_code=404, detail=f"SimulationDomain for {domain_name}not found")
        return simulation_domain

@app.get("/v1/simulationdomains/{simulationdomain_id}/retentiontypes/", response_model=list[RetentionTypeDTO])
def read_retentiontypes_by_domain_id(simulationdomain_id: int):
    with Session(Database.get_engine()) as session:
        retention_types = session.exec(select(RetentionTypeDTO).where(RetentionTypeDTO.simulationdomain_id == simulationdomain_id)).all()
        if not retention_types or len(retention_types) == 0:
            raise HTTPException(status_code=404, detail="retentiontypes not found")
        return retention_types
#@app.get("/simulationdomain/{simulationdomain_id}/retentiontypes/dict", response_model=dict[str,RetentionTypeDTO]) do not expose before needed
def read_retentiontypes_dict_by_domain_id(simulationdomain_id: int):
    return {retention.name.lower(): retention for retention in read_retentiontypes_by_domain_id(simulationdomain_id)}

@app.get("/v1/simulationdomains/{simulationdomain_id}/foldertypes/", response_model=list[FolderTypeDTO])
def read_folder_types_pr_domain_id(simulationdomain_id: int):
    with Session(Database.get_engine()) as session:
        folder_types = session.exec(select(FolderTypeDTO).where(FolderTypeDTO.simulationdomain_id == simulationdomain_id)).all()
        if not folder_types or len(folder_types) == 0:
            raise HTTPException(status_code=404, detail="foldertypes not found")
        return folder_types
#@app.get("/foldertypes/dict", response_model=dict[str,FolderTypeDTO]) #isadble webapi untill we need it
def read_folder_type_dict_pr_domain_id(simulationdomain_id: int):
    return {folder_type.name.lower(): folder_type for folder_type in read_folder_types_pr_domain_id(simulationdomain_id)}

# The cycle time for one simulation is the time from initiating the simulation, til cleanup the simulation. 
@app.get("/v1/simulationdomains/{simulationdomain_id}/cycletimes/", response_model=list[CycleTimeDTO])
def read_cycle_time_by_domain_id(simulationdomain_id: int):
    with Session(Database.get_engine()) as session:
        cycle_time = session.exec(select(CycleTimeDTO).where(CycleTimeDTO.simulationdomain_id == simulationdomain_id)).all()
        if not cycle_time or len(cycle_time) == 0:
            raise HTTPException(status_code=404, detail="CycleTime not found")
        return cycle_time
#@app.get("/simulationdomain/{simulationdomain_id}/cycletimes/dict", response_model=dict[str,CycleTimeDTO]) # do not expose before needed
def read_cycle_time_dict_by_domain_id(simulationdomain_id: int):
    return {cycle.name.lower(): cycle for cycle in read_cycle_time_by_domain_id(simulationdomain_id)}

# The cycle time for one simulation is the time from initiating the simulation, til cleanup the simulation. 
@app.get("/v1/simulationdomains/{simulationdomain_id}/cleanupfrequencies/", response_model=list[CleanupFrequencyDTO])
def read_cleanupfrequency_by_domain_id(simulationdomain_id: int):
    with Session(Database.get_engine()) as session:
        cleanupfrequency = session.exec(select(CleanupFrequencyDTO).where(CleanupFrequencyDTO.simulationdomain_id == simulationdomain_id)).all()
        if not cleanupfrequency or len(cleanupfrequency) == 0:
            raise HTTPException(status_code=404, detail="CleanupFrequency not found")
        return cleanupfrequency
#@app.get("/simulationdomain/{simulationdomain_id}/cleanupfrequencies/dict", response_model=dict[str,CleanupFrequencyDTO]) # do not expose before needed
def read_cleanupfrequency_name_dict_by_domain_id(simulationdomain_id: int):
    return {cleanup.name.lower(): cleanup for cleanup in read_cleanupfrequency_by_domain_id(simulationdomain_id)}
#-----------------end retrieval of metadata for a simulation domain -------------------


#-----------------start maintenance of rootfolders and information under it -------------------
def insert_rootfolder(rootfolder:RootFolderDTO):
    if rootfolder.simulationdomain_id is None or rootfolder.simulationdomain_id == 0:
        raise HTTPException(status_code=404, detail="You must provide a valid simulationdomain_id to create a rootfolder")
    with Session(Database.get_engine()) as session:
        session.add(rootfolder)
        session.commit()
        session.refresh(rootfolder)
        return rootfolder

# we must only allow the webclient to read the RootFolders
@app.get("/v1/rootfolders/", response_model=list[RootFolderDTO])
def read_rootfolders(simulationdomain_id: int, initials: Optional[str] = Query(default="")):
    if initials is None or simulationdomain_id is None or simulationdomain_id == 0:
        raise HTTPException(status_code=404, detail="root_folders not found. you must provide simulation domain and initials")
    return read_rootfolders_by_domain_and_initials(simulationdomain_id, initials)

def read_rootfolders_by_domain_and_initials(simulationdomain_id: int, initials: str= None):
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


# update a rootfolder's cleanup_configuration
@app.post("/v1/rootfolders/{rootfolder_id}/cleanup_configuration")
def update_rootfolder_cleanup_configuration(rootfolder_id: int, cleanup_configuration: CleanupConfiguration):
    is_valid, message = cleanup_configuration.is_valid()
    if not is_valid:
        raise HTTPException(status_code=404, detail=message)

    # if we are to start cleanup then set the cleanup_round_start_date to today if required
    # else set it to None to avoid a scenario where a scheduler tries to start a cleanup round based on an old date
    if cleanup_configuration.can_start_cleanup():
        if cleanup_configuration.cleanup_round_start_date is None:
            cleanup_configuration.cleanup_round_start_date = func.current_date()
    else:
        cleanup_configuration.cleanup_round_start_date = None

    #now the configuration is consistent
    with Session(Database.get_engine()) as session:
        rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")
      
        rootfolder.set_cleanup_configuration(cleanup_configuration)
        session.add(rootfolder)
        session.commit()

        if cleanup_configuration.can_start_cleanup():
            print(f"Starting cleanup for rootfolder {rootfolder_id} with configuration {cleanup_configuration}")
            #from app.web_server_retention_api import start_new_cleanup_cycle  #avoid circular import
            #start_new_cleanup_cycle(rootfolder_id)

        return {"message": f"Cleanup configuration updated for rootfolder {rootfolder_id}"}

def get_cleanup_configuration_by_rootfolder_id(rootfolder_id: int)-> CleanupConfiguration:
    with Session(Database.get_engine()) as session:
        rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")

        cleanup_configuration = rootfolder.get_cleanup_configuration()
        if not cleanup_configuration:
            raise HTTPException(status_code=404, detail="cleanup_configuration not found")

    return cleanup_configuration


@app.get("/v1/rootfolders/{rootfolder_id}/retentiontypes", response_model=list[RetentionTypeDTO])
def read_rootfolder_retentiontypes(rootfolder_id: int):
    retention_types:list[RetentionTypeDTO] = read_rootfolder_retention_type_dict(rootfolder_id).values()
    return retention_types
#@app.get("/v1/rootfolders/{rootfolder_id}/retentiontypes/dict", response_model=dict[str, RetentionTypeDTO]) #do not expose untill needed
def read_rootfolder_retention_type_dict(rootfolder_id: int)-> dict[str, RetentionTypeDTO]:
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")

        retention_types:dict[str,RetentionTypeDTO] = read_retentiontypes_dict_by_domain_id(rootfolder.simulationdomain_id)
        if not retention_types:
            raise HTTPException(status_code=404, detail="retentiontypes not found")
        
        if not retention_types.get("+next",None):
            raise HTTPException(status_code=404, detail="retentiontypes does not contain 'next' retention type")
        
        return retention_types

def read_rootfolder_numeric_retentiontypes_dict(rootfolder_id: int) -> dict[str, RetentionTypeDTO]:
    retention_types_dict:dict[str, RetentionTypeDTO] = read_rootfolder_retention_type_dict(rootfolder_id)
    #filter to keep only retentions with at cycletime
    return {key:retention for key,retention in retention_types_dict.items() if retention.days_to_cleanup is not None}


#develop a @app.get("/folders/")   that extract send all FolderNodeDTOs as csv
@app.get("/v1/rootfolders/{rootfolder_id}/folders/", response_model=list[FolderNodeDTO])
def read_folders( rootfolder_id: int ):
    with Session(Database.get_engine()) as session:
        folders = session.exec(select(FolderNodeDTO).where(FolderNodeDTO.rootfolder_id == rootfolder_id)).all()
        return folders

# Endpoint to extract and send all FolderNodeDTOs as CSV
@app.get("/v1/rootfolders/{rootfolder_id}/folders/csv")
def read_folders_csv():
    def generate_csv():
        engine = Database.get_engine()
        conn = engine.raw_connection()
        if conn is None:
            return io.StringIO()
        else :
            cursor = conn.cursor()
            
            # Get column names
            cursor.execute("PRAGMA table_info(foldernodedto)")
            columns = [row[1] for row in cursor.fetchall()]
            
            # Create CSV header
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(columns)
            yield output.getvalue()
            
            # Stream data in chunks
            chunk_size = 10000
            offset = 0
            
            while True:
                cursor.execute(f"SELECT * FROM foldernodedto LIMIT {chunk_size} OFFSET {offset}")
                rows = cursor.fetchall()
                
                if not rows:
                    conn.close()
                    conn=None
                    break
                
                # Write chunk to CSV
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerows(rows)
                yield output.getvalue()
                
                offset += chunk_size
        
    return StreamingResponse(
        generate_csv(), 
        media_type="text/csv", 
        headers={"Content-Disposition": "attachment; filename=folders.csv"}
    )

# Endpoint to extract and send all FolderNodeDTOs as compressed CSV using zstd
@app.get("/v1/rootfolders/{rootfolder_id}/folders/csv-zstd")
def read_folders_csv_zstd():
    def generate_compressed_csv():
        compressor = ZstdCompressor(level=3)
        engine = Database.get_engine()
        
        conn = engine.raw_connection()
        if conn is None:
            return io.StringIO()
        else :
            cursor = conn.cursor()
            
            # Get column names and create header
            cursor.execute("PRAGMA table_info(foldernodedto)")
            columns = [row[1] for row in cursor.fetchall()]
            
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(columns)
            yield compressor.compress(output.getvalue().encode('utf-8'))
            
            # Stream data in chunks
            chunk_size = 10000
            offset = 0
            
            while True:
                cursor.execute(f"SELECT * FROM foldernodedto LIMIT {chunk_size} OFFSET {offset}")
                rows = cursor.fetchall()
                
                if not rows:
                    conn.close()
                    conn=None
                    break
                
                # Write chunk to CSV and compress
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerows(rows)
                yield compressor.compress(output.getvalue().encode('utf-8'))
                
                offset += chunk_size
        
    return StreamingResponse(
        generate_compressed_csv(), 
        media_type="application/zstd",
        headers={
            "Content-Disposition": "attachment; filename=folders.csv.zst",
            "Content-Encoding": "zstd"
        }
    )

#get all path protections for a specific root folder
@app.get("/v1/rootfolders/{rootfolder_id}/pathprotections", response_model=list[PathProtectionDTO])
def read_pathprotections( rootfolder_id: int ):
    with Session(Database.get_engine()) as session:
        paths = session.exec(select(PathProtectionDTO).where(PathProtectionDTO.rootfolder_id == rootfolder_id)).all()
        return paths

# Add a new path protection to a specific root folder
@app.post("/v1/rootfolders/{rootfolder_id}/pathprotection")
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

# Delete a path protection from a specific root folder
@app.delete("/v1/rootfolders/{rootfolder_id}/pathprotection")
def delete_path_protection(rootfolder_id: int, protection_id: int):
    with Session(Database.get_engine()) as session:
        # Find the path protection by ID and rootfolder_id
        protection = session.exec( select(PathProtectionDTO).where((PathProtectionDTO.id == protection_id) & (PathProtectionDTO.rootfolder_id == rootfolder_id)) ).first()
        if not protection:
            raise HTTPException(status_code=404, detail="Path protection not found")
        
        session.delete(protection)
        session.commit()
        return {"message": f"Path protection {protection_id} deleted"}
    
# The following can be called when the securefolder' cleanup configuration is fully defined meaning that rootfolder.cleanupfrequency and rootfolder.cycletime msut be set
# it will adjust the expiration dates to the user selected retention categories in the webclient
#   the expiration date for non-numeric retentions is set to None
#   the expiration date for numeric retention is set to cleanup_round_start_date + days_to_cleanup for the user selected retention type
@app.post("/v1/rootfolders/{rootfolder_id}/retentions")
def change_retentions(rootfolder_id: int, retentions: list[RetentionUpdateDTO]):
    print(f"start change_retention_category rootfolder_id{rootfolder_id} changing number of retention {len(retentions)}")
    
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        cleanup_config: CleanupConfiguration = rootfolder.get_cleanup_configuration()
        # the cleanup_round_start_date must be set for calculation of retention.expiration_date. It could make sens to set path retentions before the first cleanup round. 
        # However, when a cleanup round is started they have time at "rootfolder.cycletime" to adjust retention
        if not cleanup_config.can_start_cleanup():
            raise HTTPException(status_code=400, detail="The rootFolder's CleanupConfiguration is is missing cleanupfrequency, cleanup_round_start_date or cycletime ")

        # Get retention types for calculations
        retention_calculator: RetentionCalculator = RetentionCalculator(read_rootfolder_numeric_retentiontypes_dict(rootfolder_id), cleanup_config) 

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


#-----------------end maintenance of rootfolders and information under it -------------------
from dataclasses import dataclass
from datetime import date
from datamodel.dtos import Retention 
from datamodel.dtos import FolderTypeEnum
from datamodel.retention_validators import RetentionCalculator, PathProtectionEngine
from sqlalchemy import func, case

@dataclass
class FileInfo:
    filepath: str
    modified_date: date
    nodetype: FolderTypeEnum
    retention_id: int | None = None
    id: int = None   # will be used during updates


# -------------------------- all calculations for expiration dates, retentions are done in below functions ---------
# The consistency of these calculations is highly critical to the system working correctly




# The following is called by the scheduler (or webclient through update_rootfolder_cleanup_configuration at present) to starts a new cleanup round
# That is the retentions must be adapted to the new cleanup_configuration using the existing expiration dates
#
# The following update will happen
#   - folders with numeric retentiontypes: calculation of retention category and expiration dates
#   - folders with non-numeric retentiontypes: The expiration date is set to None in order to show that cleanup must not be done
def start_new_cleanup_cycle(rootfolder_id: int):
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        cleanup_config: CleanupConfiguration = rootfolder.get_cleanup_configuration()
        if not cleanup_config.can_start_cleanup():
            return {"message": f"Cannot start cleanup for rootfolder {rootfolder_id} due to invalid cleanup configuration {cleanup_config}"}
        elif cleanup_config.cleanup_round_start_date is None:
            cleanup_config.cleanup_round_start_date = func.current_date()
            rootfolder.set_cleanup_configuration(cleanup_config)

        # Update the cleanup_round_start_date to current date
        session.add(rootfolder)
        session.commit()
        session.refresh(rootfolder)

        # Get retention types for calculations
        retention_calculator: RetentionCalculator = RetentionCalculator(read_rootfolder_numeric_retentiontypes_dict(rootfolder_id), cleanup_config) 

        # extract all folders with rootfolder_id. 
        # We can possibly optimise by 
        #   setting expiration date to None for non-numeric retention types
        #   only selecting folders with a numeric retentiontype for further processing
        folders = session.exec( select(FolderNodeDTO).where( FolderNodeDTO.rootfolder_id == rootfolder_id) ).all()


        # Update retentions 
        for folder in folders:
            folder.set_retention( retention_calculator.adjust_retentions_from_cleanup_configuration( folder.get_retention()) )
            session.add(folder)

        #@TODO  must we make a bulk commit here to save the modified retention ids ?
        session.commit()
        print(f"start_new_cleanup_cycle rootfolder_id: {rootfolder_id} updated retention of {len(folders)} folders" )

# This function will insert new simulations and update existing simulations
#  - I can be called with all simulations modified or not since last scan, because the list of simulations will be reduced to simulations that have changed or are new
#  - in all cases the caller's change to modify, retention_id (not None retention_id) will be applied and path protection will be applied and takes priority if any
#  - the expiration date and numeric retentions will:
#       if cleanup is active then recalculate them 
#       if cleanup is inactive then ignore them 
def insert_or_update_simulation_in_db(rootfolder_id: int, simulations: list[FileInfo]):
    #here we can remove all existing simulation if nothing changes for them
    return insert_or_update_simulation_in_db_internal(rootfolder_id, simulations)



#the function is slow so before calling this function remove all simulation that does not provide new information. new simulation, new modified date, new retention
def insert_or_update_simulation_in_db_internal(rootfolder_id: int, simulations: list[FileInfo]):
    if len(simulations) == 0: 
        return {"nothing to do. simulation count": 0}

    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        #if rootfolder.cycletime is None:
        #    raise HTTPException(status_code=400, detail="Missing root_folder.days_to_analyse")

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
        existing_folders                    = None

        insertion_results: dict[str, int]   = insert_simulations_in_db(rootfolder, insert_simulations)
        insert_simulations = None

        print(insertion_results)
        if insertion_results.get("failed_path_count", 0) > 0:
            print(f"Failed to insert some simulations for rootfolder {rootfolder_id}: {insertion_results.get('failed_paths', [])}")
            raise HTTPException(status_code=500, detail=f"Failed to insert some new paths: {insertion_results.get('failed_paths', [])}")

        update_results: dict[str, int] = update_simulation_attributes_in_db_internal(session, rootfolder, simulations)

        # Commit all changes
        session.commit()

        print(update_results)
        insertion_results.update(update_results)
        return insertion_results


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
    path_retention_dict:dict[str, RetentionTypeDTO] = read_rootfolder_retention_type_dict(rootfolder.id)
    if path_retention_dict.get("path", None) is None:
        raise HTTPException(status_code=500, detail=f"Unable to retrieve node_type_id=vts_simulation for {rootfolder.id}")
    path_matcher:PathProtectionEngine = PathProtectionEngine(read_pathprotections(rootfolder.id), path_retention_dict.get("path", None).id)

    # do we have a cleanup configuration that allows start of cleanup
    can_start_cleanup:bool = rootfolder.get_cleanup_configuration().can_start_cleanup()

    #Prepare calculation of numeric retention_id.  
    retention_calculator = RetentionCalculator( read_rootfolder_numeric_retentiontypes_dict(rootfolder.id), rootfolder.get_cleanup_configuration() ) if can_start_cleanup else None


    # Prepare bulk update data for existing folders
    bulk_updates = []
    for folder, sim in zip(existing_folders, simulations):
        modified_date:date       = folder.modified_date
        retention:Retention      = folder.get_retention()
        path_retention:Retention = path_matcher.match(folder.path)
        sim_retention:Retention  = Retention(sim.retention_id) if sim.retention_id is not None else None

        # step 1: set changes to non numeric retentions with path protection having priority
        if not path_retention is None:
            retention = path_retention
        elif not sim_retention is None: #retention that the caller want to set. Could be #retention_id for "clean" or "issue"
            retention = sim_retention
        elif modified_date != sim.modified_date: 
            modified_date = sim.modified_date
            #step 2: if the modified date changed we need to update the retention because it might be numeric
            if retention_calculator is not None: #is cleanup active then retention_calculator is exist
                retention = retention_calculator.adjust_from_cleanup_configuration_and_modified_date(retention, sim.modified_date)
        else: 
            # all new simulation are created with modified_date=None in insert_simulations_in_db and are therefore handled above 
            # ANYWAY lets process this case just to be sure. we can optimise later
            if retention_calculator is not None:
                retention = retention_calculator.adjust_from_cleanup_configuration_and_modified_date(retention, modified_date)
            
        bulk_updates.append({
            "id": folder.id,
            "modified_date": sim.modified_date,
            "expiration_date": retention.expiration_date,
            "retention_id": retention.retention_id,
            "pathprotection_id": retention.pathprotection_id
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
        
        print(f"end insert_simulations rootfolder_id {rootfolder.id} - successfully inserted hierarchy for {inserted_count}/{len(simulations)} paths, {len(failed_paths)} failed")
        
    return {"inserted_hierarchy_count": inserted_count, "failed_path_count": len(failed_paths), "failed_paths": failed_paths}


def insert_hierarchy_for_one_filepath(session: Session, rootfolder_id: int, simulation: FileInfo, nodetypes:dict[str,FolderTypeDTO]) -> int:
    """
    Insert missing hierarchy for a single filepath and return the leaf node ID.
    
    Args:
        session: Database session
        rootfolder_id: ID of the root folder
        filepath: The complete file path to ensure exists
        innernode_type_id: The nodetype_id to use for inner nodes where as the node for the full simulation filepath will use simulation.nodetype_id
    Returns:
        ID of the leaf node (final segment in the path)
    
    Raises:
        ValueError: If filepath is invalid or empty
        HTTPException: If database constraints are violated
    """
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
            # @TODO need to add modified date to the leafnode
            if index < len(segments) - 1:
                new_node = FolderNodeDTO(
                    rootfolder_id=rootfolder_id,
                    parent_id=current_parent_id,
                    name=segment,
                    #@TODO very vts specifc - must be made generic when other departments are onboarded
                    nodetype_id = nodetypes[FolderTypeEnum.INNERNODE].id,  
                    path="/".join(current_path_segments),  # Full path up to this segment
                    path_ids=""  # Will be set after getting the ID
                )
            else:
                new_node = FolderNodeDTO(
                    rootfolder_id=rootfolder_id,
                    parent_id=current_parent_id,
                    name=segment,
                    nodetype_id=nodetypes[FolderTypeEnum.VTS_SIMULATION].id,
                    path="/".join(current_path_segments),  # Full path up to this segment
                    modified_date=simulation.modified_date,
                    retention_id=simulation.retention_id,
                    path_ids=""  # Will be set after getting the ID
                )            
            try:
                session.add(new_node)
                session.flush()  # Flush to get the ID without committing
                #session.refresh(new_node)  # Refresh to ensure we have the latest state

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