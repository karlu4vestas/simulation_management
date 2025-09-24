import io
import csv
from typing import Literal
from typing import Optional
from zstandard import ZstdCompressor
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlmodel import Session, func, select
from datamodel.dtos import CleanupConfiguration, CleanupFrequencyDTO, CycleTimeDTO, RetentionTypeDTO, FolderTypeDTO, RootFolderDTO, FolderNodeDTO, PathProtectionDTO, SimulationDomainDTO
from datamodel.db import Database
from app.config import AppConfig
#from app.web_server_retention_api import start_new_cleanup_cycle
from testdata.vts_generate_test_data import insert_test_data_in_db

app = FastAPI()

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

@app.on_event("startup")
def on_startup():
    db = Database.get_db()

    if db.is_empty() and AppConfig.is_client_test():
        db.clear_all_tables_and_schemas()
        db.create_db_and_tables()
        insert_test_data_in_db(db.get_engine()) 
    
    if db.is_empty():
        db.create_db_and_tables()


@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {
        "message": "Welcome to your todo list.",
        "test_mode": AppConfig.get_test_mode().value
    }


@app.get("/config/test-mode", tags=["config"])
async def get_current_test_mode() -> dict:
    # Get the current test mode configuration.
    return {
        "test_mode": AppConfig.get_test_mode().value,
        "is_unit_test": AppConfig.is_unit_test(),
        "is_client_test": AppConfig.is_client_test(),
        "is_production": AppConfig.is_production()
    }

#-----------------start retrieval of metadata for a simulation domain -------------------
simulation_domain_name: Literal["vts"]
simulation_domain_names = ["vts"]  # Define the allowed domain names

@app.get("/simulation_domain/", response_model=list[SimulationDomainDTO])
def read_simulation_domains(domain_name: Optional[str] = Query(default=None)):
    with Session(Database.get_engine()) as session:
        simulation_domain = session.exec(select(SimulationDomainDTO)).where(SimulationDomainDTO.name == domain_name).first()
        if not simulation_domain:
            raise HTTPException(status_code=404, detail="SimulationDomain not found")
        return simulation_domain
@app.get("/simulation_domain/dict", response_model=dict[str,SimulationDomainDTO])
def read_simulation_domains_dict(domain_name: Optional[str] = Query(default=None)):
    return {domain.name.lower(): domain for domain in read_simulation_domains(domain_name)}

@app.get("/retentiontypes/", response_model=list[RetentionTypeDTO])
def read_retentiontypes_by_domain_id(simulation_domain_id: int):
    with Session(Database.get_engine()) as session:
        retention_types = session.exec(select(RetentionTypeDTO).where(RetentionTypeDTO.simulation_domain_id == simulation_domain_id)).all()
        if not retention_types or len(retention_types) == 0:
            raise HTTPException(status_code=404, detail="retentiontypes not found")
        return retention_types
@app.get("/retentiontypes/dict", response_model=dict[str,RetentionTypeDTO])
def read_retentiontypes_dict_by_domain_id(simulation_domain_id: int):
    return {retention.name.lower(): retention for retention in read_retentiontypes_by_domain_id(simulation_domain_id)}

@app.get("/foldertypes/", response_model=list[FolderTypeDTO])
def read_folder_types_pr_domain_id(simulation_domain_id: int):
    with Session(Database.get_engine()) as session:
        folder_types = session.exec(select(FolderTypeDTO).where(FolderTypeDTO.simulation_domain_id == simulation_domain_id)).all()
        if not folder_types or len(folder_types) == 0:
            raise HTTPException(status_code=404, detail="foldertypes not found")
        return folder_types
@app.get("/foldertypes/dict", response_model=dict[str,FolderTypeDTO])
def read_folder_type_dict_pr_domain_id(simulation_domain_id: int):
    return {folder_type.name.lower(): folder_type for folder_type in read_folder_types_pr_domain_id(simulation_domain_id)}

# The cycle time for one simulation is the time from initiating the simulation, til cleanup the simulation. 
@app.get("/cycletime/", response_model=list[CycleTimeDTO])
def read_cycle_time(simulation_domain_id: int):
    with Session(Database.get_engine()) as session:
        cycle_time = session.exec(select(CycleTimeDTO).where(CycleTimeDTO.simulation_domain_id == simulation_domain_id)).all()
        if not cycle_time or len(cycle_time) == 0:
            raise HTTPException(status_code=404, detail="CycleTime not found")
        return cycle_time
@app.get("/cycletime/dict", response_model=dict[str,CycleTimeDTO])
def read_cycle_time_dict(simulation_domain_id: int):
    return {cycle.name.lower(): cycle for cycle in read_cycle_time(simulation_domain_id)}

# The cycle time for one simulation is the time from initiating the simulation, til cleanup the simulation. 
@app.get("/cleanupfrequencies/", response_model=list[CleanupFrequencyDTO])
def read_cleanup_frequency(simulation_domain_id: int):
    with Session(Database.get_engine()) as session:
        cleanup_frequency = session.exec(select(CleanupFrequencyDTO).where(CleanupFrequencyDTO.simulation_domain_id == simulation_domain_id)).all()
        if not cleanup_frequency or len(cleanup_frequency) == 0:
            raise HTTPException(status_code=404, detail="CleanupFrequency not found")
        return cleanup_frequency
@app.get("/cleanupfrequencies/dict", response_model=dict[str,CleanupFrequencyDTO])
def read_cleanup_frequency_dict(simulation_domain_id: int):
    return {cleanup.name.lower(): cleanup for cleanup in read_cleanup_frequency(simulation_domain_id)}


#-----------------end retrieval of metadata for a simulation domain -------------------

# we must only allow the webclient to read the RootFolders
@app.get("/rootfolders/", response_model=list[RootFolderDTO])
def read_root_folders(simulation_domain_id: int, initials: Optional[str] = Query(default=None)):
    with Session(Database.get_engine()) as session:
        if initials is None or len(initials) == 0 or simulation_domain_id is None or simulation_domain_id == 0:
            raise HTTPException(status_code=404, detail="root_folders not found. you must provide simulation domain and initials")
        else:
            root_folders = session.exec(
                select(RootFolderDTO).where( (RootFolderDTO.simulation_domain_id == simulation_domain_id) &
                    ((RootFolderDTO.owner == initials) | (RootFolderDTO.approvers.like(f"%{initials}%")))
                )
            ).all()
        return root_folders        

@app.get("/rootfolder/{root_folder_id}/cleanupfrequency", response_model=CleanupFrequencyDTO)
def read_rootfolder_cleanupfrequency(root_folder_id: int):
    with Session(Database.get_engine()) as session:
        rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == root_folder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")

        return rootfolder.cleanup_frequency_days

# update a rootfolder's cleanup_configuration
@app.post("/rootfolder/{rootfolder_id}/cleanup_configuration")
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
            start_new_cleanup_cycle(rootfolder_id)

        return {"message": f"Cleanup configuration updated for rootfolder {rootfolder_id}"}

@app.get("/rootfolder/{rootfolder_id}/retentiontypes", response_model=list[str, RetentionTypeDTO])
def read_rootfolder_retentiontypes(rootfolder_id: int):
    return read_rootfolder_retention_type_dict(rootfolder_id).values()

@app.get("/rootfolder/{rootfolder_id}/retentiontypes/dict", response_model=dict[str, RetentionTypeDTO])
def read_rootfolder_retention_type_dict(rootfolder_id: int):
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="rootfolder not found")

        retention_types:dict[str,RetentionTypeDTO] = read_retentiontypes_dict_by_domain_id(rootfolder.simulation_domain_id)
        if not retention_types:
            raise HTTPException(status_code=404, detail="retentiontypes not found")
        
        if not retention_types.get("path",None):
            raise HTTPException(status_code=404, detail="retentiontypes does not contain 'path' retention type")
        retention_types["path"] = rootfolder.cycletime
        return retention_types

def read_rootfolder_numeric_retentiontypes_dict(rootfolder_id: int) -> dict[str, RetentionTypeDTO]:
    retention_types_dict:dict[str, RetentionTypeDTO] = read_rootfolder_retention_type_dict(rootfolder_id)
    #filter to keep only retentions with at cycletime
    return {key:retention for key,retention in retention_types_dict.items() if retention.days_to_cleanup is not None}


#develop a @app.get("/folders/")   that extract send all FolderNodeDTOs as csv
@app.get("/rootfolder/{rootfolder_id}/folders/", response_model=list[FolderNodeDTO])
def read_folders( rootfolder_id: int ):
    with Session(Database.get_engine()) as session:
        folders = session.exec(select(FolderNodeDTO).where(FolderNodeDTO.rootfolder_id == rootfolder_id)).all()
        return folders

# Endpoint to extract and send all FolderNodeDTOs as CSV
@app.get("/rootfolder/{rootfolder_id}/folders/csv")
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
@app.get("/rootfolder/{rootfolder_id}/folders/csv-zstd")
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
@app.get("/rootfolder/{rootfolder_id}/pathprotections", response_model=list[PathProtectionDTO])
def read_pathprotections( rootfolder_id: int ):
    with Session(Database.get_engine()) as session:
        paths = session.exec(select(PathProtectionDTO).where(PathProtectionDTO.rootfolder_id == rootfolder_id)).all()
        return paths

# Add a new path protection to a specific root folder
@app.post("/rootfolder/{rootfolder_id}/pathprotection")
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
        return {"message": f"Path protection added for path '{path_protection.path}'"}

# Delete a path protection from a specific root folder
@app.delete("/rootfolder/{rootfolder_id}/pathprotection")
def delete_path_protection(rootfolder_id: int, protection_id: int):
    with Session(Database.get_engine()) as session:
        # Find the path protection by ID and rootfolder_id
        protection = session.exec( select(PathProtectionDTO).where((PathProtectionDTO.id == protection_id) & (PathProtectionDTO.rootfolder_id == rootfolder_id)) ).first()
        if not protection:
            raise HTTPException(status_code=404, detail="Path protection not found")
        
        session.delete(protection)
        session.commit()
        return {"message": f"Path protection {protection_id} deleted"}