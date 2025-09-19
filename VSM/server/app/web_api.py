from datetime import date
from os import path
import csv
import io
from typing import Literal
from zstandard import ZstdCompressor
from typing import Optional
from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlmodel import Session, select
from datamodel.dtos import CleanupFrequencyDTO, DaysToAnalyseDTO, RetentionTypeDTO, FolderTypeDTO, RootFolderDTO, FolderNodeDTO, PathProtectionDTO, CleanupFrequencyUpdate, RetentionUpdateDTO, SimulationDomainDTO
from datamodel.db import Database
from sqlalchemy import create_engine, text, or_, func
from app.config import AppConfig
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

@app.get("/retentiontypes/", response_model=list[RetentionTypeDTO])
def read_retention_types_by_domain_id(simulation_domain_id: int):
    with Session(Database.get_engine()) as session:
        retention_types = session.exec(select(RetentionTypeDTO).where(RetentionTypeDTO.simulation_domain_id == simulation_domain_id)).all()
        if not retention_types or len(retention_types) == 0:
            raise HTTPException(status_code=404, detail="retentiontypes not found")
        return retention_types

@app.get("/retentiontypes/", response_model=list[RetentionTypeDTO])
def read_retention_types_by_domain_name(domain_name: str):
    if not domain_name in simulation_domain_names:
        raise HTTPException(status_code=400, detail=f"domain_name must be one of {simulation_domain_names}")
    
    domain_id:int= read_simulation_domains(domain_name=domain_name).id
    if domain_id is None or domain_id == 0:
        raise HTTPException(status_code=404, detail="SimulationDomain not found")
    
    with Session(Database.get_engine()) as session:
        retention_types = session.exec(select(RetentionTypeDTO).where(RetentionTypeDTO.simulation_domain_id == domain_id)).all()
        if not retention_types or len(retention_types) == 0:
            raise HTTPException(status_code=404, detail="retentiontypes not found")
        return retention_types

@app.get("/foldertypes/", response_model=list[FolderTypeDTO])
def read_folder_types(simulation_domain_id: int):
    with Session(Database.get_engine()) as session:
        folder_types = session.exec(select(FolderTypeDTO).where(FolderTypeDTO.simulation_domain_id == simulation_domain_id)).all()
        if not folder_types or len(folder_types) == 0:
            raise HTTPException(status_code=404, detail="foldertypes not found")
        return folder_types

@app.get("/foldertypes/", response_model=list[DaysToAnalyseDTO])
def read_days_to_analyse(simulation_domain_id: int):
    with Session(Database.get_engine()) as session:
        days_to_analyse = session.exec(select(DaysToAnalyseDTO).where(DaysToAnalyseDTO.simulation_domain_id == simulation_domain_id)).all()
        if not days_to_analyse or len(days_to_analyse) == 0:
            raise HTTPException(status_code=404, detail="DaysToAnalyse not found")
        return days_to_analyse

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

# update a rootfolder's cleanup_frequency
@app.post("/rootfolder/{rootfolder_id}/cleanup-frequency")
def update_rootfolder_cleanup_frequency(rootfolder_id: int, update_data: CleanupFrequencyUpdate):
    with Session(Database.get_engine()) as session:
        # Find the rootfolder by ID
        rootfolder = session.exec(
            select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)
        ).first()
        
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")
        
        # Update the cleanup_frequency
        rootfolder.cleanup_frequency = update_data.cleanup_frequency
        session.add(rootfolder)
        session.commit()
        session.refresh(rootfolder)
        
        return {"message": f"Cleanup frequency updated to '{update_data.cleanup_frequency}' for rootfolder {rootfolder_id}"}

@app.get("/rootfolder/{root_folder_id}/cleanup_frequencies", response_model=list[CleanupFrequencyDTO])
def read_root_folder_cleanup_frequencies(root_folder_id: int):
    with Session(Database.get_engine()) as session:
        root_folder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == root_folder_id)).first()
        if not root_folder:
            raise HTTPException(status_code=404, detail="root_folder not found")

        cleanup_frequencies = session.exec(select(CleanupFrequencyDTO).where(CleanupFrequencyDTO.simulation_domain_id == root_folder.simulation_domain_id)).all()
        if not cleanup_frequencies or len(cleanup_frequencies) == 0:
            raise HTTPException(status_code=404, detail="cleanup_frequencies not found")
        
        #need to set the "Next" retentions days_to_cleanup to the number of days for the root_folder cleanup_frequency 
        root_folder_cleanup_frequency = session.exec(select(CleanupFrequencyDTO).where(CleanupFrequencyDTO.id == root_folder.cleanup_frequency_days)).first()
        if not root_folder_cleanup_frequency:
            raise HTTPException(status_code=404, detail="root_folder's cleanup_frequency not found")

        # Find and update the cleanup frequency with name "Next" efficiently
        next_cleanup = next((cf for cf in cleanup_frequencies if cf.name == "Next"), None)
        if next_cleanup:
            next_cleanup.days = root_folder_cleanup_frequency.days
        else:
            raise HTTPException(status_code=404, detail="cleanup_frequencies failed to find the retention 'Next'")

        return cleanup_frequencies

#develop a @app.get("/folders/")   that extract send all FolderNodeDTOs as csv
@app.get("/rootfolder/{rootfolder_id}/folders/", response_model=list[FolderNodeDTO])
def read_folders( rootfolder_id: int ):
    with Session(Database.get_engine()) as session:
        folders = session.exec(select(FolderNodeDTO).where(FolderNodeDTO.rootfolder_id == rootfolder_id)).all()
        #@TODO  calculate the retention setting using the retention_days, state of the using current date and retention type

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

#get alle path protections for  
@app.get("/pathprotections/{rootfolder_id}", response_model=list[PathProtectionDTO])
def read_pathprotections( rootfolder_id: int ):
    with Session(Database.get_engine()) as session:
        paths = session.exec(select(PathProtectionDTO).where(PathProtectionDTO.rootfolder_id == rootfolder_id)).all()
        return paths

# Add a new path protection to a specific root folder
@app.post("/pathprotections")
def add_path_protection(path_protection: PathProtectionDTO):
    print(f"Adding path protection {path_protection}")
    with Session(Database.get_engine()) as session:
        # Check if path protection already exists for this path in this rootfolder
        existing_protection = session.exec(
            select(PathProtectionDTO).where(
                (PathProtectionDTO.rootfolder_id == path_protection.rootfolder_id) & 
                (PathProtectionDTO.folder_id == path_protection.folder_id)
            )
        ).first()
        
        if existing_protection:
            raise HTTPException(status_code=409, detail="Path protection already exists for this path")
        
        # Create new path protection
        new_protection = PathProtectionDTO(
            rootfolder_id=path_protection.rootfolder_id,
            folder_id=path_protection.folder_id,
            path=path_protection.path
        )
        
        session.add(new_protection)
        session.commit()
        session.refresh(new_protection)
        return {"message": f"Path protection added for path '{path_protection.path}'", "id": new_protection.id}

# Delete a path protection from a specific root folder
@app.delete("/pathprotections/{protection_id}")
def delete_path_protection(protection_id: int):
    with Session(Database.get_engine()) as session:
        # Find the path protection by ID and rootfolder_id
        protection = session.exec(
            select(PathProtectionDTO).where((PathProtectionDTO.id == protection_id))
        ).first()
        
        if not protection:
            raise HTTPException(status_code=404, detail="Path protection not found")
        
        session.delete(protection)
        session.commit()
        
        return {"message": f"Path protection {protection_id} deleted"}