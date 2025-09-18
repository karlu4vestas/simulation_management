from datetime import date
from os import path
import csv
import io
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
@app.get("/simulation_domain/", response_model=list[SimulationDomainDTO])
def read_simulation_domains(domain_name: Optional[str] = Query(default=None)):
    with Session(Database.get_engine()) as session:
        simulation_domain = session.exec(select(SimulationDomainDTO)).where(SimulationDomainDTO.name == domain_name).first()
        if not simulation_domain:
            raise HTTPException(status_code=404, detail="SimulationDomain not found")
        return simulation_domain

@app.get("/retentiontypes/", response_model=list[RetentionTypeDTO])
def read_retention_types(simulation_domain_id: int):
    with Session(Database.get_engine()) as session:
        retention_types = session.exec(select(RetentionTypeDTO).where(RetentionTypeDTO.simulation_domain_id == simulation_domain_id)).all()
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
def read_folder_types(simulation_domain_id: int):
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

# -------------------------- all calculations for expiration dates, retentions are done in below functions ---------
# The consistency of these calculations is highly critical to the system working correctly

# The following is called when the user has selected a retention category for a folder in the webclient
# change retentions category for a list of folders in a rootfolder and update the corresponding expiration date
# the expiration date of non-numeric retention is set to None
# the expiration date of numeric retention is set to cleanup_status_date + days_to_cleanup of the retention type
@app.post("/rootfolder/{rootfolder_id}/retentions")
def change_retention_category( rootfolder_id: int, retentions: list[RetentionUpdateDTO]):
    print(f"start change_retention_category rootfolder_id{rootfolder_id} changing number of retention {len(retentions)}")

    # Get retention types for calculations
    retention_types:list[RetentionTypeDTO] = read_root_folder_cleanup_frequencies(rootfolder_id) 
    #convert the numeric retentions to a dict for fast lookup
    retention_types_dict:dict[int,RetentionTypeDTO] = {retention.id:retention for retention in retention_types}
    
    with Session(Database.get_engine()) as session:
        root_folder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        cleanup_status_date = root_folder.cleanup_status_date

        for retention in retentions:
            retention.expiration_date = cleanup_status_date+retention_types_dict[retention.retention_id].days_to_cleanup if (retention_types_dict[retention.retention_id].days_to_cleanup is not None) else None

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

# The following is called by the scheduler when it starts a new cleanup round
# extract all folders with an numeric retentiontypes 
# fail is any of the folders have a missing expiration date 
# if all ok then calculate the retention category for each folder with a numeric retention type based on days_until_expiration = (folder.expiration_date - root_folder.cleanup_status_date).days
def start_new_cleanup_cycle(rootfolder_id: int):
    with Session(Database.get_engine()) as session:
        root_folder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not root_folder:
            raise HTTPException(status_code=404, detail="RootFolder not found")
        
        # Update the cleanup_status_date to current date
        root_folder.cleanup_status_date = func.current_date()
        session.add(root_folder)
        session.commit()
        session.refresh(root_folder)

        #get the retention types for the rootfolder' simulation domain
        retention_types:list[RetentionTypeDTO] = read_retention_types(rootfolder_id)
        #get the numeric retention types in a dict for fast lookup
        retention_types_dict:dict[int,RetentionTypeDTO] = {retention.id:retention for retention in retention_types if retention.days_to_cleanup is not None}

        # extract all folder with rootfolder_id and a a numeric retentiontype
        folders = session.exec(
            select(FolderNodeDTO).where(
                (FolderNodeDTO.rootfolder_id == rootfolder_id) &
                (FolderNodeDTO.retention_id.in_(retention_types_dict.keys()))
            )
        ).all()

        # find all with missing expiration date and set to cleanup_status_date + days_to_analyse for the retention type
        # this should not happen so throw an exception
        folders_missing_expiration_date = [ folder for folder in folders if folder.expiration_date is None ]
        if len(folders_missing_expiration_date)>0:
            raise HTTPException(status_code=404, detail="Path protection not found")

        # now use the expiration dates to calculate the retention categories that the user will see in the webclient
        # @todo: this double loop will be very slow for large number of folders
        for folder in folders:
            days_until_expiration = (folder.expiration_date - root_folder.cleanup_status_date).days
            i = 0
            while( i<len(retention_types.values) and days_until_expiration > retention_types.values[i]) :
                i += 1
            folder.retention_id = retention_types.values[i].id if i<len(retention_types.values) else retention_types.values[-1].id           

        print(f"start_new_cleanup_cycle rootfolder_id: {rootfolder_id} updated retention of {len(folders)} folders" )

# The following is called when inserting or modifying simulations after a scan.
# The operations are: insert new folders, update existing folders, delete folders that no longer exist

# The source data is a table with the following information pr simulation:
# [ filepath, last_modified_date, status ] the field status can be: "issue", "clean", ""

# Insert simulation:  is the most difficult part because the folder hierarchy must be resolved or created first
#   For the leaf FolderDTO set:
#   - retention_id to the None or the id for "clean", "issue" according to the status of each simulation
#   - path to the "filepath"
#   - the simulation' modified date
#   - the expiration_date to  modified_date + rootfolder.days_to_analyse

# Update the simulation: This case is for simulations where the filepath can be matched in caseinsensitive compare 
#  For the leaf FolderDTO set:
#   - retention_id to None or the id for "clean", "issue" according to the status of each simulation
#   - the simulation' modified date
#   - the expiration_date to modified_date + rootfolder.days_to_analyse

# Delete simulation: This case is for simulations in the database that were not found on disk in a full scan of the root folder
#   Set the retention_id to the id for "missing"

def insert_or_update_simulations(rootfolder_id: int, simulations: list[dict]):
    with Session(Database.get_engine()) as session:
        root_folder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not root_folder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        if root_folder.days_to_analyse is None:
            raise HTTPException(status_code=404, detail="Missing root_folder.days_to_analyse")

        #get the retention types for the rootfolder' simulation domain and convert to dict for fast lookup
        retention_types_dict:dict[str,RetentionTypeDTO] = {retention.name.lower():retention for retention in read_retention_types(rootfolder_id)}

        # extract all existing folders for the rootfolder
        existing_folders = session.exec(
            select(FolderNodeDTO).where(
                (FolderNodeDTO.rootfolder_id == rootfolder_id)
            )
        ).all()
        existing_folders_dict:dict[str,FolderNodeDTO] = {folder.path.lower():folder for folder in existing_folders}

        # Prepare bulk update and insert data
        class UpdateFolderDTO:
            id: int             #the folders id    
            retention_id: int 
            modified_date: str
            expiration_date: str 
        bulk_updates:list[UpdateFolderDTO] = []
        bulk_inserts:list[FolderNodeDTO] = []

        for sim in simulations:
            filepath = sim['filepath']
            last_modified_date = sim['last_modified_date']
            status = sim['status'].lower() if 'status' in sim and sim['status'] is not None else ""

            # Determine retention based on status
            if status == "clean":
                retention = retention_types_dict.get("clean")
            elif status == "issue":
                retention = retention_types_dict.get("issue")
            else:
                retention = None  # Default retention for normal simulations where the downstream cleanup will calculate the retention based on the expiration date

            # Calculate expiration date based on days_to_analyse
            expiration_date = last_modified_date + root_folder.days_to_analyse

            # Check if folder already exists (case-insensitive)
            existing_folder = existing_folders_dict.get(filepath.lower())

            if existing_folder:
                upd=UpdateFolderDTO(
                        id              = existing_folder.id,
                        retention_id    = retention.id if retention else None,
                        modified_date   = last_modified_date,
                        expiration_date = expiration_date
                    )
                bulk_updates.append(upd)
            else:
                # Insert new folder
                new_folder: FolderNodeDTO = FolderNodeDTO(
                    rootfolder_id=rootfolder_id,
                    name=path.basename(filepath),
                    path=filepath,
                    retention_id=retention.id if retention else None,
                    modified_date=last_modified_date,
                    expiration_date=expiration_date
                )
                bulk_inserts.append(new_folder)

        session.bulk_update_mappings(FolderNodeDTO, bulk_updates)
        session.commit()

        # Bulk insert new folders
        # @todo need to findout how to make hierarchical inserts
