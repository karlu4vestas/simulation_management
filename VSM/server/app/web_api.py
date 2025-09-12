from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlmodel import Session, select
from datamodel.dtos import RetentionTypePublic, RetentionTypeDTO, FolderTypeDTO, FolderTypePublic, RootFolderDTO, RootFolderPublic, FolderNodeDTO, PathProtectionDTO, PathProtectionCreate, CleanupFrequencyUpdate, RetentionUpdateDTO
from datamodel.db import Database
from sqlalchemy import create_engine, text, or_, func
from app.config import AppConfig
import csv
import io
from zstandard import ZstdCompressor
from typing import Optional

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
        from testdata.generate_test_data import insert_test_data_in_db
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


# we must only allow the webclient to read the RetentionTypeDTOs
@app.get("/retentiontypes/", response_model=list[RetentionTypePublic])
def read_retention_types():
    with Session(Database.get_engine()) as session:
        retention_types = session.exec(select(RetentionTypeDTO)).all()
        return retention_types
    
# we must only allow the webclient to read the FolderTypeDTOs
@app.get("/foldertypes/", response_model=list[FolderTypePublic])
def read_folder_types():
    with Session(Database.get_engine()) as session:
        retention_types = session.exec(select(FolderTypeDTO)).all()
        return retention_types    
    
# we must only allow the webclient to read the RootFolders
@app.get("/rootfolders/", response_model=list[RootFolderPublic])
def read_root_folders(initials: Optional[str] = Query(default=None)):
    with Session(Database.get_engine()) as session:
        if initials is None or len(initials) == 0:
            retention_types = session.exec(select(RootFolderDTO)).all()
        else:
            retention_types = session.exec(
                select(RootFolderDTO).where(
                    (RootFolderDTO.owner == initials) | (RootFolderDTO.approvers.like(f"%{initials}%"))
                )
            ).all()
        return retention_types        

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

@app.post("/rootfolder/{rootfolder_id}/retentions")
def changeretentions( rootfolder_id: int, retentions: list[RetentionUpdateDTO]):
    print(f"rootfolder_id{rootfolder_id} changing number of retention {len(retentions)}")
    with Session(Database.get_engine()) as session:
        for retention in retentions:
            folder = session.exec(
                select(FolderNodeDTO).where(
                    (FolderNodeDTO.rootfolder_id == rootfolder_id) & 
                    (FolderNodeDTO.id == retention.folder_id)
                )
            ).first()
            
            if not folder:
                raise HTTPException(status_code=404, detail=f"Folder {retention.folder_id} not found in rootfolder {rootfolder_id}")
            
            folder.retention_id = retention.retention_id
            folder.pathprotection_id = retention.pathprotection_id
            session.add(folder)
        
        session.commit()
        
        return {"message": f"Updated {len(retentions)} folders in rootfolder {rootfolder_id}"}

 