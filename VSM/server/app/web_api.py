from fastapi import FastAPI, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlmodel import Session, select
from datamodel.dtos import RetentionTypePublic, RetentionTypeDTO, FolderTypeDTO, FolderTypePublic, RootFolderDTO, RootFolderPublic, FolderNodeDTO, FolderNodePublic
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

#develop a @app.get("/folders/")   that extract send all FolderNodeDTOs as csv
@app.get("/folders/", response_model=list[FolderNodeDTO])
def read_folders( rootfolder_id: int ):
    with Session(Database.get_engine()) as session:
        folders = session.exec(select(FolderNodeDTO).where(FolderNodeDTO.rootfolder_id == rootfolder_id)).all()
        return folders




# Endpoint to extract and send all FolderNodeDTOs as CSV
@app.get("/folders/csv")
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
@app.get("/folders/csv-zstd")
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
