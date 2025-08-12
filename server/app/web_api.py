from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Session, select
from datamodel.dtos import RetentionTypePublic, RetentionTypeDTO
from datamodel.db import Database

from .config import get_test_mode, is_unit_test, is_client_test, is_production
import tempfile
import os
from sqlalchemy import create_engine


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
    if db.is_empty():
        db.create_db_and_tables()
    if is_client_test:
        from testdata.generate_test_data import insert_test_data_in_db
        insert_test_data_in_db(db.get_engine()) 

@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {
        "message": "Welcome to your todo list.",
        "test_mode": get_test_mode().value
    }


@app.get("/config/test-mode", tags=["config"])
async def get_current_test_mode() -> dict:
    """Get the current test mode configuration."""
    return {
        "test_mode": get_test_mode().value,
        "is_unit_test": is_unit_test(),
        "is_client_test": is_client_test(),
        "is_production": is_production()
    }


# endpoint for reading the RetentionTypeDTO
# it must not be able to change the RetentionTypeDTO
@app.get("/retentiontypes/", response_model=list[RetentionTypePublic])
def read_retention_types():
    with Session(Database.get_engine()) as session:
        retention_types = session.exec(select(RetentionTypeDTO)).all()
        return retention_types