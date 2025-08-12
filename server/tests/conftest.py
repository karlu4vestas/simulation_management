import pytest
from sqlmodel import SQLModel, create_engine, Session
from datamodel.db import Database


@pytest.fixture(scope="function")
def clean_database():
    """Reset the database singleton for each test"""
    # Clear the singleton instance before each test
    Database._instance = None
    Database._engine = None
    yield
    # Clean up after test
    Database._instance = None
    Database._engine = None


@pytest.fixture(scope="function")
def database_with_tables(clean_database):
    """Get a database instance with tables created"""
    db = Database.get_db()
    db.create_db_and_tables()
    return db


@pytest.fixture(scope="function")
def test_engine():
    """Create a fresh test database engine for each test"""
    engine = create_engine("sqlite:///:memory:", echo=False)
    SQLModel.metadata.create_all(engine)
    return engine


@pytest.fixture(scope="function")
def test_session(test_engine):
    """Create a test database session"""
    with Session(test_engine) as session:
        yield session


@pytest.fixture(scope="function")
def sample_root_folder_data():
    """Sample data for RootFolderDTO testing"""
    return {
        "path": "/test/folder",
        "folder_id": 1,
        "owner": "JD",
        "approvers": "AB,CD",
        "active_cleanup": False
    }


@pytest.fixture(scope="function")
def sample_folder_node_data():
    """Sample data for FolderNodeDTO testing"""
    return {
        "parent_id": 0,
        "name": "TestFolder",
        "type_id": 1
    }


@pytest.fixture(scope="function") 
def sample_folder_node_with_attributes_data():
    """Sample data for FolderNodeDTO testing with attributes"""
    return {
        "parent_id": 0,
        "name": "TestFolderWithAttrs",
        "type_id": 1,
        "modified": "2025-08-12T10:30:00Z",
        "retention_date": "2025-12-31",
        "retention_id": 3
    }


@pytest.fixture(scope="function")
def sample_retention_data():
    """Sample data for RetentionDTO testing"""
    return {
        "name": "30 days",
        "is_system_managed": "false",
        "display_rank": 1
    }
