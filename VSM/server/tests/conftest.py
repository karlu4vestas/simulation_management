import pytest
import os
from sqlmodel import SQLModel, create_engine, Session
from app.app_config import AppConfig
from db.database import Database


@pytest.fixture(scope="function")
def clean_database():
    """Reset the database singleton for each test"""
    # Clear the singleton instance before each test
    Database._instance = None
    Database._engine = None
    
    # Clean up any existing test database files
    test_db_files = [
        "unit_test_db.sqlite",
        "client_test_db.sqlite", 
        "integration_test.sqlite",
        "integration_test.db"
    ]
    
    for db_file in test_db_files:
        if os.path.exists(db_file):
            os.remove(db_file)
    
    yield
    
    # Clean up after test
    Database._instance = None
    Database._engine = None
    
    # Clean up any database files created during test
    for db_file in test_db_files:
        if os.path.exists(db_file):
            os.remove(db_file)


@pytest.fixture(scope="function")
def test_session(clean_database):
    """Create a test database session with clean tables using appropriate DB name"""
    db_url = AppConfig.get_db_url()
    engine = create_engine(db_url, echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    
    # Clean up the test database file
    if os.path.exists(db_url):
        os.remove(db_url)


@pytest.fixture(scope="function")
def integration_session(clean_database):
    """Create a persistent test database session for integration tests"""
    db_name = "integration_test.db"
    engine = create_engine(f"sqlite:///{db_name}", echo=False)
    SQLModel.metadata.create_all(engine)
    
    # Keep session open for the entire integration test
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()
        if os.path.exists(db_name):
            os.remove(db_name)


@pytest.fixture(scope="function")
def database_with_tables(clean_database):
    """Create a database instance with tables created but no data"""
    from db.database import Database
    
    db = Database.get_db()
    db.create_db_and_tables()
    return db


@pytest.fixture
def sample_data():
    """Consolidated sample data factory for all DTO testing"""
    return {
        "root_folder": {
            "path": "/test/folder",
            "folder_id": 1,
            "owner": "JD",
            "approvers": "AB,CD",
            "cleanupfrequency": "inactive"
        },
        "folder_node_basic": {
            "parent_id": 0,
            "name": "TestFolder",
            "type_id": 1
        },
        "folder_node_with_attrs": {
            "parent_id": 0,
            "name": "TestFolderWithAttrs",
            "type_id": 1,
            "modified": "2025-08-12T10:30:00Z",
            "retention_date": "2025-12-31",
            "retention_id": 3
        },
        "retention": {
            "name": "30 days",
            "is_system_managed": False,
            "display_rank": 1
        }
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
        "is_system_managed": False,
        "display_rank": 1
    }


# Pytest markers for test organization
def pytest_configure(config):
    """Register custom pytest markers"""
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
    config.addinivalue_line("markers", "integration: marks tests as integration tests") 
    config.addinivalue_line("markers", "cleanup_workflow: marks tests as cleanup workflow scenarios")
    config.addinivalue_line("markers", "slow: marks tests as slow running")
