import pytest
from sqlmodel import Session
from db.database import Database


@pytest.fixture(scope="function")
def clean_database():
    """Reset the database singleton for each test to ensure test isolation"""
    # Clear the singleton instance before each test
    Database._instance = None
    Database._engine = None
    
    yield
    
    # Clean up after test
    if Database._instance is not None:
        db = Database.get_db()
        db.delete_db()
    Database._instance = None
    Database._engine = None

@pytest.fixture(scope="function")
def test_session():
    """Create a test database session with clean tables using appropriate DB name"""
    # Reset the Database singleton to ensure fresh engine
    Database._instance = None
    Database._engine = None
    
    db:Database = Database.get_db()
    db.delete_db()
    db.create_db_and_tables()
    session:Session = Session(db.get_engine())
    try:
        yield session
    finally:
        session.close()
        db.delete_db()
        # Reset singleton after cleanup
        Database._instance = None
        Database._engine = None


@pytest.fixture(scope="function")
def integration_session():
    # Create a persistent test database session for integration tests
    # Keep session open for the entire integration test
    # delete the database when the integration test is done 
    
    # Reset the Database singleton to ensure fresh engine
    Database._instance = None
    Database._engine = None
    
    db:Database = Database.get_db()
    db.delete_db()
    db.create_db_and_tables()
    session:Session = Session(db.get_engine())
    try:
        yield session
    finally:
        session.close()
        db.delete_db()
        # Reset singleton after cleanup
        Database._instance = None
        Database._engine = None

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
def sample_root_folder_data():
    """Sample data for RootFolderDTO testing"""
    return {
        "simulationdomain_id": 1,
        "folder_id": 1,
        "path": "/test/folder",
        "owner": "JD",
        "approvers": "AB,CD",
        "cycletime": 30,
        "cleanupfrequency": 7
    }


@pytest.fixture(scope="function")
def sample_folder_node_data(test_session):
    from datamodel import dtos
    """Sample data for FolderNodeDTO testing"""
    # Create prerequisites first
    
    # Create simulation domain
    domain = dtos.SimulationDomainDTO(name="TestDomain")
    test_session.add(domain)
    test_session.commit()
    test_session.refresh(domain)
    
    # Create folder type
    folder_type = dtos.FolderTypeDTO(simulationdomain_id=domain.id, name="innernode")
    test_session.add(folder_type)
    test_session.commit()
    test_session.refresh(folder_type)
    
    # Create root folder
    root_folder = dtos.RootFolderDTO(
        simulationdomain_id=domain.id,
        folder_id=1,
        path="/test/folder",
        owner="TestUser",
        approvers="TestApprover",
        cycletime=30,
        cleanupfrequency=7
    )
    test_session.add(root_folder)
    test_session.commit()
    test_session.refresh(root_folder)
    
    return {
        "rootfolder_id": root_folder.id,
        "parent_id": 0,
        "name": "TestFolder",
        "nodetype_id": folder_type.id,
        "path": "",
        "path_ids": ""
    }


@pytest.fixture(scope="function")
def test_root_folder(test_session):
    from datamodel import dtos
    """Create a root folder for tests that need it"""
    
    # Create simulation domain first
    domain = dtos.SimulationDomainDTO(name="TestDomain")
    test_session.add(domain)
    test_session.commit()
    test_session.refresh(domain)
    
    root_folder = dtos.RootFolderDTO(
        simulationdomain_id=domain.id,
        folder_id=1,
        path="/test/folder",
        owner="TestUser",
        approvers="TestApprover",
        cycletime=30,
        cleanupfrequency=7
    )
    test_session.add(root_folder)
    test_session.commit()
    test_session.refresh(root_folder)
    return root_folder


@pytest.fixture(scope="function")
def sample_retention_data(test_session):
    from datamodel import dtos
    """Sample data for RetentionDTO testing"""
    
    # Create simulation domain first
    domain = dtos.SimulationDomainDTO(name="TestDomain")
    test_session.add(domain)
    test_session.commit()
    test_session.refresh(domain)
    
    return {
        "simulationdomain_id": domain.id,
        "name": "numeric",  # Use string value
        "is_system_managed": False,
        "display_rank": 1
    }

@pytest.fixture(scope="function")
def cleanup_scenario_data():
    from tests.integration import testdata_for_import 
    return testdata_for_import.generate_cleanup_scenario_data()


# Pytest markers for test organization
def pytest_configure(config):
    """Register custom pytest markers"""
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
    config.addinivalue_line("markers", "integration: marks tests as integration tests") 
    config.addinivalue_line("markers", "cleanup_workflow: marks tests as cleanup workflow scenarios")
    config.addinivalue_line("markers", "slow: marks tests as slow running")
