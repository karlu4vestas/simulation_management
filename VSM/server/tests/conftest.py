from collections import deque
import random
import pytest
from sqlmodel import Session
import os
from datetime import date
from app.app_config import AppConfig
from db.database import Database
from datamodel.dtos import CleanupConfiguration, RootFolderDTO, SimulationDomainDTO, FolderTypeDTO
from .integration.testdata_for_import import RootFolderWithMemoryFolders, RootFolderWithMemoryFolderTree, flatten_folder_structure 
from .integration.testdata_for_import import generate_in_memory_rootfolder_and_folder_hierarchies, randomize_modified_dates_of_leaf_folders


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
    """Sample data for FolderNodeDTO testing"""
    # Create prerequisites first
    
    # Create simulation domain
    domain = SimulationDomainDTO(name="TestDomain")
    test_session.add(domain)
    test_session.commit()
    test_session.refresh(domain)
    
    # Create folder type
    folder_type = FolderTypeDTO(simulationdomain_id=domain.id, name="innernode")
    test_session.add(folder_type)
    test_session.commit()
    test_session.refresh(folder_type)
    
    # Create root folder
    root_folder = RootFolderDTO(
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
    """Create a root folder for tests that need it"""
    from datamodel.dtos import RootFolderDTO, SimulationDomainDTO
    
    # Create simulation domain first
    domain = SimulationDomainDTO(name="TestDomain")
    test_session.add(domain)
    test_session.commit()
    test_session.refresh(domain)
    
    root_folder = RootFolderDTO(
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
    """Sample data for RetentionDTO testing"""
    from datamodel.dtos import SimulationDomainDTO
    
    # Create simulation domain first
    domain = SimulationDomainDTO(name="TestDomain")
    test_session.add(domain)
    test_session.commit()
    test_session.refresh(domain)
    
    return {
        "simulationdomain_id": domain.id,
        "name": "30 days",
        "is_system_managed": False,
        "display_rank": 1
    }

@pytest.fixture(scope="function")
def cleanup_scenario_data():
    # Sample data with 3 datasets for 2 root folders:
    #  - part one with the first root folder and a list of all its subfolders in random order
    #  - part two and three with a random split of each of the second rootfolders list of subfolders
    # The root folder's cleanup configuration is not initialised means that assumes default values

    number_of_rootfolders = 2
    cleanup_configuration = CleanupConfiguration(cycletime=30, cleanupfrequency=7, cleanup_start_date=date(2000, 1, 1))
    random_days:int = 10
    rootfolders: deque[RootFolderWithMemoryFolderTree] = deque( generate_in_memory_rootfolder_and_folder_hierarchies(number_of_rootfolders) )
    assert len(rootfolders) > 0

    #Split the two root folder in three parts:
    # first rootfolder with all its folders randomized
    first_rootfolder: RootFolderWithMemoryFolders = flatten_folder_structure(rootfolders.popleft())
    first_rootfolder.rootfolder.set_cleanup_configuration(cleanup_configuration)
    randomize_modified_dates_of_leaf_folders(first_rootfolder.rootfolder, first_rootfolder.folders)

    random.shuffle(first_rootfolder.folders)
    #first_rootfolder.folders

    # second RootFolders is split into two datasets for the same rootfolder with an "equal" number of the folders drawn in random order from the second rootfolder
    second_rootfolder: RootFolderWithMemoryFolders = flatten_folder_structure(rootfolders.popleft())
    random.shuffle(second_rootfolder.folders)
    mid_index = len(second_rootfolder.folders) // 2
    second_rootfolder.rootfolder.set_cleanup_configuration(cleanup_configuration)
    
    second_rootfolder_part_one = RootFolderWithMemoryFolders(rootfolder=second_rootfolder.rootfolder, folders=second_rootfolder.folders[:mid_index])
    randomize_modified_dates_of_leaf_folders(second_rootfolder.rootfolder, second_rootfolder_part_one.folders)

    second_rootfolder_part_two = RootFolderWithMemoryFolders(rootfolder=second_rootfolder.rootfolder, folders=second_rootfolder.folders[mid_index:])
    randomize_modified_dates_of_leaf_folders(second_rootfolder.rootfolder, second_rootfolder_part_two.folders)

    return {
        "rootfolder_tuples": rootfolders,
        "first_rootfolder": first_rootfolder,
        "second_rootfolder_part_one": second_rootfolder_part_one,
        "second_rootfolder_part_two": second_rootfolder_part_two
    }

# Pytest markers for test organization
def pytest_configure(config):
    """Register custom pytest markers"""
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
    config.addinivalue_line("markers", "integration: marks tests as integration tests") 
    config.addinivalue_line("markers", "cleanup_workflow: marks tests as cleanup workflow scenarios")
    config.addinivalue_line("markers", "slow: marks tests as slow running")


