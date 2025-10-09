import random
import pytest
import os
from sqlmodel import SQLModel, create_engine, Session
from app.app_config import AppConfig
from db.database import Database
from datamodel.dtos import RootFolderDTO


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
        "integration_test.sqlite"
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
    # Create a persistent test database session for integration tests
    # Keep session open for the entire integration test
    # delete the database when the integration test is done 
    db:Database = Database.get_db()
    db.create_db_and_tables()
    session:Session = Session(db.get_engine())
    try:
        yield session
    finally:
        session.close()
        db.delete_db()

@pytest.fixture(scope="function")
def database_with_tables(clean_database):
    """Create a database instance with tables created but no data"""
   
    db:Database = Database.get_db()
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
    from datamodel.dtos import RootFolderDTO, SimulationDomainDTO, FolderTypeDTO
    
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

from .integration.testdata_for_import import InMemoryFolderNode, flatten_folder_structure, flatten_multiple_folder_structures, generate_in_memory_rootfolder_and_folder_hierarchies
@pytest.fixture(scope="function")
def cleanup_scenario_data():
    """Sample data for cleanup workflow testing"""
    number_of_rootfolders=2
    rootfolder_tuples: list[tuple[RootFolderDTO, InMemoryFolderNode]] = generate_in_memory_rootfolder_and_folder_hierarchies(number_of_rootfolders)
    assert len(rootfolder_tuples) > 0


    #Split the simulations in three parts:
    # first part with same rootfolder and all its folders
    # second and third part with random split of the remaining folders and rootfolders. part 2 and 3 have the same size +-1

    # part one: reserver the first tuple of (rootfolder, InMemoryFolderNode) and flatten it for later updates
    first_rootfolder_tuple: tuple[RootFolderDTO, InMemoryFolderNode] = rootfolder_tuples[0]
    del rootfolder_tuples[0]

    # part two and three: make a random split of folders by 
    # step: create a list of tuple[RootFolderDTO, InMemoryFolderNode] from all folders by iterating top down or breath first through the folder trees
    items: list[tuple[RootFolderDTO, InMemoryFolderNode]] = flatten_multiple_folder_structures(rootfolder_tuples)
    assert len(items) > 0           
    # step: shuffling the list
    random.shuffle(items)
    # step: splitting that random list in half
    mid_index = len(items) // 2
    second_random_rootfolder_tuples: list[tuple[RootFolderDTO, InMemoryFolderNode]] = items[:mid_index]
    third_random_rootfolder_tuples:  list[tuple[RootFolderDTO, InMemoryFolderNode]] = items[mid_index:]

    return {
        "rootfolder_tuples": rootfolder_tuples,
        "first_rootfolder_tuple": first_rootfolder_tuple,
        "second_random_rootfolder_tuples": second_random_rootfolder_tuples,
        "third_random_rootfolder_tuples": third_random_rootfolder_tuples
    }

# Pytest markers for test organization
def pytest_configure(config):
    """Register custom pytest markers"""
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
    config.addinivalue_line("markers", "integration: marks tests as integration tests") 
    config.addinivalue_line("markers", "cleanup_workflow: marks tests as cleanup workflow scenarios")
    config.addinivalue_line("markers", "slow: marks tests as slow running")


