import pytest
from sqlmodel import Session
from fastapi.testclient import TestClient
from app.web_api import app
from app.app_config import AppConfig
from db.database import Database
from datamodel.vts_create_meta_data import insert_vts_metadata_in_db
from testdata.vts_generate_test_data import insert_minimal_test_data_for_unit_tests 

@pytest.fixture
def client():
    AppConfig.set_test_mode(AppConfig.Mode.UNIT_TEST)
    
    # Reset the Database singleton to ensure fresh engine
    Database._instance = None
    Database._engine = None
    
    # Create a fresh database with test data
    db = Database.get_db()
    db.delete_db()
    db.create_db_and_tables()
    
    session = Session(db.get_engine())
    try:
        insert_vts_metadata_in_db(session)
        # Use minimal test data for unit tests instead of the massive hierarchy
        insert_minimal_test_data_for_unit_tests(session)
        session.close()
        
        # Yield the test client
        yield TestClient(app)
    finally:
        # Cleanup after test
        db.delete_db()
        Database._instance = None
        Database._engine = None

class TestRootEndpoint:
    #Test cases for the root endpoint

    def test_read_root_endpoint(self, client):
        #Test GET / endpoint returns test mode configuration
        response = client.get("/")
        
        # Verify response status
        assert response.status_code == 200
        
        #Test that root endpoint returns JSON content type
        assert response.headers["content-type"] == "application/json"

        # Verify response content
        data = response.json()
        expected_keys = {"is_unit_test", "is_client_test", "is_production"}
        assert set(data.keys()) == expected_keys
        
        # Verify response structure and data types
        assert isinstance(data["is_unit_test"], bool)
        assert isinstance(data["is_client_test"], bool)
        assert isinstance(data["is_production"], bool)
        

class TestRetentionAPI:
    # Test API endpoints for Retention operations

    def test_get_retention(self, client):
        # Test GET /v1/simulationdomains/{simulationdomain_id}/retentiontypes/ endpoint
        response = client.get("/v1/simulationdomains/1/retentiontypes/")
            
        # Verify the response
        assert response.status_code == 200
            
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0  # Should have test data
            
        # Verify content type
        assert response.headers["content-type"] == "application/json"
            
        # Test the structure of the first item
        first_item = data[0]
        # Note: Adjusted expected keys to match RetentionTypeDTO structure
        assert "name" in first_item
        assert "id" in first_item
        assert isinstance(first_item["name"], str)
        assert isinstance(first_item["id"], int)

    def test_get_rootfolder_retentions(self, client):
        # Test GET /v1/rootfolders/{id}/retentiontypes endpoint
        # First get a root folder
        root_folders_response = client.get("/v1/rootfolders/?simulationdomain_id=1&initials=jajac")
        assert root_folders_response.status_code == 200
        
        root_folders = root_folders_response.json()
        if len(root_folders) > 0:
            rootfolder_id = root_folders[0]["id"]
            
            # Test both with and without trailing slash
            response1 = client.get(f"/v1/rootfolders/{rootfolder_id}/retentiontypes")
            assert response1.status_code == 200
            
            response2 = client.get(f"/v1/rootfolders/{rootfolder_id}/retentiontypes/")
            assert response2.status_code == 200
            
            # Both should return the same data
            data1 = response1.json()
            data2 = response2.json()
            assert data1 == data2
            
            # Verify structure
            assert isinstance(data1, list)
            if len(data1) > 0:
                first_item = data1[0]
                assert "name" in first_item
                assert "id" in first_item

class TestRootFolderAPI:
    # Test API endpoints for RootFolder operations

    def test_get_root_folders(self, client):
        # Test GET /v1/rootfolders endpoint - requires parameters
        response = client.get("/v1/rootfolders/?simulationdomain_id=1&initials=jajac")
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"

    def test_query_root_folders(self, client):
        # Test GET /v1/rootfolders endpoint with initials query
        initials = "jajac"
        response = client.get(f"/v1/rootfolders/?simulationdomain_id=1&initials={initials}")
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"
        root_folders = response.json()
        if len(root_folders) > 0:
            assert root_folders[0]["owner"] == initials or initials in root_folders[0]["approvers"]

class TestFolderNodeAPI:
    # Test API endpoints for FolderNode operations

    def test_get_folder_nodes(self, client):
        # Test GET /v1/rootfolders/{rootfolder_id}/folders/ endpoint
        response = client.get("/v1/rootfolders/1/folders/")  
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"

class TestRootfolder_vs_FoldersNodeAPI:
    # Test API endpoints for FolderNode operations
    
    def test_query_folder_nodes_by_rootfolder(self, client):
        # First retrieve root folders with required parameters
        root_folders_response = client.get("/v1/rootfolders/?simulationdomain_id=1&initials=jajac")
        assert root_folders_response.status_code == 200
        assert root_folders_response.headers["content-type"] == "application/json"
        
        root_folders = root_folders_response.json()
        assert isinstance(root_folders, list)
        assert len(root_folders) > 0  # Should have test data
        
        # For each root folder, verify that folder nodes are returned
        for root_folder in root_folders:
            assert "id" in root_folder
            root_folder_id = root_folder["id"]
            
            # Get folder nodes for this root folder using correct endpoint
            response = client.get(f"/v1/rootfolders/{root_folder_id}/folders/")  
            assert response.status_code == 200
            assert response.headers["content-type"] == "application/json"
            
            folder_nodes = response.json()
            if len(folder_nodes) > 0:
                assert isinstance(folder_nodes, list)

