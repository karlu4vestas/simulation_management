import pytest
from sqlmodel import Session, select
from fastapi.testclient import TestClient
from app.web_api import app
from server.db.database import Database
from server.app.app_config import AppConfig, AppTestMode
from datamodel.vts_create_meta_data import insert_vts_metadata_in_db
from server.testdata.vts_generate_test_data import insert_test_data_in_db


@pytest.fixture
def client():
    AppConfig.set_test_mode(AppTestMode.UNIT_TEST)
    # test client does not call the startup where the db is setup so do it here
    db = Database.get_db()
    if db.is_empty() and AppConfig.is_unit_test():
            db.clear_all_tables_and_schemas()
            db.create_db_and_tables()
            engine = db.get_engine()
            insert_vts_metadata_in_db(engine)
            insert_test_data_in_db(engine) 

    return TestClient(app)

class TestRootEndpoint:
    #Test cases for the root endpoint

    def test_read_root_endpoint(self, client):
        #Test GET / endpoint returns welcome message and test mode
        response = client.get("/")
        
        # Verify response status
        assert response.status_code == 200
        
        #Test that root endpoint returns JSON content type
        assert response.headers["content-type"] == "application/json"

        # Verify response content
        data = response.json()
        expected_keys = {"message", "test_mode"}
        assert set(data.keys()) == expected_keys
        assert data["message"] == "Welcome to your todo list."
        assert data["test_mode"] in ["unit_test", "client_test", "production"]
        
        # Verify response structure
        assert "message" in data
        assert "test_mode" in data
        assert isinstance(data["message"], str)
        assert isinstance(data["test_mode"], str)
        
class TestConfigEndpoint:
    # Test cases for the config endpoint

    def test_get_test_mode_endpoint(self, client):
        # Test GET /config/test-mode endpoint
        response = client.get("/config/test-mode")
        
        # Verify response status
        assert response.status_code == 200
        
        # Verify response content
        data = response.json()
        expected_keys = {"test_mode", "is_unit_test", "is_client_test", "is_production"}
        assert set(data.keys()) == expected_keys
        
        # Verify data types
        assert isinstance(data["is_unit_test"], bool)
        assert isinstance(data["is_client_test"], bool)
        assert isinstance(data["is_production"], bool)
        
        # Verify valid test_mode values
        assert data["test_mode"] in ["unit_test", "client_test", "production"]
        
        # Verify content type
        assert response.headers["content-type"] == "application/json"

class TestRetentionAPI:
    # Test API endpoints for Retention operations

    def test_get_retention(self, client):
        # Test GET /retentiontypes/ endpoint returns proper structure
        response = client.get("/retentiontypes/")
            
        # Verify the response
        assert response.status_code == 200
            
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0  # Should have test data
            
        # Verify content type
        assert response.headers["content-type"] == "application/json"
            
        # Test the structure of the first item
        first_item = data[0]
        expected_keys = {"name", "display_rank", "is_system_managed", "id"}
        assert set(first_item.keys()) == expected_keys
        assert isinstance(first_item["name"], str)
        assert isinstance(first_item["display_rank"], int)
        assert isinstance(first_item["is_system_managed"], bool)
        assert isinstance(first_item["id"], int)

    def test_get_rootfolder_retentions(self, client):
        # Test GET /rootfolders/{id}/retentiontypes endpoint
        # First get a root folder
        root_folders_response = client.get("/rootfolders/?simulationdomain_id=1&initials=jajac")
        assert root_folders_response.status_code == 200
        
        root_folders = root_folders_response.json()
        if len(root_folders) > 0:
            rootfolder_id = root_folders[0]["id"]
            
            # Test both with and without trailing slash
            response1 = client.get(f"/rootfolders/{rootfolder_id}/retentiontypes")
            assert response1.status_code == 200
            
            response2 = client.get(f"/rootfolders/{rootfolder_id}/retentiontypes/")
            assert response2.status_code == 200
            
            # Both should return the same data
            data1 = response1.json()
            data2 = response2.json()
            assert data1 == data2
            
            # Verify structure
            assert isinstance(data1, list)
            if len(data1) > 0:
                first_item = data1[0]
                expected_keys = {"name", "display_rank", "is_system_managed", "id"}
                assert set(first_item.keys()) == expected_keys

class TestRootFolderAPI:
    # Test API endpoints for RootFolder operations

    def test_get_root_folders(self):
        # Test GET /root-folders endpoint
        client = TestClient(app)
        response = client.get("/rootfolders")
        assert response.status_code == 200

        #Test that root endpoint returns JSON content type
        assert response.headers["content-type"] == "application/json"


    def test_query_root_folders(self):
        # Test GET /root-folders endpoint with initials query
        client = TestClient(app)
        initials = "jajac"
        response = client.get(f"/rootfolders/?simulationdomain_id=1&initials={initials}")
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"
        root_folders = response.json()
        if len(root_folders) > 0:
            assert root_folders[0]["owner"] == initials or initials in root_folders[0]["approvers"]

class TestFolderNodeAPI:
    # Test API endpoints for FolderNode operations

    def test_get_folder_nodes(self):
        # Test GET /folder-nodes endpoint
        client = TestClient(app)
        response = client.get("/folders/?rootfolder_id=1")  
        assert response.status_code == 200

        #Test that root endpoint returns JSON content type
        assert response.headers["content-type"] == "application/json"

class TestRootfolder_vs_FoldersNodeAPI:
    # Test API endpoints for FolderNode operations
    
    def test_query_folder_nodes_by_rootfolder(self, client):
        # First retrieve root folders
        root_folders_response = client.get("/rootfolders")
        assert root_folders_response.status_code == 200
        assert root_folders_response.headers["content-type"] == "application/json"
        
        root_folders = root_folders_response.json()
        assert isinstance(root_folders, list)
        assert len(root_folders) > 0  # Should have test data
        
        # For each root folder, verify that folder nodes are returned
        for root_folder in root_folders:
            assert "id" in root_folder
            root_folder_id = root_folder["id"]
            
            # Get folder nodes for this root folder
            response = client.get(f"/folders/?rootfolder_id={root_folder_id}")  
            assert response.status_code == 200
            assert response.headers["content-type"] == "application/json"
            
            folder_nodes = response.json()
            if len(folder_nodes) > 0:  # Each root folder should have at least one folder node
                assert not root_folder["folder_id"] == "0" #DB index are >0
                assert isinstance(folder_nodes, list)

 