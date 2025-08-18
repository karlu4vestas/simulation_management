import pytest
from fastapi.testclient import TestClient
from app.web_api import app
from app.config import AppConfig, AppTestMode


class TestWebAPIConfig:
    """Test the web API configuration endpoints."""

    def setup_method(self):
        """Set up test client and reset config."""
        self.client = TestClient(app)
        set_test_mode(TestMode.UNIT_TEST)

    def test_root_endpoint_includes_test_mode(self):
        """Test that the root endpoint includes test mode information."""
        response = self.client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "test_mode" in data
        assert data["test_mode"] == "unit_test"

    def test_config_test_mode_endpoint(self):
        """Test the test mode configuration endpoint."""
        response = self.client.get("/config/test-mode")
        
        assert response.status_code == 200
        data = response.json()
        
        expected_keys = ["test_mode", "is_unit_test", "is_client_test", "is_production"]
        for key in expected_keys:
            assert key in data
        
        assert data["test_mode"] == "unit_test"
        assert data["is_unit_test"] is True
        assert data["is_client_test"] is False
        assert data["is_production"] is False

    def test_config_endpoint_reflects_mode_changes(self):
        #Test that the config endpoint reflects test mode changes.
        # Change to client_test mode
        AppConfig.set_test_mode(AppTestMode.CLIENT_TEST)

        response = self.client.get("/config/test-mode")
        assert response.status_code == 200
        data = response.json()
        
        assert data["test_mode"] == "client_test"
        assert data["is_unit_test"] is False
        assert data["is_client_test"] is True
        assert data["is_production"] is False

    def teardown_method(self):
        #Reset test mode to default after each test-
        AppConfig.set_test_mode(AppTestMode.UNIT_TEST)
