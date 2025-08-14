import pytest
from app.config import AppConfig, TestMode, set_test_mode, get_test_mode, is_unit_test, is_client_test, is_production


class TestAppConfig:
    """Test the application configuration system."""

    def test_default_test_mode_is_unit_test(self):
        """Test that the default test mode is unit_test."""
        config = AppConfig()
        assert config.test_mode == TestMode.UNIT_TEST
        assert config.is_unit_test() is True
        assert config.is_client_test() is False
        assert config.is_production() is False

    def test_set_test_mode_with_enum(self):
        """Test setting test mode using TestMode enum."""
        config = AppConfig()
        
        config.set_test_mode(TestMode.CLIENT_TEST)
        assert config.test_mode == TestMode.CLIENT_TEST
        assert config.is_client_test() is True
        assert config.is_unit_test() is False
        assert config.is_production() is False

    def test_set_test_mode_with_string(self):
        """Test setting test mode using string values."""
        config = AppConfig()
        
        config.set_test_mode("production")
        assert config.test_mode == TestMode.PRODUCTION
        assert config.is_production() is True
        assert config.is_unit_test() is False
        assert config.is_client_test() is False

    def test_invalid_test_mode_raises_error(self):
        """Test that invalid test mode raises ValueError."""
        config = AppConfig()
        
        with pytest.raises(ValueError, match="Invalid test mode"):
            config.set_test_mode("invalid_mode")

    def test_global_configuration_functions(self):
        """Test the global configuration functions."""
        # Set to client_test
        set_test_mode(TestMode.CLIENT_TEST)
        
        assert get_test_mode() == TestMode.CLIENT_TEST
        assert is_client_test() is True
        assert is_unit_test() is False
        assert is_production() is False
        
        # Set to production
        set_test_mode("production")
        
        assert get_test_mode() == TestMode.PRODUCTION
        assert is_production() is True
        assert is_client_test() is False
        assert is_unit_test() is False

    def test_test_mode_property_setter(self):
        """Test the test_mode property setter."""
        config = AppConfig()
        
        # Test with enum
        config.test_mode = TestMode.PRODUCTION
        assert config.test_mode == TestMode.PRODUCTION
        
        # Test with string
        config.test_mode = "client_test"
        assert config.test_mode == TestMode.CLIENT_TEST

    def teardown_method(self):
        """Reset test mode to default after each test."""
        set_test_mode(TestMode.UNIT_TEST)
