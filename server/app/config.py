"""
Configuration module for the simulation management server.
Manages global settings including test mode configuration.
"""

from enum import Enum
from typing import Literal


class TestMode(str, Enum):
    """Enumeration of available test modes for the application."""
    UNIT_TEST = "unit_test"
    CLIENT_TEST = "client_test"
    PRODUCTION = "production"


class AppConfig:
    """Application configuration class that manages global settings."""
    
    _instance = None
    _test_mode: TestMode = TestMode.UNIT_TEST
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppConfig, cls).__new__(cls)
        return cls._instance
    
    @property
    def test_mode(self) -> TestMode:
        """Get the current test mode."""
        return self._test_mode
    
    @test_mode.setter
    def test_mode(self, mode: TestMode | str) -> None:
        """Set the test mode."""
        if isinstance(mode, str):
            # Convert string to TestMode enum
            try:
                mode = TestMode(mode)
            except ValueError:
                raise ValueError(f"Invalid test mode: {mode}. Valid modes are: {list(TestMode)}")
        AppConfig._test_mode = mode
    
    def set_test_mode(self, mode: TestMode | str) -> None:
        """Alternative method to set test mode."""
        self.test_mode = mode
    
    def is_unit_test(self) -> bool:
        """Check if current mode is unit_test."""
        return self._test_mode == TestMode.UNIT_TEST
    
    def is_client_test(self) -> bool:
        """Check if current mode is client_test."""
        return self._test_mode == TestMode.CLIENT_TEST
    
    def is_production(self) -> bool:
        """Check if current mode is production."""
        return self._test_mode == TestMode.PRODUCTION


# Global configuration instance
config = AppConfig()


def get_test_mode() -> TestMode:
    """Get the current test mode."""
    return config.test_mode


def set_test_mode(mode: TestMode | str) -> None:
    """Set the test mode globally."""
    config.test_mode = mode


def is_unit_test() -> bool:
    """Check if current mode is unit_test."""
    return config.is_unit_test()


def is_client_test() -> bool:
    """Check if current mode is client_test."""
    return config.is_client_test()


def is_production() -> bool:
    """Check if current mode is production."""
    return config.is_production()
