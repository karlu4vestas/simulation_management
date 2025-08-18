# Configuration module for the simulation management server.
# Manages global settings including test mode configuration.

from enum import Enum
from typing import Literal


class AppTestMode(str, Enum):
    # Enumeration of available test modes for the application.
    UNIT_TEST = "unit_test"
    CLIENT_TEST = "client_test"
    PRODUCTION = "production"


class AppConfig:
    # Application configuration class that manages global settings.
    
    _instance = None
    #_test_mode: TestMode = TestMode.UNIT_TEST
    _test_mode: AppTestMode = AppTestMode.CLIENT_TEST
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppConfig, cls).__new__(cls)
        return cls._instance
    
    @classmethod
    def Instance(cls):
        # Get the singleton instance of AppConfig.
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @staticmethod
    def set_test_mode(mode: AppTestMode) -> None:
        # Alternative method to set test mode.
        AppConfig.Instance()._test_mode = mode

    @staticmethod
    def get_test_mode() -> AppTestMode:
        # Check if current mode is unit_test.
        return AppConfig.Instance()._test_mode

    @staticmethod
    def is_unit_test() -> bool:
        # Check if current mode is unit_test.
        return AppConfig.Instance()._test_mode == AppTestMode.UNIT_TEST
    
    @staticmethod
    def is_client_test() -> bool:
        # Check if current mode is client_test.
        return AppConfig.Instance()._test_mode == AppTestMode.CLIENT_TEST

    @staticmethod
    def is_production() -> bool:
        # Check if current mode is production.
        return AppConfig.Instance()._test_mode == AppTestMode.PRODUCTION
    
    @staticmethod
    def get_db_url() -> str:
        # Get the database URL based on the current test mode.
        inst = AppConfig.Instance()
        if inst._test_mode == AppTestMode.UNIT_TEST:
            return "sqlite:///unit_test_db.sqlite"  # Separate unit test database file
        elif inst._test_mode == AppTestMode.CLIENT_TEST:
            return "sqlite:///test_db.sqlite"  # Separate test database file
        else:  # PRODUCTION
            return "sqlite:///db.sqlite"  # Production database file