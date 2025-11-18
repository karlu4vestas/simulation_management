# Configuration module for the simulation management server.
# Manages global settings including test mode configuration.
from enum import Enum
from typing import Literal

class AppConfig:
    class Mode(str, Enum):
        # Enumeration of available test modes for the application.
        UNIT_TEST = "unit_test"
        CLIENT_TEST = "client_test"
        INTEGRATION_TEST = "integration_test"
        PRODUCTION = "production"

    # Application configuration class that manages global settings.
    
    _instance = None
    _test_mode:Mode  = None  # Mode.CLIENT_TEST
    
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
    
    @classmethod
    def set_test_mode(cls, mode: Mode) -> bool:
        # Alternative method to set test mode.
        cls._test_mode = mode
        return True

    @classmethod
    def get_test_mode(cls) -> Mode:
        # Check if current mode is unit_test.
        return cls._test_mode

    @classmethod
    def is_unit_test(cls) -> bool:
        # Check if current mode is unit_test.
        return cls._test_mode == AppConfig.Mode.UNIT_TEST
    
    @classmethod
    def is_client_test(cls) -> bool:
        # Check if current mode is client_test.
        return cls._test_mode == AppConfig.Mode.CLIENT_TEST

    @classmethod
    def is_production(cls) -> bool:
        # Check if current mode is production.
        return cls._test_mode == AppConfig.Mode.PRODUCTION
    
    @staticmethod
    def get_db_url() -> str:
        # Get the database URL based on the current test mode.
        inst = AppConfig.Instance()
        print(f"get_db_url:{inst}")
        if inst._test_mode == AppConfig.Mode.UNIT_TEST:
            return "sqlite:///unit_test_db.sqlite"  # Separate unit test database file
        elif inst._test_mode == AppConfig.Mode.CLIENT_TEST:
            return "sqlite:///client_test_db.sqlite"  # Separate test database file
        elif inst._test_mode == AppConfig.Mode.INTEGRATION_TEST:
            return "sqlite:///integration_test.sqlite"  # Separate test database file
        else:  # PRODUCTION
            return "sqlite:///db.sqlite"  # Production database file
