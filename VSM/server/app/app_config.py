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
    
    @staticmethod
    def configure_clock() -> None:
        """
        Configure SystemClock based on environment variables or test mode.
        Should be called early in application startup.
        
        Environment variables:
            APP_TIME_FIXED: ISO format datetime (e.g., "2025-12-31T12:00:00")
            APP_TIME_OFFSET_SECONDS: Integer seconds offset from real time
            APP_TIME_OFFSET_DAYS: Integer days offset from real time
        """
        import os
        from datetime import datetime
        from app.clock import SystemClock
        
        # Check environment variables first
        fixed_time = os.getenv("APP_TIME_FIXED")
        offset_seconds = os.getenv("APP_TIME_OFFSET_SECONDS")
        offset_days = os.getenv("APP_TIME_OFFSET_DAYS")
        
        if fixed_time:
            try:
                dt = datetime.fromisoformat(fixed_time)
                SystemClock.set_fixed(dt)
                print(f"SystemClock: Fixed time set to {dt}")
                return
            except ValueError as e:
                print(f"Warning: Invalid APP_TIME_FIXED format '{fixed_time}': {e}")
        
        if offset_days:
            try:
                days = int(offset_days)
                SystemClock.set_offset_days(days)
                print(f"SystemClock: Offset set to {days} days")
                return
            except ValueError as e:
                print(f"Warning: Invalid APP_TIME_OFFSET_DAYS value '{offset_days}': {e}")
        
        if offset_seconds:
            try:
                seconds = int(offset_seconds)
                SystemClock.set_offset_seconds(seconds)
                print(f"SystemClock: Offset set to {seconds} seconds")
                return
            except ValueError as e:
                print(f"Warning: Invalid APP_TIME_OFFSET_SECONDS value '{offset_seconds}': {e}")
        
        # Default: real time
        SystemClock.set_real()
        print("SystemClock: Using real system time")
