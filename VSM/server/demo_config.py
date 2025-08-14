#!/usr/bin/env python3
"""
Demo script showing how to use the test mode configuration system.
"""

from app.config import (
    AppConfig, TestMode, 
    set_test_mode, get_test_mode,
    is_unit_test, is_client_test, is_production
)


def demonstrate_config_system():
    """Demonstrate the configuration system functionality."""
    
    print("=== Test Mode Configuration System Demo ===\n")
    
    # Show default mode
    print("1. Default configuration:")
    print(f"   Current test mode: {get_test_mode().value}")
    print(f"   Is unit test: {is_unit_test()}")
    print(f"   Is client test: {is_client_test()}")
    print(f"   Is production: {is_production()}")
    
    # Change to client_test mode
    print("\n2. Setting mode to 'client_test':")
    set_test_mode(TestMode.CLIENT_TEST)
    print(f"   Current test mode: {get_test_mode().value}")
    print(f"   Is unit test: {is_unit_test()}")
    print(f"   Is client test: {is_client_test()}")
    print(f"   Is production: {is_production()}")
    
    # Change to production mode using string
    print("\n3. Setting mode to 'production' (using string):")
    set_test_mode("production")
    print(f"   Current test mode: {get_test_mode().value}")
    print(f"   Is unit test: {is_unit_test()}")
    print(f"   Is client test: {is_client_test()}")
    print(f"   Is production: {is_production()}")
    
    # Show individual config object
    print("\n4. Using individual AppConfig instance:")
    config = AppConfig()  # This will have the global state
    print(f"   Config test mode: {config.test_mode.value}")
    print(f"   Config is production: {config.is_production()}")
    
    # Show available modes
    print("\n5. Available test modes:")
    for mode in TestMode:
        print(f"   - {mode.value}")
    
    # Reset to default
    print("\n6. Resetting to default (unit_test):")
    set_test_mode(TestMode.UNIT_TEST)
    print(f"   Current test mode: {get_test_mode().value}")
    
    print("\n=== Demo Complete ===")


if __name__ == "__main__":
    demonstrate_config_system()
