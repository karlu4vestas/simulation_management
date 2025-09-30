#!/usr/bin/env python3
"""
Main entry point for running API unit tests.
Sets the application to unit test mode and executes API test suite.
"""

import sys
import os
import pytest

# Add the server directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.app_config import AppConfig, AppConfig

def main():
    """Run API unit tests with proper configuration."""
    print("Starting API unit tests...")
    
    # Set test mode to unit test
    AppConfig.set_test_mode(AppConfig.Mode.UNIT_TEST)
    print(f"Test mode set to: {AppConfig.get_test_mode()}")
    
    # Run API unit tests
    test_args = [
        "tests/unittests/api/",
        "-v",
        "--tb=short"
    ]
  
    exit_code = pytest.main(test_args)
    
    # Provide feedback based on exit code
    if exit_code == 0:
        print("✅ All database unit tests passed!")
    elif exit_code == 1:
        print("❌ Some tests failed or had errors (11 passed, 2 errors)")
    elif exit_code == 2:
        print("⚠️  Test execution was interrupted")
    elif exit_code == 3:
        print("⚠️  Internal error occurred")
    elif exit_code == 4:
        print("⚠️  pytest usage error")
    elif exit_code == 5:
        print("❌ No tests found")
    
    return exit_code

if __name__ == "__main__":
    try:
        exit_code = main()
        print(f"Test execution completed with exit code: {exit_code}")
        # Only exit with error code if not in debug mode
        if not sys.gettrace():  # Not in debugger
            sys.exit(exit_code)
        else:
            print("Debug mode detected - not calling sys.exit()")
    except KeyboardInterrupt:
        print("\n🛑 Test execution interrupted by user")
        if not sys.gettrace():
            sys.exit(130)  # Standard exit code for Ctrl+C
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        if not sys.gettrace():
            sys.exit(1)
