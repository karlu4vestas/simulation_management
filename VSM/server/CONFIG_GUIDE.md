# Test Mode Configuration Guide

This guide explains how to use the test mode configuration system in the simulation management server.

## Overview

The system provides three test modes:
- `unit_test` (default) - For unit testing
- `client_test` - For client-side testing 
- `production` - For production environment

## Configuration Files

### `/app/config.py`
Contains the `AppConfig` class and global configuration functions.

### Key Components:
- `TestMode` enum with three modes
- `AppConfig` singleton class for global state management
- Global functions for easy access: `get_test_mode()`, `set_test_mode()`, `is_unit_test()`, etc.

## Usage Examples

### 1. Setting Test Mode in main.py

```python
from app.config import set_test_mode, TestMode

# Set to client test mode
set_test_mode(TestMode.CLIENT_TEST)

# Or set to production
set_test_mode(TestMode.PRODUCTION)

# You can also use strings
set_test_mode("production")
```

### 2. Checking Current Mode

```python
from app.config import get_test_mode, is_production, is_client_test, is_unit_test

# Get current mode
current_mode = get_test_mode()  # Returns TestMode enum
print(f"Current mode: {current_mode.value}")

# Check specific modes
if is_production():
    print("Running in production mode")
elif is_client_test():
    print("Running in client test mode")
elif is_unit_test():
    print("Running in unit test mode")
```

### 3. Using in API Endpoints

The test mode is automatically included in API responses:

**GET /** - Root endpoint includes current test mode:
```json
{
  "message": "Welcome to your todo list.",
  "test_mode": "client_test"
}
```

**GET /config/test-mode** - Detailed configuration information:
```json
{
  "test_mode": "client_test",
  "is_unit_test": false,
  "is_client_test": true,
  "is_production": false
}
```

### 4. Conditional Logic Based on Test Mode

```python
from app.config import is_production, is_unit_test

# Example: Different database behavior based on mode
def get_database_url():
    if is_production():
        return "postgresql://prod-server/db"
    elif is_unit_test():
        return "sqlite:///:memory:"
    else:  # client_test
        return "sqlite:///test.db"

# Example: Different logging levels
def get_log_level():
    if is_production():
        return "WARNING"
    else:
        return "DEBUG"
```

## Quick Start

1. **For main.py**: Set the desired mode at the beginning:
   ```python
   set_test_mode(TestMode.CLIENT_TEST)  # or PRODUCTION
   ```

2. **For tests**: The mode defaults to `unit_test`, but you can change it:
   ```python
   set_test_mode(TestMode.UNIT_TEST)  # Reset to default if needed
   ```

3. **Check mode anywhere**: Use the convenience functions:
   ```python
   if is_production():
       # Production-specific code
   ```

## API Testing

You can test the configuration via HTTP endpoints:

```bash
# Check current configuration
curl http://localhost:5173/config/test-mode

# Check root endpoint (includes test mode)
curl http://localhost:5173/
```

## Demo Scripts

- `demo_config.py` - Demonstrates the configuration system
- Run with: `python demo_config.py`
