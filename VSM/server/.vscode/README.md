# VS Code Testing Configuration

This directory contains VS Code configuration files for running tests in the simulation management server project.

## Files Created

### `.vscode/launch.json`
Debug configurations for running tests:

- **Run All Tests** - Runs all tests with coverage reporting
- **Run Unit Tests Only** - Runs only unit tests
- **Run Integration Tests Only** - Runs only integration tests  
- **Run Database Tests** - Runs database-specific tests
- **Run DTO Tests** - Runs DTO model tests
- **Run Database Operations Tests** - Runs database operation tests
- **Run Tests with Debug** - Runs tests with detailed debugging
- **Run Database Demo** - Runs the database demo script

### `.vscode/settings.json`
VS Code Python settings:
- Configures virtual environment path
- Enables pytest test discovery
- Sets up linting and formatting
- Excludes cache directories

### `.vscode/tasks.json`
VS Code tasks for test execution:
- Run All Tests (default test task)
- Run Unit Tests
- Run Integration Tests
- Run Database Tests
- Run Tests with Watch (continuous testing)
- Install Test Dependencies

## How to Use

### Using Debug Configurations (F5)
1. Open the Run and Debug panel (Ctrl+Shift+D)
2. Select the desired test configuration from the dropdown
3. Click the play button or press F5

### Using Tasks (Ctrl+Shift+P)
1. Open Command Palette (Ctrl+Shift+P)
2. Type "Tasks: Run Task"
3. Select the desired test task

### Using VS Code Test Explorer
1. Open the Test Explorer panel
2. Tests should auto-discover due to settings.json configuration
3. Click individual tests or test files to run them

### Keyboard Shortcuts
- **Ctrl+Shift+P** → "Tasks: Run Task" → "Run All Tests"
- **F5** with "Run All Tests" selected in debug panel

## Test Output

- **Terminal Output**: Test results appear in the integrated terminal
- **Coverage Report**: HTML coverage report generated in `htmlcov/` directory
- **Test Discovery**: Tests automatically discovered in VS Code Test Explorer

## Prerequisites

Ensure you have:
1. Virtual environment activated (`.venv/`)
2. Dependencies installed (`pip install -r requirements.txt`)
3. Python extension for VS Code installed
4. Pytest extension for VS Code (optional but recommended)

## Test Structure

```
tests/
├── unit/                    # Unit tests
│   ├── test_database.py    # Database singleton tests
│   └── test_dtos.py        # DTO model tests
├── integration/            # Integration tests
│   └── test_database_operations.py
└── api/                    # API tests (future)
    └── test_endpoints.py
```
