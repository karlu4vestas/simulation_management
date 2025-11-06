# Quick Reference: InternalAgentFactory with Context Manager

## ✅ What Was Changed

**File**: `/workspaces/simulation_management/VSM/server/cleanup_cycle/internal_agents.py`

1. Added `from contextlib import contextmanager` import
2. Redesigned `InternalAgentFactory` class with dependency injection support
3. Added `with_agents()` context manager method

## 🚀 How to Use in Your Test

### Step 1: Import What You Need
```python
from cleanup_cycle.internal_agents import (
    InternalAgentFactory,
    AgentCalendarCreation,
    AgentCleanupCycleStart,
    AgentNotification,
    AgentCleanupCycleFinishing,
    AgentCleanupCyclePrepareNext
)
from cleanup_cycle.on_premise_scan_agent import AgentScanVTSRootFolder
from cleanup_cycle.on_premise_clean_agent import AgentCleanVTSRootFolder
```

### Step 2: Set Your Environment Variables
```python
os.environ['SCAN_TEMP_FOLDER'] = os.path.join(io_dir_for_storage_test, "temp_for_scanning")
os.environ['SCAN_THREADS'] = str(1)
os.environ['CLEAN_TEMP_FOLDER'] = os.path.join(io_dir_for_storage_test, "temp_for_cleaning")
os.environ['CLEAN_SIM_WORKERS'] = str(1)
os.environ['CLEAN_DELETION_WORKERS'] = str(2)
os.environ['CLEAN_MODE'] = 'ANALYSE'
```

### Step 3: Create Test Agents
```python
test_agents = [
    AgentCalendarCreation(),
    AgentScanVTSRootFolder(),       # Will use test env vars
    AgentCleanupCycleStart(),
    AgentNotification(),
    AgentCleanVTSRootFolder(),      # Will use test env vars
    AgentCleanupCycleFinishing(),
    AgentCleanupCyclePrepareNext(),
]
```

### Step 4: Use Context Manager
```python
with InternalAgentFactory.with_agents(test_agents):
    run_scheduler_tasks()
    # Your assertions here
```

## 📖 Context Manager Reference

The `@contextmanager` decorator is already used in your codebase:

**See**: `/workspaces/simulation_management/VSM/server/app/web_api.py:1`
```python
from contextlib import asynccontextmanager
```

**Pattern**:
```python
@contextmanager
def my_context():
    # Setup
    old_state = save_state()
    try:
        yield  # Your code runs here
    finally:
        restore_state(old_state)  # Always runs, even on error
```

## 💡 Why This Works

1. **Agent Creation**: When you create `AgentScanVTSRootFolder()`, it reads environment variables in `__init__()`
2. **Test Environment**: Your test sets environment variables BEFORE creating agents
3. **Agent Registration**: Context manager temporarily registers your test agents
4. **Scheduler Execution**: `run_scheduler_tasks()` uses your test agents
5. **Automatic Cleanup**: When exiting the `with` block, original agents are restored

## 📋 Complete Test Pattern

```python
def test_scheduler_and_agents_with_full_cleanup_round(self, integration_session, cleanup_scenario_data):
    # ... all your existing setup code ...
    
    # Set test environment
    os.environ['SCAN_TEMP_FOLDER'] = test_scan_path
    os.environ['CLEAN_MODE'] = 'ANALYSE'
    # ... other env vars ...
    
    # Create test agents (they read the env vars above)
    test_agents = [
        AgentCalendarCreation(),
        AgentScanVTSRootFolder(),
        AgentCleanupCycleStart(),
        AgentNotification(),
        AgentCleanVTSRootFolder(),
        AgentCleanupCycleFinishing(),
        AgentCleanupCyclePrepareNext(),
    ]
    
    # Run test with injected agents
    with InternalAgentFactory.with_agents(test_agents):
        # Step 3: Run scheduler to create scan tasks
        run_scheduler_tasks()
        # Step 4: Verify scan tasks completed
        # ... assertions ...
        
        # Step 6: Run scheduler for cleanup calendar
        run_scheduler_tasks()
        # Step 7: Run scheduler to execute cleanup
        run_scheduler_tasks()
        # Step 8-9: Verify cleanup completed
        # ... assertions ...
    
    # Done! Original agents automatically restored
```

## 📚 Documentation Files Created

1. `CONTEXT_MANAGER_REFERENCE.md` - Deep dive into @contextmanager
2. `USAGE_EXAMPLE.md` - Various usage examples
3. `HOW_TO_UPDATE_TEST.md` - Step-by-step test update guide
4. `REDESIGN_SUMMARY.md` - Complete solution overview
5. `QUICK_REFERENCE.md` - This file

## ✨ Key Points

- ✅ Production code: No changes needed
- ✅ Test code: Use context manager
- ✅ Automatic cleanup: Guaranteed by `finally` block
- ✅ Exception-safe: Cleanup happens even if test fails
- ✅ Backward compatible: Existing code works as-is
