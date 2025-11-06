# InternalAgentFactory Redesign - Summary

## Problem Statement
The original `InternalAgentFactory` had hardcoded agent registration with no way to inject test-specific agents. This made it impossible to:
- Use test-specific environment variables in agents
- Mock agents for testing
- Test individual agents in isolation
- Control agent execution in integration tests

## Solution Implemented

### 1. Context Manager Pattern with Dependency Injection

The `InternalAgentFactory` class has been redesigned to support:
- **Default behavior**: Production code unchanged - agents auto-register
- **Test injection**: Context manager for temporary agent registration
- **Automatic cleanup**: Guaranteed restoration of default agents

### 2. Key Changes Made

#### File: `/workspaces/simulation_management/VSM/server/cleanup_cycle/internal_agents.py`

**Added Import:**
```python
from contextlib import contextmanager
```

**New Class Structure:**
```python
class InternalAgentFactory:
    # Private class-level state for dependency injection
    _agent_registry: list[AgentTemplate] | None = None
    _default_agents_factory: callable | None = None
    
    @staticmethod
    def _create_default_agents() -> list[AgentTemplate]:
        """Creates default production agents"""
        
    @staticmethod
    def get_internal_agents() -> list[AgentTemplate]:
        """Returns registered agents or defaults"""
        
    @staticmethod
    def register_agents(agents: list[AgentTemplate]) -> None:
        """Register custom agents for testing"""
        
    @staticmethod
    def reset_to_defaults() -> None:
        """Reset to default configuration"""
        
    @staticmethod
    @contextmanager
    def with_agents(agents: list[AgentTemplate]):
        """Context manager for temporary agent registration (RECOMMENDED)"""
        
    @staticmethod
    def run_internal_agents(agents=None, run_randomized=False):
        """Run agents with optional override"""
```

## Usage

### Production Code (Unchanged)
```python
# No changes needed
InternalAgentFactory.run_internal_agents()
```

### Test Code (New Pattern)
```python
# Create test-specific agents
test_agents = [
    AgentCalendarCreation(),
    AgentScanVTSRootFolder(),  # Picks up test env vars
    # ... other agents
]

# Use context manager
with InternalAgentFactory.with_agents(test_agents):
    run_scheduler_tasks()  # Uses test agents
    # ... assertions ...
# Original agents automatically restored
```

## Benefits

1. **Backward Compatible**: Existing production code works without changes
2. **Testable**: Easy to inject test-specific agent configurations
3. **Clean**: Context manager ensures proper cleanup
4. **Flexible**: Supports mocking, partial agent lists, and custom implementations
5. **Safe**: Exception-safe - cleanup happens even if test fails
6. **Clear Intent**: Explicit scope for test agent registration

## Reference to @contextmanager

The `@contextmanager` decorator is from Python's `contextlib` module and is already used in the codebase:

**Location**: `/workspaces/simulation_management/VSM/server/app/web_api.py`
```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    db = Database.get_db()
    # ...
    yield
    # Shutdown
```

**Our Implementation**:
```python
from contextlib import contextmanager

@contextmanager
def with_agents(agents: list[AgentTemplate]):
    old_state = save_current_state()
    try:
        register_new_agents(agents)
        yield  # Control returns to with block
    finally:
        restore_old_state()  # Always runs
```

## Documentation Created

1. **CONTEXT_MANAGER_REFERENCE.md** - Comprehensive guide to @contextmanager
2. **USAGE_EXAMPLE.md** - Examples of using the new factory
3. **HOW_TO_UPDATE_TEST.md** - Step-by-step guide for updating tests

## Next Steps

To use this in your test `test_scheduler_and_agents_with_full_cleanup_round`:

1. Import the factory and agents
2. Create a list of test agents (they'll pick up your test environment variables)
3. Wrap `run_scheduler_tasks()` calls in the context manager
4. Add your assertions within the context manager block

The context manager ensures that:
- Test agents are used during the test
- Original agents are restored after the test
- Cleanup happens even if test fails

## Example Test Pattern

```python
def test_scheduler_and_agents_with_full_cleanup_round(self, integration_session, cleanup_scenario_data):
    # Setup test data, environment variables, etc.
    os.environ['SCAN_TEMP_FOLDER'] = test_path
    os.environ['CLEAN_MODE'] = 'ANALYSE'
    
    # Create test agents
    test_agents = [
        AgentCalendarCreation(),
        AgentScanVTSRootFolder(),
        AgentCleanVTSRootFolder(),
        # ... all needed agents
    ]
    
    # Run test with injected agents
    with InternalAgentFactory.with_agents(test_agents):
        run_scheduler_tasks()
        # assertions...
```
