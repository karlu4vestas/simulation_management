# Context Manager Reference

## What is `@contextmanager`?

`@contextmanager` is a decorator from Python's `contextlib` module that allows you to create context managers using generator functions instead of classes. It's used with the `with` statement to ensure proper setup and cleanup of resources.

## Import

```python
from contextlib import contextmanager
```

## Basic Pattern

```python
from contextlib import contextmanager

@contextmanager
def my_context():
    # Setup code (runs when entering the 'with' block)
    print("Setting up")
    resource = acquire_resource()
    
    try:
        yield resource  # Provide the resource to the with block
    finally:
        # Cleanup code (runs when exiting the 'with' block, even if an exception occurs)
        print("Cleaning up")
        release_resource(resource)

# Usage
with my_context() as resource:
    # Use the resource
    do_something(resource)
```

## How It Works

1. **Setup Phase**: Code before `yield` runs when entering the `with` block
2. **Yield**: The value after `yield` is assigned to the variable after `as`
3. **Cleanup Phase**: Code after `yield` (typically in a `finally` block) runs when exiting, even if an exception occurs

## Example in Our Codebase

### In `web_api.py` - Async Context Manager

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    db = Database.get_db()
    if db.is_empty():
        db.create_db_and_tables()
    
    yield  # App runs here
    
    # Shutdown (if needed)
    # cleanup code would go here
```

### In `internal_agents.py` - Sync Context Manager

```python
from contextlib import contextmanager

@staticmethod
@contextmanager
def with_agents(agents: list[AgentTemplate]):
    """Context manager for temporarily using custom agents."""
    # Setup: Save current state
    old_registry = InternalAgentFactory._agent_registry
    old_factory = InternalAgentFactory._default_agents_factory
    
    try:
        # Setup: Register new agents
        InternalAgentFactory.register_agents(agents)
        yield  # Control returns to the with block
    finally:
        # Cleanup: Restore original state (guaranteed to run)
        InternalAgentFactory._agent_registry = old_registry
        InternalAgentFactory._default_agents_factory = old_factory
```

## Usage in Tests

```python
def test_scheduler_and_agents_with_full_cleanup_round(self, integration_session, cleanup_scenario_data):
    # Setup test-specific agents
    test_agents = [
        AgentCalendarCreation(),
        AgentScanVTSRootFolder(),
        AgentCleanupCycleStart(),
        # ... other agents
    ]
    
    # Use context manager - ensures cleanup even if test fails
    with InternalAgentFactory.with_agents(test_agents):
        # Within this block, run_scheduler_tasks() uses test_agents
        run_scheduler_tasks()
        
        # Run assertions
        assert something
    
    # After the block, original agents are automatically restored
    # This happens even if an exception was raised in the with block
```

## Benefits of Context Managers

1. **Automatic Cleanup**: Cleanup code runs even if exceptions occur
2. **Clear Scope**: Resource lifecycle is explicit and bounded
3. **Less Error-Prone**: No need to remember to call cleanup manually
4. **Cleaner Code**: No try/finally blocks needed at call sites

## Common Use Cases

- **File Operations**: 
  ```python
  with open('file.txt', 'r') as f:
      content = f.read()
  # File is automatically closed
  ```

- **Database Transactions**:
  ```python
  with Session(engine) as session:
      session.add(record)
      session.commit()
  # Session is automatically closed
  ```

- **Temporary State Changes** (our use case):
  ```python
  with InternalAgentFactory.with_agents(test_agents):
      # Use test agents
      run_tests()
  # Original agents restored
  ```

- **Locks**:
  ```python
  with threading.Lock():
      # Critical section
      modify_shared_resource()
  # Lock is automatically released
  ```

## Advanced: Exception Handling

```python
@contextmanager
def error_handling_context():
    try:
        yield
    except SpecificError as e:
        # Handle specific errors
        log_error(e)
        # Can re-raise or suppress
    finally:
        # Always runs
        cleanup()
```

## References

- [Python contextlib documentation](https://docs.python.org/3/library/contextlib.html)
- [PEP 343 - The "with" Statement](https://www.python.org/dev/peps/pep-0343/)
- [Real Python: Context Managers](https://realpython.com/python-with-statement/)
