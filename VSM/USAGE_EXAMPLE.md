# InternalAgentFactory Usage Examples

## Default Production Behavior

No changes needed in production code. The factory automatically creates and registers default agents:

```python
# In web_api.py or anywhere else
from cleanup_cycle.internal_agents import InternalAgentFactory

def run_scheduler_tasks():
    """Background task to run internal agents and update calendars/tasks"""
    InternalAgentFactory.run_internal_agents()  # Uses default agents
    CleanupScheduler.update_calendars_and_tasks()
```

## Testing with Custom Agents

### Recommended: Using Context Manager (with_agents)

```python
def test_scheduler_and_agents_with_full_cleanup_round(self, integration_session, cleanup_scenario_data):
    # ... setup test data ...
    
    # Set up environment variables for agents
    os.environ['SCAN_TEMP_FOLDER'] = os.path.join(io_dir_for_storage_test, "temp_for_scanning")
    os.environ['SCAN_THREADS'] = str(1)
    os.environ['CLEAN_TEMP_FOLDER'] = os.path.join(io_dir_for_storage_test, "temp_for_cleaning")
    os.environ['CLEAN_SIM_WORKERS'] = str(1)
    os.environ['CLEAN_DELETION_WORKERS'] = str(2)
    os.environ['CLEAN_MODE'] = 'ANALYSE'
    
    # Import agents
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
    
    # Create test-specific agents list
    test_agents = [
        AgentCalendarCreation(),
        AgentScanVTSRootFolder(),      # Will use test environment variables
        AgentCleanupCycleStart(),
        AgentNotification(),
        AgentCleanVTSRootFolder(),     # Will use test environment variables
        AgentCleanupCycleFinishing(),
        AgentCleanupCyclePrepareNext(),
    ]
    
    # Use context manager - ensures cleanup
    with InternalAgentFactory.with_agents(test_agents):
        # Run scheduler with test agents
        run_scheduler_tasks()
        
        # Verify scan tasks completed
        # ... assertions ...
        
        # Run scheduler again to execute next phase
        run_scheduler_tasks()
        
        # Verify cleanup tasks completed
        # ... assertions ...
    
    # Original agents automatically restored after this block
```

### Alternative: Manual Registration (Not Recommended)

```python
def test_with_manual_registration(self):
    test_agents = [AgentCalendarCreation(), AgentScanVTSRootFolder()]
    
    try:
        InternalAgentFactory.register_agents(test_agents)
        run_scheduler_tasks()
        # ... assertions ...
    finally:
        InternalAgentFactory.reset_to_defaults()
```

## Mocking Individual Agents

If you need to mock specific agent behavior:

```python
from unittest.mock import Mock, patch

def test_with_mocked_scan_agent(self):
    # Create a mock agent
    mock_scan_agent = Mock(spec=AgentScanVTSRootFolder)
    mock_scan_agent.run = Mock()
    
    test_agents = [
        AgentCalendarCreation(),
        mock_scan_agent,  # Use mock instead of real agent
        AgentCleanupCycleStart(),
        # ... other agents
    ]
    
    with InternalAgentFactory.with_agents(test_agents):
        run_scheduler_tasks()
        
        # Verify mock was called
        mock_scan_agent.run.assert_called_once()
```

## Testing Specific Agent Subsets

Run only specific agents for focused testing:

```python
def test_only_calendar_creation(self):
    # Only test calendar creation agent
    test_agents = [AgentCalendarCreation()]
    
    with InternalAgentFactory.with_agents(test_agents):
        InternalAgentFactory.run_internal_agents()
        # ... verify calendar was created ...
```

## Advanced: Custom Agent Factory

For complex scenarios where you need dynamic agent creation:

```python
def test_with_agent_factory(self):
    def custom_agent_factory():
        # Create agents dynamically based on test conditions
        return [
            AgentCalendarCreation(),
            AgentScanVTSRootFolder() if condition else MockScanAgent(),
            # ...
        ]
    
    try:
        InternalAgentFactory.register_agents_factory(custom_agent_factory)
        run_scheduler_tasks()
        # ... assertions ...
    finally:
        InternalAgentFactory.reset_to_defaults()
```

## Key Benefits for Testing

1. **Isolation**: Tests use their own agent instances
2. **Environment-Specific**: Agents pick up test environment variables
3. **No Side Effects**: Original configuration automatically restored
4. **Clean Teardown**: Context manager ensures cleanup even on test failure
5. **Flexibility**: Can mix real agents, mocks, and test-specific implementations
