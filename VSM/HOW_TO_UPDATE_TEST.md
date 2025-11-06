# Example: How to Update test_scheduler_and_agents.py

## Updated Test Implementation

Here's how to modify your `test_scheduler_and_agents_with_full_cleanup_round` test to use the new context manager:

```python
def test_scheduler_and_agents_with_full_cleanup_round(self, integration_session, cleanup_scenario_data):
    # ... [All your existing setup code remains the same until the run_scheduler_tasks() call] ...
    
    # Set environment variables for agents
    os.environ['SCAN_TEMP_FOLDER'] = os.path.join(io_dir_for_storage_test, "temp_for_scanning")
    os.environ['SCAN_THREADS'] = str(1)
    os.environ['CLEAN_TEMP_FOLDER'] = os.path.join(io_dir_for_storage_test, "temp_for_cleaning")
    os.environ['CLEAN_SIM_WORKERS'] = str(1)
    os.environ['CLEAN_DELETION_WORKERS'] = str(2)
    os.environ['CLEAN_MODE'] = 'ANALYSE'
    
    # NEW: Import the factory and agents
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
    
    # NEW: Create test agents (they will pick up the environment variables set above)
    test_agents = [
        AgentCalendarCreation(),
        AgentScanVTSRootFolder(),       # Uses SCAN_TEMP_FOLDER, SCAN_THREADS env vars
        AgentCleanupCycleStart(),
        AgentNotification(),
        AgentCleanVTSRootFolder(),      # Uses CLEAN_TEMP_FOLDER, CLEAN_SIM_WORKERS, etc. env vars
        AgentCleanupCycleFinishing(),
        AgentCleanupCyclePrepareNext(),
    ]
    
    # NEW: Use context manager to inject test agents
    with InternalAgentFactory.with_agents(test_agents):
        # Step 3: Run scheduler to create scan tasks and execute scan
        run_scheduler_tasks()
        
        # Step 4: Verify that all scan tasks are finalized
        # ... your verification code ...
        
        # Step 5: Verify that simulations have been marked for cleanup
        # ... your verification code ...
        
        # Step 6: Run scheduler to create cleanup rounds calendar of tasks
        run_scheduler_tasks()
        
        # Step 7: Run scheduler to activate tasks that will execute agents
        run_scheduler_tasks()
        
        # Step 8: Verify that all tasks are finalized
        # ... your verification code ...
        
        # Step 9: Verify that simulations have been cleaned up as expected
        # ... your verification code ...
    
    # After exiting the context manager, original agents are automatically restored
```

## Why This Works

1. **Environment Variables**: The agents created inside the test will read the test-specific environment variables when they are instantiated
2. **Isolation**: Each test run gets fresh agent instances with the test configuration
3. **No Side Effects**: When the `with` block exits, the factory automatically resets to default agents
4. **Exception Safety**: Even if your test fails with an exception, the cleanup happens automatically

## Alternative: If You Need to Run Scheduler Multiple Times with Different Agents

```python
def test_scheduler_phases(self, integration_session, cleanup_scenario_data):
    # Setup...
    
    # Phase 1: Only calendar and scan agents
    phase1_agents = [
        AgentCalendarCreation(),
        AgentScanVTSRootFolder(),
    ]
    
    with InternalAgentFactory.with_agents(phase1_agents):
        run_scheduler_tasks()
        # ... verify phase 1 results ...
    
    # Phase 2: Cleanup agents
    phase2_agents = [
        AgentCleanVTSRootFolder(),
        AgentCleanupCycleFinishing(),
    ]
    
    with InternalAgentFactory.with_agents(phase2_agents):
        run_scheduler_tasks()
        # ... verify phase 2 results ...
```

## Alternative: If You Want to Skip Certain Agents

```python
def test_scheduler_without_notifications(self, integration_session, cleanup_scenario_data):
    # Setup...
    
    # Create agents but exclude notification agent
    test_agents = [
        AgentCalendarCreation(),
        AgentScanVTSRootFolder(),
        AgentCleanupCycleStart(),
        # AgentNotification(),  # SKIP this one
        AgentCleanVTSRootFolder(),
        AgentCleanupCycleFinishing(),
        AgentCleanupCyclePrepareNext(),
    ]
    
    with InternalAgentFactory.with_agents(test_agents):
        run_scheduler_tasks()
        # ... test without notifications being sent ...
```

## Alternative: Using Mock Agents

```python
from unittest.mock import Mock

def test_scheduler_with_mock_clean_agent(self, integration_session, cleanup_scenario_data):
    # Setup...
    
    # Create a mock clean agent to verify it's called but not actually clean
    mock_clean_agent = Mock()
    mock_clean_agent.agent_info = AgentInfo(
        agent_id="MockCleanAgent",
        action_types=[ActionType.CLEAN_ROOTFOLDER.value]
    )
    
    test_agents = [
        AgentCalendarCreation(),
        AgentScanVTSRootFolder(),
        AgentCleanupCycleStart(),
        AgentNotification(),
        mock_clean_agent,  # Mock instead of real clean agent
        AgentCleanupCycleFinishing(),
        AgentCleanupCyclePrepareNext(),
    ]
    
    with InternalAgentFactory.with_agents(test_agents):
        run_scheduler_tasks()
        
        # Verify the mock was called
        mock_clean_agent.run.assert_called()
```
