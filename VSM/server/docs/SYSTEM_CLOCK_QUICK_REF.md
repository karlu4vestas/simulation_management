# SystemClock Quick Reference

## Basic Usage

```python
from app.clock import SystemClock
from datetime import timezone

# Get current time (works in production and tests)
now = SystemClock.now()              # Naive local time
today = SystemClock.today()          # Date
utc_now = SystemClock.now(timezone.utc)  # Timezone-aware UTC
```

## Testing - Control Time

```python
from datetime import datetime
from app.clock import SystemClock

# Method 1: Freeze time
SystemClock.set_fixed(datetime(2025, 12, 31, 12, 0, 0))

# Method 2: Time travel (offset from real time)
SystemClock.set_offset_days(7)      # 7 days in future
SystemClock.set_offset_days(-30)    # 30 days in past
SystemClock.set_offset_seconds(3600)  # 1 hour ahead

# Reset to real time
SystemClock.set_real()

# Check current mode
print(SystemClock.get_mode())  # "real", "fixed", or "offset"
print(SystemClock.is_real())   # True if using real time
```

## Environment Variables

```bash
# Fixed time (ISO format)
export APP_TIME_FIXED="2025-12-31T12:00:00"

# Time offset in days
export APP_TIME_OFFSET_DAYS=7

# Time offset in seconds  
export APP_TIME_OFFSET_SECONDS=3600
```

## Test Pattern

```python
class TestMyFeature:
    def setup_method(self):
        SystemClock.set_real()  # Reset before each test
    
    def teardown_method(self):
        SystemClock.set_real()  # Reset after each test
    
    def test_feature_at_specific_time(self):
        # Set time for this test
        SystemClock.set_fixed(datetime(2025, 12, 31, 0, 0, 0))
        
        # Your test code here - all calls to SystemClock.now()
        # will return 2025-12-31 00:00:00
        result = my_time_dependent_function()
        
        assert result == expected_value
```

## Key Functions Using SystemClock

### Scheduler
- `CleanupScheduler.create_calendars_for_cleanup_configuration_ready_to_start()`
- `CleanupScheduler.create_next_jit_task()`

### Cleanup State  
- `CleanupState.can_start_cleanup_now()`

### Retention Calculator
- `RetentionCalculator.__init__()`

### Agents
- `AgentScanVTSRootFolder.execute_task()` (for filename timestamps)

## Production Behavior
- Defaults to real system time (`datetime.now()`)
- Zero performance overhead
- No changes needed to existing code
- Configure via environment variables if needed
