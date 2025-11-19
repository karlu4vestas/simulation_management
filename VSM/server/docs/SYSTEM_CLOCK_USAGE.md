# SystemClock Usage Guide

The `SystemClock` provides centralized time control for the application, making it easy to test time-dependent logic without modifying production code.

## Basic Usage

### Production (Real Time)
```python
from app.clock import SystemClock
from datetime import timezone

# Returns actual current time (naive local time)
now = SystemClock.now()
today = SystemClock.today()

# Returns timezone-aware UTC time
utc_now = SystemClock.now(timezone.utc)

# Legacy utcnow (naive UTC)
utc_now_naive = SystemClock.utcnow()
```

## Testing with Time Control

### Method 1: Fixed Time (Freeze Time)
```python
from datetime import datetime, timezone
from app.clock import SystemClock

# Set clock to specific moment (naive)
SystemClock.set_fixed(datetime(2025, 12, 31, 12, 0, 0))

# All calls return the same time
now1 = SystemClock.now()  # 2025-12-31 12:00:00 (naive)
time.sleep(5)
now2 = SystemClock.now()  # Still 2025-12-31 12:00:00 (naive)

# With timezone parameter - returns timezone-aware
utc_now = SystemClock.now(timezone.utc)  # 2025-12-31 12:00:00+00:00

# Reset to real time when done
SystemClock.set_real()
```

### Method 2: Time Offset (Time Travel)
```python
from app.clock import SystemClock

# Go 7 days into the future
SystemClock.set_offset_days(7)
now = SystemClock.now()  # Returns datetime.now() + 7 days

# Or use seconds for precise control
SystemClock.set_offset_seconds(3600)  # 1 hour ahead

# Negative offsets go to the past
SystemClock.set_offset_days(-30)  # 30 days ago

# Reset to real time
SystemClock.set_real()
```

### Method 3: Environment Variables
Set environment variables before starting the application:

```bash
# Fixed time
export APP_TIME_FIXED="2025-12-31T12:00:00"
python main.py

# Time offset (7 days ahead)
export APP_TIME_OFFSET_DAYS=7
python main.py

# Time offset in seconds (1 hour ahead)
export APP_TIME_OFFSET_SECONDS=3600
python main.py
```

## Usage in Tests

### Unit Test Example
```python
import pytest
from datetime import datetime
from app.clock import SystemClock
from cleanup.cleanup_dtos import CleanupState

class TestCleanupScheduling:
    
    def setup_method(self):
        """Reset clock before each test."""
        SystemClock.set_real()
    
    def teardown_method(self):
        """Clean up after each test."""
        SystemClock.set_real()
    
    def test_cleanup_not_ready_in_future(self):
        """Test that cleanup doesn't start when start_date is in future."""
        # Set current time to Dec 1, 2025
        SystemClock.set_fixed(datetime(2025, 12, 1, 0, 0, 0))
        
        # Create config with start date in future (Dec 31, 2025)
        config = create_config_with_start_date(datetime(2025, 12, 31, 0, 0, 0))
        
        # Should not be ready to start
        assert not config.can_start_cleanup_now()
    
    def test_cleanup_ready_when_time_arrives(self):
        """Test that cleanup starts when we reach start_date."""
        # Create config with start date Dec 31, 2025
        config = create_config_with_start_date(datetime(2025, 12, 31, 0, 0, 0))
        
        # Fast-forward to Dec 31, 2025
        SystemClock.set_fixed(datetime(2025, 12, 31, 0, 0, 0))
        
        # Should now be ready to start
        assert config.can_start_cleanup_now()
    
    def test_cleanup_schedule_with_offset(self):
        """Test cleanup scheduling 7 days in the future."""
        # Start at Dec 1, 2025
        SystemClock.set_fixed(datetime(2025, 12, 1, 0, 0, 0))
        
        # Create config starting today
        config = create_config_with_start_date(SystemClock.now())
        assert config.can_start_cleanup_now()
        
        # Travel 7 days into future
        SystemClock.set_offset_days(7)
        
        # Should still work (start_date is now in the past)
        assert config.can_start_cleanup_now()
```

### Integration Test Example
```python
def test_full_cleanup_cycle_simulation():
    """Simulate a complete cleanup cycle over time."""
    
    # Day 0: Schedule cleanup
    SystemClock.set_fixed(datetime(2025, 12, 1, 0, 0, 0))
    scheduler.create_calendars_for_cleanup_configuration_ready_to_start()
    
    # Day 1: Scan should be activated
    SystemClock.set_fixed(datetime(2025, 12, 2, 0, 0, 0))
    scheduler.update_calendars_and_tasks()
    assert get_current_task().action_type == ActionType.SCAN_ROOTFOLDER
    
    # Day 7: Review period
    SystemClock.set_fixed(datetime(2025, 12, 8, 0, 0, 0))
    scheduler.update_calendars_and_tasks()
    assert get_cleanup_state().progress == Progress.RETENTION_REVIEW
    
    # Day 30: Cleanup should execute
    SystemClock.set_fixed(datetime(2025, 12, 31, 0, 0, 0))
    scheduler.update_calendars_and_tasks()
    assert get_current_task().action_type == ActionType.CLEAN_ROOTFOLDER
```

## API Reference

### Time Access Methods
- `SystemClock.now(tz=None) -> datetime` - Get current local time. Pass `timezone.utc` for UTC-aware datetime.
- `SystemClock.utcnow() -> datetime` - Get current UTC time (naive, legacy method)
- `SystemClock.today() -> date` - Get current date

### Configuration Methods
- `SystemClock.set_real()` - Use actual system time (default)
- `SystemClock.set_fixed(dt: datetime)` - Freeze time at specific moment
- `SystemClock.set_offset_seconds(seconds: int)` - Offset time by seconds
- `SystemClock.set_offset_days(days: int)` - Offset time by days

### Status Methods
- `SystemClock.get_mode() -> str` - Get current mode ("real", "fixed", or "offset")
- `SystemClock.is_real() -> bool` - Check if using real system time

## Integration with Existing Code

The following functions now use `SystemClock`:

1. **`scheduler_db_actions.py`**
   - `create_calendars_for_cleanup_configuration_ready_to_start()`
   - Setting `config.dto.start_date`

2. **`cleanup_dtos.py`**
   - `CleanupState.can_start_cleanup_now()`

## Environment Variables

| Variable | Type | Example | Description |
|----------|------|---------|-------------|
| `APP_TIME_FIXED` | ISO datetime | `2025-12-31T12:00:00` | Freeze time at specific moment |
| `APP_TIME_OFFSET_DAYS` | Integer | `7` or `-30` | Offset by days (+ future, - past) |
| `APP_TIME_OFFSET_SECONDS` | Integer | `3600` or `-86400` | Offset by seconds |

Environment variables are checked at application startup in `web_api.py` lifespan.

## Best Practices

1. **Always reset in tests**: Use setup/teardown to call `SystemClock.set_real()`
2. **Use fixed time for deterministic tests**: Easier to reason about than offsets
3. **Use offsets for relative time tests**: When you need to test "7 days from now"
4. **Don't mix modes**: Choose one mode per test case
5. **Document time assumptions**: Note what "now" means in your test docstrings
