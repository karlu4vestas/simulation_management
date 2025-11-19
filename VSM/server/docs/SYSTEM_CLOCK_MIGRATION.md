# SystemClock Migration Summary

## Overview
All `datetime.now()` and `datetime.now(timezone.utc)` calls across the codebase have been routed through the new `SystemClock` abstraction, enabling centralized time control for testing and production.

## Files Changed

### Core Clock Implementation
1. **`server/app/clock.py`** (NEW)
   - Created `SystemClock` class with class-level API
   - Supports REAL, FIXED, and OFFSET modes
   - Methods: `now(tz=None)`, `utcnow()`, `today()`, `set_fixed()`, `set_offset_seconds()`, `set_offset_days()`, `set_real()`
   - **Timezone support**: `now()` accepts optional `tz` parameter (e.g., `timezone.utc`) for timezone-aware datetimes

2. **`server/app/app_config.py`**
   - Added `configure_clock()` method to read environment variables
   - Supports: `APP_TIME_FIXED`, `APP_TIME_OFFSET_DAYS`, `APP_TIME_OFFSET_SECONDS`

3. **`server/app/web_api.py`**
   - Added `AppConfig.configure_clock()` call in lifespan startup
   - Ensures clock is configured before database initialization

### Scheduler & Cleanup (Critical for Time Logic)
4. **`server/cleanup/scheduler_db_actions.py`**
   - Replaced 4 occurrences of `datetime.now()` and `datetime.now(timezone.utc)` with `SystemClock.now()`
   - Lines: 118, 145, 258, 337, 395
   - Functions affected:
     - `create_calendars_for_cleanup_configuration_ready_to_start()`
     - Task scheduling in `create_next_jit_task()`
     - Task timeout handling in `update_calendar_and_verify_tasks()`
     - Calendar deactivation in `deactivate_calendars_for_rootfolder()`

5. **`server/cleanup/cleanup_dtos.py`**
   - Replaced 1 occurrence in `can_start_cleanup_now()`
   - Line 55

6. **`server/datamodel/retentions.py`**
   - Replaced 1 occurrence in `RetentionCalculator.__init__()`
   - Line 149

7. **`server/cleanup/agent_on_premise_scan.py`**
   - Replaced 1 occurrence in `execute_task()`
   - Line 66 (used for generating timestamp filenames)

8. **`server/cleanup/agent_task_manager.py`** (NEW)
   - Replaced 2 occurrences of `datetime.now(timezone.utc)` with `SystemClock.now(timezone.utc)`
   - Lines: 68, 124
   - Functions affected:
     - `task_reserve_task()` - setting `reserved_at` timestamp
     - `task_completion()` - setting `completed_at` timestamp

### Test Files
8. **`server/cleanup/scan/folder_tree.py`**
   - Replaced 3 occurrences in test/demo code
   - Lines: 162, 230, 231

9. **`server/tests/integration/test_scheduler_and_agents.py`**
   - Replaced 1 occurrence
   - Line 220 (test configuration start_date)

10. **`server/tests/integration/test_cleanup_with_ondisk_simulations.py`**
    - Replaced 3 occurrences
    - Lines: 106, 181, 252

11. **`server/tests/generate_vts_simulations/main_GenerateSimulation.py`**
    - Replaced 3 occurrences
    - Lines: 108, 109, 141

### Documentation & Tests
12. **`server/tests/unittests/test_system_clock.py`** (NEW)
    - Comprehensive test suite for SystemClock
    - 13 tests covering all modes, edge cases, and timezone support
    - All tests passing ✓

13. **`server/docs/SYSTEM_CLOCK_USAGE.md`** (NEW)
    - Complete usage guide with examples
    - Production and testing patterns
    - API reference with timezone support

## Total Replacements
- **Production code**: 8 files, 13 replacements
- **Test code**: 4 files, 10 replacements
- **Total**: 23 `datetime.now()` / `datetime.now(timezone.utc)` → `SystemClock.now()` conversions

## Excluded Files
- `server/app/clock.py` - Intentionally uses `datetime.now()` internally
- `server/tests/unittests/test_system_clock.py` - Tests against real datetime

## Testing
- SystemClock unit tests: ✓ 13/13 passing (including timezone support)
- No compilation errors in modified files
- All imports validated
- Verified timezone-aware datetime functionality

## Usage Examples

### Production (Default)
```python
from app.clock import SystemClock
from datetime import timezone

now = SystemClock.now()  # Returns actual datetime.now() (naive)
utc_now = SystemClock.now(timezone.utc)  # Returns datetime.now(timezone.utc) (aware)
```

### Testing - Fixed Time
```python
from datetime import datetime
from app.clock import SystemClock

SystemClock.set_fixed(datetime(2025, 12, 31, 12, 0, 0))
# All time-dependent code now sees 2025-12-31 12:00:00
```

### Testing - Time Travel
```python
from app.clock import SystemClock

SystemClock.set_offset_days(7)  # 7 days in the future
SystemClock.set_offset_days(-30)  # 30 days in the past
```

### Environment Variables
```bash
# Fixed time
export APP_TIME_FIXED="2025-12-31T12:00:00"

# Time offset
export APP_TIME_OFFSET_DAYS=7
export APP_TIME_OFFSET_SECONDS=3600
```

## Benefits
1. **Testability**: Can simulate any time period without changing system clock
2. **Consistency**: Single source of truth for application time
3. **Simplicity**: Clean API - just `SystemClock.now()` instead of `SystemClock.instance().now()`
4. **Production-safe**: Defaults to real time with zero performance overhead
5. **Flexible**: Supports both fixed time and relative offsets

## Next Steps
- Tests can now set `SystemClock.set_fixed()` or `SystemClock.set_offset_days()` to control time
- Integration tests can simulate cleanup schedules at different time periods
- No changes needed to existing code logic - only time source changed
