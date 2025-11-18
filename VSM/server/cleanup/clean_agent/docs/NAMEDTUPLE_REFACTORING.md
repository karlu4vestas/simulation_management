# NamedTuple Refactoring - Summary

## What Changed

The `clean_main` function now accepts simulation data from the database with proper type safety using **NamedTuple**.

### Previous Signature
```python
def clean_main(
    simulation_paths: list[str],  # Only paths
    ...
) -> list[FileInfo]:
```

### New Signature
```python
class FileInfo(NamedTuple):
    """Simulation metadata from database."""
    path: str
    modified_date: datetime

def clean_main(
    simulations: list[FileInfo],  # Path + modified_date
    ...
) -> list[FileInfo]:
```

## Important: Caller Responsibility

**The caller is now responsible for:**
1. **Creating the progress reporter** - `clean_main` will fail if not provided
2. **Opening it** before calling `clean_main`
3. **Closing it** after `clean_main` completes (use try/finally)
4. **Providing the output_path** - Required for error logs

This follows the principle of separation of concerns - `clean_main` doesn't create or manage external resources; it only uses them.

## Why NamedTuple?

✅ **Type-safe** - IDE autocomplete and static type checking  
✅ **Immutable** - Cannot accidentally modify data  
✅ **Memory efficient** - Less overhead than dicts  
✅ **Readable** - `sim.path` instead of `sim["path"]`  
✅ **Performance** - Faster attribute access  
✅ **Clear API contract** - Explicit structure  

## How to Use

### From Database Query
```python
from datetime import datetime
from cleanup_cycle.clean_agent.clean_main import clean_main, FileInfo
from cleanup_cycle.clean_agent.clean_parameters import CleanMode
from cleanup_cycle.clean_agent.clean_progress_reporter import CleanProgressWriter

# Query database
db_results = db_api.query_simulations(where="status = 'ready'")

# Convert to FileInfo
simulations = [
    FileInfo(path=row["path"], modified_date=row["modified_date"])
    for row in db_results
]

# Create and open progress reporter (caller's responsibility)
progress_reporter = CleanProgressWriter()
output_path = "./logs"
progress_reporter.open(output_path)

try:
    # Run cleanup
    results = clean_main(
        simulations=simulations,
        progress_reporter=progress_reporter,
        output_path=output_path,
        clean_mode=CleanMode.DELETE,
        num_sim_workers=32,
        num_deletion_workers=2
    )
    
    # Process results
    for result in results:
        print(f"{result.filepath}: {result.external_retention}")
finally:
    # Caller must close (caller's responsibility)
    progress_reporter.close()
```

### Minimal Example
```python
simulations = [
    FileInfo("//server/sim1", datetime(2024, 1, 15)),
    FileInfo("//server/sim2", datetime(2024, 2, 20)),
]

reporter = CleanProgressWriter()
reporter.open("./logs")

try:
    results = clean_main(
        simulations=simulations,
        progress_reporter=reporter,
        output_path="./logs"
    )
finally:
    reporter.close()
```

## Files Changed

1. **`clean_main.py`**
   - Added `SimulationInput` NamedTuple
   - Changed parameter from `simulation_paths: list[str]` to `simulations: list[SimulationInput]`
   - Updated queue loading logic

2. **`simulation.py`**
   - Added `modified_date` parameter to `Simulation.__init__()`
   - Modified date now comes from database instead of `date.today()`

3. **`clean_workers.py`**
   - Updated `simulation_worker()` to unpack `SimulationInput` from queue
   - Pass `modified_date` to `Simulation` constructor

4. **`test_clean_main.py`**
   - Updated tests to use `SimulationInput`
   - Added example dates

5. **`example_db_usage.py`** *(NEW)*
   - Complete examples of database integration
   - Shows how to convert various DB result formats to `SimulationInput`

## Benefits for Database Integration

The modification date is now **explicitly provided from the database**, ensuring:
- **Consistency**: Same date used for retention calculations and storage
- **Accuracy**: Database is the single source of truth
- **Performance**: No filesystem calls to get modification dates
- **Traceability**: Clear data flow from DB → clean_main → results

## Backward Compatibility

This is a **breaking change**. Code that previously called:
```python
clean_main(simulation_paths=["//server/sim1", "//server/sim2"])
```

Must now call:
```python
clean_main(simulations=[
    SimulationInput("//server/sim1", datetime(2024, 1, 15)),
    SimulationInput("//server/sim2", datetime(2024, 2, 20))
])
```

## Testing

All tests pass:
- `test_clean_main.py` - Main functionality test
- `example_db_usage.py` - Database integration example

Run with:
```bash
cd /workspaces/simulation_management/VSM/server
python -m cleanup_cycle.clean_agent.test_clean_main
python -m cleanup_cycle.clean_agent.example_db_usage
```
