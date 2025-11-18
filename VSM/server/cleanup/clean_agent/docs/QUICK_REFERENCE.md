# Clean Agent - Quick Reference

## Import

```python
from cleanup_cycle.clean_agent import clean_main, CleanMode, CleanProgressWriter
```

## Basic Usage

```python
results = clean_main(
    simulation_paths=["//server/sim1", "//server/sim2"],
    clean_mode=CleanMode.ANALYSE,  # or CleanMode.DELETE
    output_path="./logs"
)
```

## Full Parameters

```python
results = clean_main(
    simulation_paths=simulation_list,           # Required: list[str]
    clean_mode=CleanMode.ANALYSE,               # ANALYSE or DELETE
    num_sim_workers=32,                         # Simulation processing threads
    num_deletion_workers=2,                     # File deletion threads
    deletion_queue_max_size=1_000_000,          # Backpressure control
    progress_reporter=custom_reporter,          # Optional: CleanProgressReporter
    output_path="./output"                      # Optional: log directory
)
```

## Custom Progress Reporter

```python
from cleanup_cycle.clean_agent import CleanProgressWriter

class MyReporter(CleanProgressWriter):
    def __init__(self, task_id):
        super().__init__(
            seconds_between_update=5,
            seconds_between_filelog=60
        )
        self.task_id = task_id
    
    def write_realtime_progress(self, nb_processed_sims, nb_cleaned_sims,
                               nb_issue_sims, nb_skipped_sims, 
                               mean_sims_second, deletion_queue_size,
                               active_threads):
        # Your custom implementation
        msg = f"Task {self.task_id}: {nb_processed_sims} processed"
        print(msg)

# Usage
reporter = MyReporter(task_id="TASK-001")
results = clean_main(
    simulation_paths=paths,
    progress_reporter=reporter,
    output_path="./logs"
)
```

## Return Value

```python
# Returns list[FileInfo]
for result in results:
    print(f"Path: {result.filepath}")
    print(f"Status: {result.external_retention}")
    print(f"Modified: {result.modified_date}")
    print(f"Type: {result.nodetype}")
```

## Output Files

When `output_path` is provided:
- `clean_progress_log.csv` - Progress snapshots
- `clean_errors.csv` - Error log

## Recommended Thread Counts

### Analyze Mode (Count Only)
```python
num_sim_workers=32-128       # I/O bound
num_deletion_workers=2       # Minimal
```

### Delete Mode (Actually Delete)
```python
num_sim_workers=32           # Balanced
num_deletion_workers=1024    # Maximize deletion throughput
```

## Integration with Agent Pattern

```python
from cleanup_cycle.cleanup_scheduler import AgentInterfaceMethods

class AgentProgressReporter(CleanProgressWriter):
    def __init__(self, agent):
        super().__init__(
            seconds_between_update=1,
            seconds_between_filelog=60
        )
        self.agent = agent
    
    def write_realtime_progress(self, **kwargs):
        msg = f"Processed: {kwargs['nb_processed_sims']}"
        AgentInterfaceMethods.task_progress(
            self.agent.task.id, 
            msg
        )

# In agent execute_task:
reporter = AgentProgressReporter(self)
results = clean_main(
    simulation_paths=simulations,
    progress_reporter=reporter,
    output_path=self.temp_folder
)
```

## Error Handling

```python
try:
    results = clean_main(
        simulation_paths=paths,
        clean_mode=CleanMode.DELETE,
        output_path="./logs"
    )
except KeyboardInterrupt:
    print("Interrupted by user")
except Exception as e:
    print(f"Error: {e}")
```

## Test

```bash
cd server
python -m cleanup_cycle.clean_agent.test_clean_main
```

## Key Classes

### CleanMode (Enum)
- `CleanMode.ANALYSE` - Count files/bytes, don't delete
- `CleanMode.DELETE` - Actually delete files

### Simulation (Stub)
Properties:
- `modified_date: date`
- `external_retention: ExternalRetentionTypes`
- `was_cleaned: bool`
- `has_issue: bool`
- `was_skipped: bool`

Methods:
- `get_files_to_clean() -> list[str]`

### CleanParameters
Holds:
- Queues (simulation, deletion, result, error)
- Counters (processed, cleaned, issue, skipped)
- Configuration (mode, worker counts)

## Files Structure

```
clean_agent/
├── clean_main.py              # Entry point
├── clean_workers.py           # Thread workers
├── clean_parameters.py        # Config & state
├── clean_progress_reporter.py # Progress reporting
├── simulation.py        # Simulation stubs
├── thread_safe_counters.py   # Thread-safe counters
└── test_clean_main.py         # Test script
```

## Quick Tips

1. **Start with ANALYSE mode** to see what would be cleaned
2. **Use custom progress reporter** for agent integration
3. **Adjust thread counts** based on mode (analyze vs delete)
4. **Monitor deletion queue size** to prevent memory overflow
5. **Check error log** after completion
6. **Results include all simulations**, check `external_retention` for status
