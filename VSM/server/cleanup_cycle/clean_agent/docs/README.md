# Clean Agent Module

A focused, multi-threaded implementation for cleaning VTS simulations.

## Overview

The `clean_agent` module provides a streamlined approach to cleaning VTS simulation folders. It's designed to be independent from the legacy `clean_old` implementation and follows modern Python patterns with clear separation of concerns.

## Architecture

### Key Components

```
clean_agent/
├── clean_main.py              # Main entry point
├── clean_workers.py           # Worker thread implementations
├── clean_parameters.py        # Configuration and state
├── clean_progress_reporter.py # Progress reporting (abstract + default)
├── simulation_stubs.py        # Simulation class stubs
├── thread_safe_counters.py   # Thread-safe counters
└── test_clean_main.py         # Test/demo script
```

### Thread Architecture

```
Main Thread
    ├─> Simulation Workers (configurable, default: 32)
    │   └─> Process simulations, identify files to clean
    │
    ├─> Deletion Workers (configurable, default: 2-1024)
    │   └─> Delete or analyze files
    │
    ├─> Progress Monitor (1 thread)
    │   └─> Report progress periodically
    │
    └─> Error Writer (1 thread)
        └─> Write errors to CSV log
```

## Usage

### Basic Usage

```python
from cleanup_cycle.clean_agent import clean_main, CleanMode

# Analyze mode (count files/bytes, no deletion)
results = clean_main(
    simulation_paths=["//server/sim1", "//server/sim2"],
    clean_mode=CleanMode.ANALYSE,
    num_sim_workers=32,
    num_deletion_workers=2,
    output_path="./clean_logs"
)

# Delete mode (actually delete files)
results = clean_main(
    simulation_paths=["//server/sim1", "//server/sim2"],
    clean_mode=CleanMode.DELETE,
    num_sim_workers=32,
    num_deletion_workers=1024,
    deletion_queue_max_size=1_000_000,
    output_path="./clean_logs"
)
```

### Custom Progress Reporter

Following the `AgentProgressWriter` pattern from `on_premise_scan_agent.py`:

```python
from cleanup_cycle.clean_agent import clean_main, CleanProgressWriter

class MyCustomProgressReporter(CleanProgressWriter):
    """Custom progress reporter for integration with external systems."""
    
    def __init__(self, task_id: str):
        super().__init__(seconds_between_update=5, seconds_between_filelog=60)
        self.task_id = task_id
    
    def write_realtime_progress(self, nb_processed_sims, nb_cleaned_sims, 
                               nb_issue_sims, nb_skipped_sims, 
                               mean_sims_second, deletion_queue_size, 
                               active_threads):
        """Override to send progress to external API."""
        # Call external API with progress
        # AgentInterfaceMethods.task_progress(self.task_id, msg)
        msg = f"Processed: {nb_processed_sims}, Rate: {mean_sims_second}/s"
        print(msg)

# Use custom reporter
reporter = MyCustomProgressReporter(task_id="TASK-001")
results = clean_main(
    simulation_paths=paths,
    progress_reporter=reporter,
    output_path="./logs"
)
```

## Parameters

### clean_main() Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `simulation_paths` | `list[str]` | Required | List of simulation folder paths |
| `clean_mode` | `CleanMode` | `ANALYSE` | ANALYSE or DELETE mode |
| `num_sim_workers` | `int` | `32` | Number of simulation processing threads |
| `num_deletion_workers` | `int` | `2` | Number of file deletion threads |
| `deletion_queue_max_size` | `int` | `1_000_000` | Max size of deletion queue (backpressure) |
| `progress_reporter` | `CleanProgressReporter` | `None` | Custom progress reporter (optional) |
| `output_path` | `str` | `None` | Directory for log files (optional) |

### Return Value

Returns `list[FileInfo]` with:
- `filepath`: Simulation path
- `modified_date`: Last modification date
- `nodetype`: `FolderTypeEnum.VTS_SIMULATION`
- `external_retention`: Status (Clean/Issue/UNDEFINED)

## Progress Reporting

### Default Progress Output

**Console (real-time):**
```
Processed: 1523; Cleaned: 1200; Issue: 23; Skipped: 300; Rate: 45 sims/s; Queue: 12450; Threads: 42
```

**CSV Log (periodic):**
```csv
time;duration (min);processed;cleaned;issue;skipped;current sims/s;mean sims/s;deletion queue;active threads
2025-10-31 10:15:23;5;1523;1200;23;300;45;38;12450;42
```

### Custom Progress Reporter

Extend `CleanProgressReporter` (abstract) or `CleanProgressWriter` (default implementation):

```python
from cleanup_cycle.clean_agent import CleanProgressReporter

class MyReporter(CleanProgressReporter):
    def update(self, nb_processed_sims, nb_cleaned_sims, nb_issue_sims,
               nb_skipped_sims, deletion_queue_size, active_threads):
        # Your custom implementation
        pass
```

## Simulation Class (Stub)

The current implementation uses stub classes to avoid dependencies:

```python
class Simulation(BaseSimulation):
    @property
    def modified_date(self) -> date
    
    @property
    def external_retention(self) -> ExternalRetentionTypes
    
    @property
    def was_cleaned(self) -> bool
    
    @property
    def has_issue(self) -> bool
    
    @property
    def was_skipped(self) -> bool
    
    def get_files_to_clean(self) -> list[str]
```

**To replace with real implementation:**
1. Implement the actual scanning and file identification logic
2. Update `simulation_stubs.py` with the real implementation
3. Ensure the properties and methods match the interface

## Output Files

When `output_path` is provided:

- `clean_progress_log.csv` - Periodic progress snapshots
- `clean_errors.csv` - Errors encountered during processing

## Error Handling

Errors are:
1. Logged to `clean_errors.csv`
2. Counted in error counter
3. Processing continues (fail-safe)

## Testing

Run the test script:

```bash
cd server/cleanup_cycle/clean_agent
python test_clean_main.py
```

This will:
- Process sample simulation paths
- Demonstrate basic usage
- Show custom progress reporter example
- Create logs in `./clean_test_output/`

## Thread Configuration

### Recommended Settings

**Analyze Mode:**
- `num_sim_workers`: 32-128 (I/O bound)
- `num_deletion_workers`: 2 (minimal, just counting)

**Delete Mode:**
- `num_sim_workers`: 32 (balanced)
- `num_deletion_workers`: 1024 (maximize deletion throughput)
- `deletion_queue_max_size`: 1,000,000 (prevent memory overflow)

### Queue Size Considerations

The `deletion_queue_max_size` parameter provides backpressure:
- Simulation workers will block if queue is full
- Prevents memory overflow with large simulations
- Balance between memory usage and throughput

## Comparison with clean_old

| Feature | clean_old | clean_agent |
|---------|-----------|-------------|
| Dependencies | Tightly coupled | Minimal, focused |
| Progress Reporting | Custom writer tasks | Abstract base + default impl |
| Configuration | Complex parameters class | Simple, clear parameters |
| Testing | Integrated with scanning | Standalone test script |
| Simulation Class | Full implementation | Stub (replaceable) |
| Code Size | ~2000 lines | ~800 lines |

## Future Enhancements

1. **Replace Simulation Stub**: Implement real file scanning and identification
2. **Add Cleaner Strategies**: Port `clean_all_pr_ext`, `clean_all_but_one_pr_ext`
3. **Enhanced Logging**: Add detailed simulation-level logs
4. **Statistics**: More detailed statistics and reporting
5. **Resume Capability**: Save/resume from interrupted runs

## Integration Example

Example integration with `on_premise_clean_agent.py`:

```python
from cleanup_cycle.clean_agent import clean_main, CleanMode, CleanProgressWriter
from cleanup_cycle.cleanup_scheduler import AgentInterfaceMethods

class AgentCleanProgressWriter(CleanProgressWriter):
    def __init__(self, agent_clean, seconds_between_update, seconds_between_filelog):
        super().__init__(seconds_between_update, seconds_between_filelog)
        self.agent_clean = agent_clean
    
    def write_realtime_progress(self, nb_processed_sims, nb_cleaned_sims,
                               nb_issue_sims, nb_skipped_sims, mean_sims_second,
                               deletion_queue_size, active_threads):
        msg = f"Processed: {nb_processed_sims}, Cleaned: {nb_cleaned_sims}, Rate: {mean_sims_second}/s"
        AgentInterfaceMethods.task_progress(self.agent_clean.task.id, msg)

# In your agent's execute_task method:
progress_reporter = AgentCleanProgressWriter(self, 
                                            seconds_between_update=1,
                                            seconds_between_filelog=60)
results = clean_main(
    simulation_paths=simulations_to_clean,
    clean_mode=CleanMode.DELETE,
    progress_reporter=progress_reporter,
    output_path=self.temporary_result_folder
)
```
