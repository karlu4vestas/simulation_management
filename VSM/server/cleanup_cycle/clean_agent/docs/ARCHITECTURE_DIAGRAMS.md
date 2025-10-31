# Clean Agent Architecture Diagrams

## System Overview

```mermaid
flowchart TB
    User[User/Agent] --> CleanMain[clean_main function]
    
    CleanMain --> Params[CleanParameters]
    CleanMain --> Reporter[Progress Reporter]
    CleanMain --> Workers[Spawn Workers]
    
    Workers --> SimWorkers[Simulation Workers]
    Workers --> DelWorkers[Deletion Workers]
    Workers --> Monitor[Progress Monitor]
    Workers --> ErrorWriter[Error Writer]
    
    SimWorkers --> SimQueue[Simulation Queue]
    SimWorkers --> ResultQueue[Result Queue]
    SimWorkers --> DelQueue[Deletion Queue]
    
    DelWorkers --> DelQueue
    
    Monitor --> Reporter
    ErrorWriter --> ErrorLog[errors.csv]
    Reporter --> ProgressLog[progress_log.csv]
    
    ResultQueue --> Results[FileInfo Results]
    Results --> User
```

## Thread Flow

```mermaid
sequenceDiagram
    participant Main as clean_main()
    participant Params as CleanParameters
    participant SimWorker as Simulation Worker
    participant DelWorker as Deletion Worker
    participant Monitor as Progress Monitor
    participant Sim as Simulation (stub)
    
    Main->>Params: Create parameters & queues
    Main->>Main: Load simulation_queue with paths
    Main->>SimWorker: Spawn N workers
    Main->>DelWorker: Spawn M workers
    Main->>Monitor: Spawn monitor
    
    loop For each simulation
        SimWorker->>Params: Get path from simulation_queue
        SimWorker->>Sim: Create Simulation(path)
        Sim-->>SimWorker: Simulation object
        SimWorker->>Sim: get_files_to_clean()
        Sim-->>SimWorker: List of file paths
        SimWorker->>Params: Put files in file_deletion_queue
        SimWorker->>Params: Put Simulation in result_queue
        SimWorker->>Params: Update counters
    end
    
    loop While files in deletion_queue
        DelWorker->>Params: Get file from file_deletion_queue
        DelWorker->>DelWorker: Delete or analyze file
        DelWorker->>Params: Update deletion_measures
    end
    
    loop Every N seconds
        Monitor->>Params: Read counters
        Monitor->>Monitor: Calculate stats
        Monitor->>Monitor: write_realtime_progress()
        Monitor->>Monitor: write_filelog()
    end
    
    Main->>Main: Wait for simulation_queue.join()
    Main->>SimWorker: Send poison pills
    Main->>Main: Wait for file_deletion_queue.join()
    Main->>DelWorker: Send poison pills
    Main->>Monitor: Set stop_event
    Main->>Main: Collect results from result_queue
    Main-->>Main: Return list[FileInfo]
```

## Component Architecture

```mermaid
classDiagram
    class CleanMain {
        +clean_main(paths, mode, workers, ...) list~FileInfo~
    }
    
    class CleanParameters {
        +CleanMode clean_mode
        +Queue simulation_queue
        +Queue file_deletion_queue
        +Queue result_queue
        +Queue error_queue
        +Event stop_event
        +ThreadSafeCounter simulations_processed
        +ThreadSafeCounter simulations_cleaned
        +ThreadSafeCounter simulations_issue
        +ThreadSafeCounter simulations_skipped
        +ThreadSafeDeletionCounter deletion_measures
    }
    
    class CleanProgressReporter {
        <<abstract>>
        +update()*
    }
    
    class CleanProgressWriter {
        +open(output_path)
        +close()
        +update(stats)
        +write_realtime_progress()
        +write_filelog()
    }
    
    class Simulation {
        <<stub>>
        +str path
        +date modified_date
        +ExternalRetentionTypes external_retention
        +bool was_cleaned
        +bool has_issue
        +bool was_skipped
        +get_files_to_clean() list~str~
    }
    
    class Workers {
        +simulation_worker(params)
        +deletion_worker(params)
        +progress_monitor_worker(params, reporter)
        +error_writer_worker(params, log_path)
    }
    
    class ThreadSafeCounter {
        -int _counter
        -Lock _lock
        +increment(value)
        +value() int
        +change_value(new_value)
    }
    
    class ThreadSafeDeletionCounter {
        -int files_deleted
        -int bytes_deleted
        -Lock _lock
        +add(files, bytes)
        +values() tuple
    }
    
    CleanMain --> CleanParameters
    CleanMain --> CleanProgressReporter
    CleanMain --> Workers
    CleanProgressReporter <|-- CleanProgressWriter
    CleanParameters --> ThreadSafeCounter
    CleanParameters --> ThreadSafeDeletionCounter
    Workers --> Simulation
    Workers --> CleanParameters
```

## State Diagram - Simulation Processing

```mermaid
stateDiagram-v2
    [*] --> InQueue: Path added to simulation_queue
    
    InQueue --> Processing: Worker picks up path
    
    Processing --> CreateSim: Create Simulation(path)
    
    CreateSim --> GetFiles: Call get_files_to_clean()
    
    GetFiles --> QueueFiles: Put files in deletion_queue
    
    QueueFiles --> UpdateStatus: Determine status
    
    UpdateStatus --> Cleaned: was_cleaned = True
    UpdateStatus --> Issue: has_issue = True
    UpdateStatus --> Skipped: was_skipped = True
    
    Cleaned --> Logged: Increment simulations_cleaned
    Issue --> Logged: Increment simulations_issue
    Skipped --> Logged: Increment simulations_skipped
    
    Logged --> ResultQueue: Put in result_queue
    ResultQueue --> IncrementProcessed: Increment simulations_processed
    IncrementProcessed --> [*]
    
    state Processing {
        [*] --> Scanning
        Scanning --> Evaluating: Scan complete
        Evaluating --> [*]
    }
```

## Data Flow

```mermaid
flowchart LR
    Input[Simulation Paths] --> SimQueue[Simulation Queue]
    
    SimQueue --> SimWorker1[Sim Worker 1]
    SimQueue --> SimWorker2[Sim Worker 2]
    SimQueue --> SimWorkerN[Sim Worker N]
    
    SimWorker1 --> FileQueue[File Deletion Queue<br/>Max Size: 1M]
    SimWorker2 --> FileQueue
    SimWorkerN --> FileQueue
    
    SimWorker1 --> ResQueue[Result Queue]
    SimWorker2 --> ResQueue
    SimWorkerN --> ResQueue
    
    FileQueue --> DelWorker1[Del Worker 1]
    FileQueue --> DelWorker2[Del Worker 2]
    FileQueue --> DelWorkerM[Del Worker M]
    
    DelWorker1 --> Counters[Deletion Counters]
    DelWorker2 --> Counters
    DelWorkerM --> Counters
    
    ResQueue --> Output[FileInfo Results]
    
    SimWorker1 --> ErrQueue[Error Queue]
    SimWorker2 --> ErrQueue
    DelWorker1 --> ErrQueue
    DelWorker2 --> ErrQueue
    
    ErrQueue --> ErrWriter[Error Writer]
    ErrWriter --> ErrLog[errors.csv]
    
    Counters --> Monitor[Progress Monitor]
    Monitor --> Console[Console Output]
    Monitor --> ProgLog[progress_log.csv]
```

## Backpressure Mechanism

```mermaid
flowchart TD
    SimWorker[Simulation Worker] --> CheckQueue{Deletion Queue<br/>Size < Max?}
    
    CheckQueue -->|Yes| PutFile[Put file in queue]
    CheckQueue -->|No| Block[Block until space available]
    
    Block --> CheckQueue
    PutFile --> NextFile{More files?}
    
    NextFile -->|Yes| CheckQueue
    NextFile -->|No| Done[Processing complete]
    
    DelWorker[Deletion Worker] --> GetFile[Get file from queue]
    GetFile --> Process[Process file]
    Process --> FreeSpace[Free space in queue]
    FreeSpace --> GetFile
```

## Custom Progress Reporter Extension

```mermaid
classDiagram
    class CleanProgressReporter {
        <<abstract>>
        +update()*
    }
    
    class CleanProgressWriter {
        +open(output_path)
        +close()
        +update(stats)
        +write_realtime_progress()
        +write_filelog()
    }
    
    class AgentCleanProgressWriter {
        +agent_instance
        +write_realtime_progress()
    }
    
    class YourCustomReporter {
        +your_custom_fields
        +write_realtime_progress()
        +your_custom_methods()
    }
    
    CleanProgressReporter <|-- CleanProgressWriter
    CleanProgressWriter <|-- AgentCleanProgressWriter
    CleanProgressWriter <|-- YourCustomReporter
    
    AgentCleanProgressWriter --> AgentInterface: task_progress()
```

## Usage Pattern

```mermaid
sequenceDiagram
    participant User
    participant CleanMain
    participant Reporter
    participant Workers
    participant FileSystem
    
    User->>CleanMain: clean_main(paths, mode, config)
    CleanMain->>Reporter: open(output_path)
    CleanMain->>Workers: Start threads
    
    loop For each simulation
        Workers->>FileSystem: Scan simulation
        FileSystem-->>Workers: File list
        Workers->>Workers: Queue files
    end
    
    loop For each file
        Workers->>FileSystem: Delete/analyze file
        FileSystem-->>Workers: Done
    end
    
    loop Every N seconds
        Reporter->>Reporter: write_realtime_progress()
        Reporter->>Reporter: write_filelog()
    end
    
    Workers-->>CleanMain: All done
    CleanMain->>Reporter: close()
    CleanMain-->>User: list[FileInfo]
```
