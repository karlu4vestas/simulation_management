# Mermaid Diagrams for VTS Simulation Cleanup Application

## 1. Class Diagram

```mermaid
classDiagram
    class IO {
        +int size_of_buffer
        +write(file_path, data_to_write)$ void
        +read(file_path)$ bytes
        +exist_path(path)$ bool
        +create_folder(path, exist_ok)$ void
        +delete_folder_tree(path, ignore_errors)$ void
        +delete_file(file_path)$ void
        +get_file_list(folder_root)$ list
        +getDirectories(root_folder, ignore_error)$ tuple
    }

    class FileDeletionMethod {
        <<enumeration>>
        Analyse
        Delete
    }

    class FolderType {
        <<enumeration>>
        MissingFolder
        NormalFolder
        PartialSimulation
        StandardSimulation
        StandardSimulation_partial
    }

    class FolderLogInfo {
        <<enumeration>>
        folder
        root_folder
        folder_files
        folder_bytes
        folder_type
        prepper
        setname_count
        reproducibility
        cleaning_status
        cleanable_files
        cleanable_bytes
    }

    class ThreadSafeCounter {
        -int _counter
        -Lock _lock
        +increment(increment)
        +value() int
        +change_value(new_value)
    }

    class ThreadSafeDeletionCounter {
        -int files_deleted
        -int bytes_deleted
        -Lock _lock
        +add(files_deleted, bytes_deleted)
        +values() tuple
    }

    class LogInfoElement {
        +bool can_clean
        +__init__(pass_criteria)
    }

    class SimulationStatus {
        +dict columns
        +dict can_clean_criteria
        +dict default_columns_dict$
        +__init__(folder, ignore_splits)
        +add(info_type, can_clean, log_msg)
        +can_clean() bool
    }

    class BaseSimulation {
        +str base_path
        +__init__(base_path)
        +hasValidSetNames() bool
        +getSetNames() list
        +get_cleaner_files() dict
    }

    class clean_folder_type {
        +str key
        +list local_folder_names
        +tuple extensions
        +__init__(local_folder_names, extensions)
        +retrieve_file_list(simulation, base_path) list
    }

    class clean_all_pr_ext {
        +__init__(local_folder_names, extensions)
        +retrieve_file_list(simulation, base_path) list
    }

    class clean_all_but_one_pr_ext {
        +__init__(local_folder_names, extensions)
        +retrieve_file_list(simulation, base_path) list
    }

    class Simulation {
        +list~clean_folder_type~ cleaners$
        +regex prepspace4_files2ignore$
        +str base_path
        +SimulationStatus simulation_status
        +list base_entries
        +FolderType folder_type
        +list set_names
        +ThreadPoolExecutor threadpool
        +__init__(base_path, threadpool)
        +get_base_entries() list
        +get_direct_child_folders() list
        +get_standard_folders() list
        +getFolderType() FolderType
        +get_all_entries() tuple
        +get_entries(local_path) tuple
        +get_set_output_files_for_cleaners() dict
        +get_cleaner_files() dict
        +getINPUTS_files() list
        +get_simulation_size() tuple
        +get_cleaner_size() tuple
        +is_reproducible() tuple
        +getSetFiles() list
        +getSetNames() list
        +getPrepper() str
        +hasValidSetNames() bool
        +get_folder_exclusions(max_modification_date) tuple
        +eval(file_deletion_method) tuple
    }

    class clean_parameters {
        +FileDeletionMethod file_deletion_method
        +regex re_folder_exclusions
        +datetime min_date
        +datetime max_date
        +set vts_standard_folders
        +set hawc2_standard_folders
        +Queue file_error_queue
        +Queue folder_error_queue
        +Queue deletion_log_queue
        +Queue file_deletion_queue
        +Queue folder_scan_queue
        +Queue scan_log_queue
        +Event stop_event
        +ThreadSafeCounter file_errors
        +ThreadSafeCounter folder_errors
        +ThreadSafeCounter folder_count
        +ThreadSafeCounter folder_count_total
        +ThreadSafeDeletionCounter deletionMeasures
        +ThreadSafeCounter folders_processed
        +ThreadSafeCounter sim_processed
        +ThreadSafeCounter sim_cleaned
        +ThreadSafeCounter sim_already_cleaned
        +ThreadSafeCounter sim_irreproducible
        +ThreadSafeCounter sim_ignored
        +dict folder_root_dict
        +float start_time
        +__init__(base_folder, file_deletion_method, cleaner_threads, deletion_threads)
        +set_min_max_date(min_date, max_date)
        +close_logStatus()
        +start_worker(no_folder_scanner_threads, no_deletions_threads)
        +stop_workers()
    }

    class clean_parameters_start_stop {
        +FileHandle progress_file_handle
        +__init__(base_folder, file_deletion_method, cleaner_threads, deletion_threads)
        +close_logStatus()
        +logStatus() str
        +start_workers(no_folder_scanner_threads, no_deletions_threads)
        +stop_workers()
    }

    BaseSimulation <|-- Simulation
    clean_folder_type <|-- clean_all_pr_ext
    clean_folder_type <|-- clean_all_but_one_pr_ext
    clean_parameters <|-- clean_parameters_start_stop
    
    SimulationStatus --> LogInfoElement
    SimulationStatus --> FolderLogInfo
    Simulation --> FolderType
    Simulation --> SimulationStatus
    Simulation --> clean_folder_type
    Simulation --> clean_all_pr_ext
    Simulation --> clean_all_but_one_pr_ext
    clean_parameters --> ThreadSafeCounter
    clean_parameters --> ThreadSafeDeletionCounter
    clean_parameters --> FileDeletionMethod
```

## 2. Sequence Diagram - Main Cleanup Flow

```mermaid
sequenceDiagram
    participant Main as main()
    participant Reader as read_simulation_folders_from_file()
    participant Params as clean_parameters_start_stop
    participant Cleaner as clean_simulations()
    participant Monitor as monitor()
    participant Workers as start_workers()
    participant Scanner as folder_scanner()
    participant Sim as Simulation
    participant Deleter as delete_files()

    Main->>Reader: Read simulation folders from CSV
    Reader-->>Main: List of simulation folders
    Main->>Params: Create parameters object
    Main->>Cleaner: clean_simulations(folders, params)
    
    Cleaner->>Params: Initialize queues
    Cleaner->>+Workers: start_workers(scanner_threads, deletion_threads)
    Workers->>Scanner: Start folder scanner threads
    Workers->>Deleter: Start file deletion threads
    Workers->>Monitor: Start monitor thread
    Workers-->>-Cleaner: Workers started
    
    loop For each simulation folder
        Scanner->>Sim: Create Simulation object
        Sim->>Sim: get_base_entries()
        Sim->>Sim: get_standard_folders()
        Sim->>Sim: getFolderType()
        
        alt StandardSimulation
            Sim->>Sim: get_all_entries()
            Sim->>Sim: getSetNames()
            Sim->>Sim: is_reproducible()
            Sim->>Sim: get_cleaner_files()
            Sim-->>Scanner: files_to_delete
            Scanner->>Params: Put files in deletion queue
        else NormalFolder/PartialSimulation
            Sim-->>Scanner: Scan subfolders
            Scanner->>Params: Queue subfolders
        end
    end
    
    loop While files in deletion queue
        Deleter->>Deleter: Get file from queue
        Deleter->>IO: Delete file (if Delete mode)
        Deleter->>Params: Update deletion measures
    end
    
    Monitor->>Params: logStatus() every 5 seconds
    
    Cleaner->>Params: stop_workers()
    Params->>Workers: Send stop signals
    Workers->>Scanner: Stop scanner threads
    Workers->>Deleter: Stop deletion threads
    Workers-->>Params: All workers stopped
    
    Cleaner-->>Main: Return statistics
    Main->>Params: close_logStatus()
```

## 3. Flow Diagram - Simulation Evaluation Process

```mermaid
flowchart TD
    Start([Start: Process Simulation]) --> CreateSim[Create Simulation Object]
    CreateSim --> GetBase[Get Base Entries via scandir]
    GetBase --> GetStd[Get Standard Folders]
    GetStd --> DetType{Determine Folder Type}
    
    DetType -->|Missing/Error| Missing[FolderType: MissingFolder]
    DetType -->|≤1 standard folder| Normal[FolderType: NormalFolder]
    DetType -->|Has INPUTS + some| Partial[FolderType: StandardSimulation_partial]
    DetType -->|Missing INPUTS| PartialSim[FolderType: PartialSimulation]
    DetType -->|All standard folders| Standard[FolderType: StandardSimulation]
    
    Missing --> LogError[Log to folder_error_queue]
    Normal --> ScanSub[Scan Subfolders]
    PartialSim --> ScanSub
    
    Standard --> ScanAll[Scan All Entries Recursively]
    Partial --> ScanAll
    
    ScanAll --> GetSet[Get .set Files from INPUTS]
    GetSet --> ParseSet[Parse Set Names]
    ParseSet --> CheckValid{Valid Set Names?}
    
    CheckValid -->|No| NotReproducible[Mark as Irreproducible]
    CheckValid -->|Yes| GetTimes[Get Max INPUTS Time & Min Output Time]
    
    GetTimes --> CompTimes{Max INPUTS < Min Output?}
    CompTimes -->|Yes| Reproducible[Mark as Reproducible]
    CompTimes -->|No| NotReproducible
    
    Reproducible --> CheckExcl{Check Exclusions}
    NotReproducible --> CheckExcl
    
    CheckExcl -->|Excluded| Ignore[Mark as Ignored]
    CheckExcl -->|Not Excluded| GetCleanFiles[Get Files to Clean]
    
    GetCleanFiles --> CheckMode{File Deletion Mode?}
    CheckMode -->|Analyse| LogOnly[Log cleanable files/bytes]
    CheckMode -->|Delete & Can Clean| QueueDel[Queue files for deletion]
    
    LogOnly --> LogSim[Write to scan_log_queue]
    QueueDel --> LogSim
    Ignore --> LogSim
    ScanSub --> LogSim
    
    LogSim --> End([End])
```

## 4. State Diagram - Simulation Processing States

```mermaid
stateDiagram-v2
    [*] --> Discovered: Folder added to queue
    
    Discovered --> Scanning: Scanner thread picks up folder
    
    Scanning --> TypeDetermined: scandir() completed
    
    TypeDetermined --> Missing: Missing/Error
    TypeDetermined --> Normal: Normal folder
    TypeDetermined --> PartialSim: Partial simulation
    TypeDetermined --> StandardSim: Standard simulation
    TypeDetermined --> StandardPartial: Standard partial
    
    Missing --> Logged: Log error
    
    Normal --> SubfolderQueued: Queue child folders
    PartialSim --> SubfolderQueued: Queue child folders
    
    StandardSim --> Analyzing: Deep scan entries
    StandardPartial --> Analyzing: Deep scan entries
    
    Analyzing --> SetValidation: Read .set files
    
    SetValidation --> ReproducibilityCheck: Valid set names
    SetValidation --> Irreproducible: Invalid set names
    
    ReproducibilityCheck --> Reproducible: INPUTS older than outputs
    ReproducibilityCheck --> Irreproducible: INPUTS newer than outputs
    
    Reproducible --> ExclusionCheck
    Irreproducible --> ExclusionCheck
    
    ExclusionCheck --> Excluded: Matches exclusion regex
    ExclusionCheck --> CanClean: No exclusions
    
    CanClean --> AnalyseMode: file_deletion_method = Analyse
    CanClean --> DeleteMode: file_deletion_method = Delete
    
    AnalyseMode --> Logged: Log metrics only
    DeleteMode --> FilesQueued: Queue files for deletion
    
    FilesQueued --> Logged: After queueing
    Excluded --> Logged: Log as ignored
    SubfolderQueued --> Logged
    
    Logged --> [*]
    
    state FilesQueued {
        [*] --> WaitingInQueue
        WaitingInQueue --> Deleting: Deletion thread picks file
        Deleting --> Deleted: File removed
        Deleted --> LoggedDeletion: Update counters
        LoggedDeletion --> [*]
    }
```

## 5. Entity Relationship Diagram - Data Model

```mermaid
erDiagram
    SIMULATION ||--o{ FILE_ENTRY : contains
    SIMULATION ||--|| SIMULATION_STATUS : has
    SIMULATION ||--o{ STANDARD_FOLDER : has
    SIMULATION ||--o{ SET_FILE : has
    SIMULATION ||--o{ SET_NAME : defines
    SIMULATION ||--o{ CLEANER_FILE : identifies
    
    SIMULATION {
        string base_path PK
        enum folder_type
        int sim_bytes
        int sim_count_files
        datetime sim_max_datetime
        bool is_reproducible
        bool can_clean
    }
    
    FILE_ENTRY {
        string path PK
        string name
        int size
        datetime mtime
        bool is_dir
    }
    
    STANDARD_FOLDER {
        string name PK
        string type
    }
    
    SET_FILE {
        string path PK
        string name
    }
    
    SET_NAME {
        string name PK
        int index
    }
    
    SIMULATION_STATUS {
        string folder PK
        string folder_type
        int folder_files
        int folder_bytes
        datetime folder_max_date
        string prepper
        int setname_count
        bool reproducibility
        bool exclusion_status
        string cleaning_status
        int cleanable_files
        int cleanable_bytes
    }
    
    CLEANER_FILE {
        string file_path PK
        string extension
        string cleaner_key
        int size
    }
    
    CLEAN_PARAMETERS ||--o{ QUEUE : manages
    CLEAN_PARAMETERS ||--o{ COUNTER : tracks
    
    CLEAN_PARAMETERS {
        enum file_deletion_method
        datetime min_date
        datetime max_date
        regex folder_exclusions
        set vts_standard_folders
    }
    
    QUEUE {
        string name PK
        string type
        int size
    }
    
    COUNTER {
        string name PK
        int value
    }
    
    CLEANER_TYPE ||--o{ CLEANER_FILE : generates
    
    CLEANER_TYPE {
        string key PK
        list local_folder_names
        tuple extensions
        string type
    }
    
    SIMULATION ||--|| FOLDER_TYPE : classifies
    
    FOLDER_TYPE {
        string value PK
        string description
    }
```

## 6. Component/Architecture Diagram

```mermaid
flowchart TB
    subgraph Entry["Entry Point"]
        Main[clean_simulations_from_file.py]
    end
    
    subgraph Core["Core Processing"]
        Cleaner[cleaner.py<br/>clean_simulations<br/>clean_parameters_start_stop]
        Scanner[clean_folder_scanner.py<br/>folder_scanner<br/>clean_simulation]
        Sim[clean_simulation.py<br/>Simulation class]
    end
    
    subgraph CleanerTypes["Cleaner Implementations"]
        FolderType[clean_folder_type.py<br/>BaseSimulation<br/>clean_folder_type]
        AllPrExt[clean_all_pr_ext.py<br/>clean_all_pr_ext]
        AllButOne[clean_all_but_one_pr_ext.py<br/>clean_all_but_one_pr_ext]
    end
    
    subgraph Config["Configuration & Parameters"]
        Params[clean_parameters.py<br/>clean_parameters]
    end
    
    subgraph Workers["Worker Tasks"]
        Writers[clean_writer_tasks.py<br/>Deletion Workers<br/>Logger Workers<br/>ThreadSafe Counters]
    end
    
    subgraph IO["I/O Layer"]
        RobustIO[RobustIO.py<br/>IO class]
    end
    
    subgraph DataStructures["Data Structures"]
        Enums[FileDeletionMethod<br/>FolderType<br/>FolderLogInfo]
        Status[SimulationStatus<br/>LogInfoElement]
        Counters[ThreadSafeCounter<br/>ThreadSafeDeletionCounter]
    end
    
    Main --> Cleaner
    Cleaner --> Params
    Cleaner --> Scanner
    Cleaner --> Workers
    
    Scanner --> Sim
    Sim --> FolderType
    Sim --> AllPrExt
    Sim --> AllButOne
    Sim --> RobustIO
    
    AllPrExt -.inherits.-> FolderType
    AllButOne -.inherits.-> FolderType
    Sim -.inherits.-> FolderType
    
    Workers --> RobustIO
    Workers --> Counters
    Workers --> Status
    
    Params --> Enums
    Params --> Counters
    
    Sim --> Status
    Sim --> Enums
    
    Status --> Enums
```

## 7. Activity Diagram - Worker Thread Coordination

```mermaid
flowchart TD
    Start([Start Workers]) --> CreateQueues[Create Queues:<br/>- folder_scan_queue<br/>- file_deletion_queue<br/>- scan_log_queue<br/>- file_error_queue<br/>- folder_error_queue<br/>- deletion_log_queue]
    
    CreateQueues --> CreateCounters[Create Thread-Safe Counters]
    
    CreateCounters --> SpawnThreads{Spawn Thread Pools}
    
    SpawnThreads --> FolderScanners[Folder Scanner Threads<br/>32-128 threads]
    SpawnThreads --> FileDeleters[File Deletion Threads<br/>1-1024 threads]
    SpawnThreads --> LogWriters[Logger Threads<br/>4 threads]
    SpawnThreads --> Monitor[Monitor Thread<br/>1 thread]
    
    FolderScanners --> FSLoop{Queue Empty?}
    FSLoop -->|No| GetFolder[Get Folder from Queue]
    FSLoop -->|Yes, Signal None| FSEnd[Thread Exits]
    
    GetFolder --> ProcessFolder[Process Folder:<br/>- Create Simulation<br/>- Evaluate<br/>- Get Files to Clean]
    ProcessFolder --> QueueFiles{Files to Delete?}
    QueueFiles -->|Yes| PutFiles[Put Files in file_deletion_queue]
    QueueFiles -->|No| LogSim[Put Status in scan_log_queue]
    PutFiles --> LogSim
    LogSim --> QueueSub{Subfolders?}
    QueueSub -->|Yes| PutSubfolders[Put Subfolders in folder_scan_queue]
    QueueSub -->|No| FSLoop
    PutSubfolders --> FSLoop
    
    FileDeleters --> FDLoop{Queue Empty?}
    FDLoop -->|No| GetFile[Get File from Queue]
    FDLoop -->|Yes, Signal None| FDEnd[Thread Exits]
    
    GetFile --> DeleteFile{Deletion Mode?}
    DeleteFile -->|Delete| ActualDelete[os.remove file]
    DeleteFile -->|Analyse| SkipDelete[Skip deletion]
    ActualDelete --> UpdateCounter[Update ThreadSafeDeletionCounter]
    SkipDelete --> UpdateCounter
    UpdateCounter --> LogDeletion[Put path in deletion_log_queue]
    LogDeletion --> FDLoop
    
    LogWriters --> LWLoop{Queue Empty?}
    LWLoop -->|No| GetLog[Get Log Entry]
    LWLoop -->|Yes, Signal None| LWEnd[Thread Exits]
    GetLog --> WriteLog[Write to CSV File]
    WriteLog --> LWLoop
    
    Monitor --> Sleep[Sleep 5 seconds]
    Sleep --> CheckStop{Stop Event Set?}
    CheckStop -->|No| PrintStatus[Print Status:<br/>- Folders processed<br/>- Simulations cleaned<br/>- Files/Bytes deleted]
    CheckStop -->|Yes| MEnd[Thread Exits]
    PrintStatus --> Sleep
    
    FSEnd --> CheckAllFS{All Folder<br/>Scanners Done?}
    CheckAllFS -->|Yes| SignalFD[Send None to<br/>file_deletion_queue]
    CheckAllFS -->|No| WaitFS[Wait]
    WaitFS --> CheckAllFS
    
    FDEnd --> CheckAllFD{All File<br/>Deleters Done?}
    CheckAllFD -->|Yes| SignalLW[Send None to<br/>all log queues]
    CheckAllFD -->|No| WaitFD[Wait]
    WaitFD --> CheckAllFD
    
    LWEnd --> CheckAllLW{All Loggers<br/>Done?}
    CheckAllLW -->|Yes| SetStop[Set stop_event]
    CheckAllLW -->|No| WaitLW[Wait]
    WaitLW --> CheckAllLW
    
    SetStop --> MEnd
    MEnd --> End([All Workers Stopped])
    SignalFD --> FDLoop
    SignalLW --> LWLoop
```

## Summary

These diagrams provide comprehensive views of the VTS simulation cleanup application:

1. **Class Diagram**: Shows all classes, their attributes, methods, and relationships
2. **Sequence Diagram**: Illustrates the main cleanup flow from start to finish
3. **Flow Diagram**: Details the simulation evaluation decision process
4. **State Diagram**: Shows state transitions during simulation processing
5. **Entity Relationship Diagram**: Models the data structures and their relationships
6. **Component/Architecture Diagram**: Shows high-level module organization
7. **Activity Diagram**: Details worker thread coordination and parallel processing

The application is a multi-threaded system that:
- Scans simulation folders recursively
- Evaluates reproducibility based on file timestamps
- Identifies cleanable files using configurable cleaner strategies
- Can either analyze (count) or delete files
- Tracks progress with thread-safe counters
- Logs all operations to CSV files
