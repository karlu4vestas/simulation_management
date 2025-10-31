# Main entry point for cleanup operations.
# Coordinates multi-threaded cleanup of VTS simulations.

import os
from datetime import datetime
from typing import NamedTuple
from threading import Thread
from datamodel.dtos import FileInfo, FolderTypeEnum, FileInfo
from cleanup_cycle.clean_agent.clean_parameters import CleanMeasures, CleanParameters, CleanMode
from cleanup_cycle.clean_agent.clean_progress_reporter import CleanProgressReporter 
from cleanup_cycle.clean_agent.clean_workers import (
    simulation_worker,
    deletion_worker,
    progress_monitor_worker,
    error_writer_worker
)


class CleanMainResult(NamedTuple):
    # Return value from clean_main containing results and measures
    results: list[FileInfo]  # List of processed simulations with status
    measures: CleanMeasures  # Summary statistics from cleanup operation


def clean_main(
    simulations: list[FileInfo],
    progress_reporter: CleanProgressReporter,
    output_path: str,
    clean_mode: CleanMode = CleanMode.ANALYSE,
    num_sim_workers: int = 32,
    num_deletion_workers: int = 2,
    deletion_queue_max_size: int = 1_000_000
) -> CleanMainResult:
    # Clean VTS simulations with multi-threading as follows
    # 1. Sets up queues and counters
    # 2. Spawns worker threads:
    #    - Simulation workers: Process simulations and identify files to clean
    #    - Deletion workers: Delete or analyze files
    #    - Progress monitor: Report progress periodically
    #    - Error writer: Log errors to file
    # 3. Waits for all work to complete
    # 4. Returns results as list of FileInfo
    # Args:
    #     simulations: List of FileInfo with path and modified_date from database
    #     progress_reporter: Progress reporter (required, must be opened by caller before calling)
    #     output_path: Path for error logs (required)
    #     clean_mode: ANALYSE (count only) or DELETE (actually delete)
    #     num_sim_workers: Number of simulation processing threads (default: 32)
    #     num_deletion_workers: Number of file deletion threads (default: 2 for ANALYSE, can use more for DELETE)
    #     deletion_queue_max_size: Max size of deletion queue (default: 1,000,000)
    # Returns:
    #     CleanMainResult containing:
    #       - results: List of FileInfo with simulation results:
    #           - filepath: Simulation path
    #           - modified_date: Last modification date
    #           - nodetype: FolderTypeEnum.VTS_SIMULATION
    #           - external_retention: Clean/Issue/UNDEFINED status
    #       - measures: CleanMeasures with summary statistics:
    #           - simulations_processed, simulations_cleaned, simulations_issue, simulations_skipped
    #           - files_deleted, bytes_deleted, error_count
    # Example:
    #     >>> from datetime import datetime
    #     >>> from cleanup_cycle.clean_agent.clean_progress_reporter import CleanProgressWriter
    #     >>> sims = [
    #     ...     FileInfo("//server/sim1", datetime(2024, 1, 15)),
    #     ...     FileInfo("//server/sim2", datetime(2024, 2, 20))
    #     ... ]
    #     >>> reporter = CleanProgressWriter()
    #     >>> reporter.open("./logs")
    #     >>> try:
    #     ...     result = clean_main(
    #     ...         simulations=sims,
    #     ...         progress_reporter=reporter,
    #     ...         output_path="./clean_logs",
    #     ...         clean_mode=CleanMode.ANALYSE,
    #     ...         num_sim_workers=4
    #     ...     )
    #     ...     print(f"Processed {len(result.results)} simulations")
    #     ...     print(f"Total processed: {result.measures.simulations_processed}")
    #     >>> finally:
    #     ...     reporter.close()
    
    # Create parameters
    params = CleanParameters(
        clean_mode=clean_mode,
        deletion_queue_max_size=deletion_queue_max_size
    )
    
    # Load simulation queue
    for sim in simulations:
        params.simulation_queue.put(sim)

    # Start worker threads
    simulation_threads = []
    
    # Simulation workers
    for i in range(num_sim_workers):
        t = Thread(
            target=simulation_worker,
            args=(params,),
            name=f"SimWorker-{i}",
            daemon=True
        )
        t.start()
        simulation_threads.append(t)
    
    # Deletion workers
    deletion_threads = []
    for i in range(num_deletion_workers):
        t = Thread(
            target=deletion_worker,
            args=(params,),
            name=f"DelWorker-{i}",
            daemon=True
        )
        t.start()
        deletion_threads.append(t)
    
    # Progress monitor
    monitor = Thread(
        target=progress_monitor_worker,
        args=(params, progress_reporter),
        name="ProgressMonitor",
        daemon=True
    )
    monitor.start()
    
    # Error writer
    error_log = os.path.join(output_path, "clean_errors.csv")
    error_thread = Thread(
        target=error_writer_worker,
        args=(params, error_log),
        name="ErrorWriter",
        daemon=True
    )
    error_thread.start()
    
    try:
        # Wait for simulation queue to complete
        params.simulation_queue.join()
        print("\nAll simulations processed. Waiting for file deletions...")
        
        # Send poison pills to simulation workers
        for _ in range(num_sim_workers):
            params.simulation_queue.put(None)
        
        # Wait for deletion queue to complete
        params.file_deletion_queue.join()
        
        # Send poison pills to deletion workers
        for _ in range(num_deletion_workers):
            params.file_deletion_queue.put(None)
        
        # Stop monitoring and error writer
        params.stop_event.set()
        params.error_queue.put((None, None))
        
        # Wait for all threads
        for t in simulation_threads:
            t.join(timeout=25)
        for t in deletion_threads:
            t.join(timeout=25)
        monitor.join(timeout=25)
        error_thread.join(timeout=25)

    except KeyboardInterrupt:
        params.stop_event.set()
        # Send poison pills
        for _ in range(num_sim_workers):
            try:
                params.simulation_queue.put(None)
            except:
                pass
        for _ in range(num_deletion_workers):
            try:
                params.file_deletion_queue.put(None)
            except:
                pass
        params.error_queue.put((None, None))
    
    # Collect results
    deletion_results = []
    while not params.processed_simulations_result_queue.empty():
        try:
            # Queue now contains FileInfo objects directly (not Simulation objects)
            file_info = params.processed_simulations_result_queue.get_nowait()
            deletion_results.append(file_info)
        except:
            break
    
    # Get final statistics and return both results and measures
    measures: CleanMeasures = params.get_measures()
    return CleanMainResult(results=deletion_results, measures=measures)
