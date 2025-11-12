# Worker thread implementations for cleanup operations.
import os
import sys
import time
import threading
from queue import Empty
from cleanup_cycle.clean_agent.clean_parameters import CleanParameters, CleanMode
from cleanup_cycle.clean_agent.clean_progress_reporter import CleanProgressReporter
from cleanup_cycle.clean_agent.simulation import Simulation, SimulationEvalResult
from cleanup_cycle.clean_agent.simulation_file_registry import SimulationFileRegistry
from datamodel.retentions import ExternalRetentionTypes


def simulation_worker(params: CleanParameters):
    # Process simulations from queue as follows:
    # 1. Gets simulation data (FileInfo) from queue
    # 2. Creates Simulation object with path and modified_date
    # 3. Gets files to clean
    # 4. Queues files for deletion
    # 5. Updates counters    
    # Args:
    #     params: CleanParameters object with queues and counters
    
    while not params.stop_event.is_set():
        sim_input = None
        try:
            # Get simulation input from queue with timeout
            sim_input = params.simulation_queue.get(timeout=1)
            
            # Check for poison pill
            if sim_input is None:
                params.simulation_queue.task_done()
                break
            
            # Create simulation object with filepath and modified_date from FileInfo
            file_registry: SimulationFileRegistry = SimulationFileRegistry(sim_input.filepath, params.error_queue)
            sim: Simulation = Simulation(sim_input.filepath, sim_input.modified_date, params.error_queue, file_registry)

            # Get files to clean
            eval : SimulationEvalResult = sim.eval()

            # Queue files for deletion
            for file_path in eval.all_cleaners_files:
                params.file_deletion_queue.put(file_path)
            
            # Put lightweight FileInfo result in result queue instead of full Simulation
            # This avoids memory issues from keeping large file structures in memory
            params.processed_simulations_result_queue.put(eval.file_info)
            
            # Update appropriate counter based on simulation status
            if len(eval.all_cleaners_files) > 0:
                params.simulations_cleaned.increment()
            elif eval.file_info.external_retention == ExternalRetentionTypes.ISSUE:
                params.simulations_issue.increment()
            else:
                params.simulations_skipped.increment()
            
            params.simulations_processed.increment()
            
        except Empty:
            # Timeout - check stop_event and continue
            continue
        except Exception as e:
            # Log error and continue processing
            sim_path = sim_input.filepath if sim_input else "unknown"
            error_msg = f"simulation_worker error: {str(e)}"
            params.error_queue.put((sim_path, error_msg))
            params.error_count.increment()
            if sim_input:
                params.simulations_issue.increment()
                params.simulations_processed.increment()
        finally:
            if sim_input is not None:
                params.simulation_queue.task_done()


def deletion_worker(params: CleanParameters):
    # Delete or analyze files from queue as follows:
    # 1. Gets file path from deletion queue
    # 2. If DELETE mode: deletes the file
    # 3. If ANALYSE mode: just gets file size
    # 4. Updates deletion counters    
    # Args:
    #     params: CleanParameters object with queues and counters

    while not params.stop_event.is_set():
        file_path = None
        try:
            # Get file from queue with timeout
            timeout=None
            file_path = params.file_deletion_queue.get(timeout=timeout)

            # Check for poison pill
            if file_path is None:
                params.file_deletion_queue.task_done()
                break
            
            file_size = 0
            
            # Get file size
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                
                # Delete file if in DELETE mode
                if params.clean_mode == CleanMode.DELETE:
                    os.remove(file_path)
                
                # Update deletion measures
                params.deletion_measures.add(1, file_size)
            else:
                # File doesn't exist - log as error
                params.error_queue.put((file_path, "File not found"))
                params.error_count.increment()
            
        except Empty:
            # Timeout - check stop_event and continue
            continue
        except Exception as e:
            # Log error and continue processing
            error_msg = f"deletion_worker error: {str(e)}"
            params.error_queue.put((file_path or "unknown", error_msg))
            params.error_count.increment()
        finally:
            if file_path is not None:
                params.file_deletion_queue.task_done()

def error_writer_worker(params: CleanParameters, error_log_path: str):
    # Write errors to log file as follows:    
    # 1. Gets errors from error queue
    # 2. Writes to CSV log file    
    # Args:
    #     params: CleanParameters object with error queue
    #     error_log_path: Path to error log file

    try:
        with open(error_log_path, 'w', encoding='utf-8') as f:
            f.write("path;error\n")
            
            while not params.stop_event.is_set():
                path, error = None, None
                try:
                    # Get error from queue with timeout
                    timeout=None
                    path, error = params.error_queue.get()
                    
                    # Check for poison pill
                    if path is None:
                        params.error_queue.task_done()
                        break
                    
                    # Write error to file
                    f.write(f'"{path}";"{error}"\n')
                    f.flush()
                    
                except Empty:
                    # Timeout - check stop_event and continue
                    continue
                except Exception as e:
                    # Don't let logging errors stop the worker
                    print(f"Error writer error: {str(e)}", file=sys.stderr)
                finally:
                    if path is not None:
                        params.error_queue.task_done()
    except Exception as e:
        print(f"Error opening error log file: {str(e)}", file=sys.stderr)


def progress_monitor_worker(params: CleanParameters, 
                            progress_reporter: CleanProgressReporter):
    # Monitor and report progress periodically as follows:
    # 1. Sleeps for configured interval
    # 2. Reads current measures from params
    # 3. Calls progress_reporter.update()
    # Args:
    #     params: CleanParameters object with counters
    #     progress_reporter: Progress reporter for output

    while not params.stop_event.is_set():
        time.sleep(progress_reporter.seconds_between_update)
        
        # Skip if stop event is set
        if params.stop_event.is_set():
            break
        
        try:
            # Get current measures and report progress
            measures = params.get_measures()
            progress_reporter.update(
                measures=measures,
                deletion_queue_size=params.file_deletion_queue.qsize(),
                active_threads=threading.active_count()
            )
        except Exception as e:
            # Don't let progress monitoring errors stop the worker
            print(f"Progress monitor error: {str(e)}", file=sys.stderr)