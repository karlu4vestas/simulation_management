import os
import time as time_module
import tempfile
from datetime import datetime
from cleanup_cycle.scheduler_dtos import ActionType 
from cleanup_cycle.agent_task_manager import CleanupTaskManager 
from cleanup_cycle.agents_internal import AgentTemplate
from datamodel.dtos import FileInfo
from cleanup_cycle.clean_agent.clean_main import clean_main, CleanupResult
from cleanup_cycle.clean_agent.clean_progress_reporter import CleanProgressReporter, CleanProgressWriter
from cleanup_cycle.clean_agent.clean_parameters import CleanMeasures, CleanMode

def as_date_time(timestamp): return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d_%H-%M-%S')

# The purpose of this class is to reuse the CleanProgressReporter to report progress to the task
class AgentCleanProgressWriter(CleanProgressWriter):
    def __init__(self, agentCleanRootFolder: "AgentCleanVTSRootFolder", seconds_between_update: int, seconds_between_filelog: int):
        self.agentCleanRootFolder = agentCleanRootFolder
        self.seconds_between_update = seconds_between_update
        self.seconds_between_filelog = seconds_between_filelog

    def update(self, measures: CleanMeasures, deletion_queue_size: int, active_threads: int):
        # Report real-time progress to the task
        msg: str = (
            f"\rProcessed: {measures.simulations_processed}; "
            f"Cleaned: {measures.simulations_cleaned}; "
            f"Issue: {measures.simulations_issue}; "
            f"Skipped: {measures.simulations_skipped}; "
            f"Queue: {deletion_queue_size}; "
            f"Threads: {active_threads}"
        )
        CleanupTaskManager.task_progress(self.agentCleanRootFolder.task.id, msg)

    def open(self, output_path: str):
        super().open(output_path)
        
    def close(self):
        super().close()


class AgentCleanVTSRootFolder(AgentTemplate):
    temporary_result_folder: str | None

    def __init__(self):
        super().__init__("AgentCleanVTSRootFolder", [ActionType.CLEAN_ROOTFOLDER.value], supported_storage_ids=["local"])

        # Initialize error_message
        self.error_message: str | None = None
        
        # Get temporary result folder for clean logs
        self.temporary_result_folder: str = os.getenv('CLEAN_TEMP_FOLDER', tempfile.gettempdir())
        if len(self.temporary_result_folder) == 0: # or not os.path.exists(self.temporary_result_folder):
            self.error_message = f"CLEAN_TEMP_FOLDER environment variable is not set or the path does not exist: {self.temporary_result_folder}"
            self.temporary_result_folder = None
        else:
            try:
                os.makedirs(self.temporary_result_folder, exist_ok=True)
                if not os.path.exists(self.temporary_result_folder):
                    self.error_message = f"Failed to create temporary result folder for scans: {self.temporary_result_folder}"
                    self.temporary_result_folder = None
            except (PermissionError, OSError) as e:
                self.error_message = f"Failed to create temporary result folder for scans: {self.temporary_result_folder}. Error: {str(e)}"
                self.temporary_result_folder = None
       
        self.nb_clean_sim_workers: int = int(os.getenv('CLEAN_SIM_WORKERS', 32))
        self.nb_clean_deletion_workers: int = int(os.getenv('CLEAN_DELETION_WORKERS', 2))
        self.clean_mode_str: str = os.getenv('CLEAN_MODE', 'ANALYSE')  # ANALYSE or DELETE
        
        # Convert clean mode string to enum
        try:
            self.clean_mode: CleanMode = CleanMode[self.clean_mode_str.upper()]
        except KeyError:
            self.error_message = f"Invalid CLEAN_MODE: {self.clean_mode_str}. Must be ANALYSE or DELETE"
            self.clean_mode = CleanMode.ANALYSE

    def execute_task(self):
        if self.error_message is not None:
            return
        if self.temporary_result_folder is None:
            self.error_message = "Temporary result folder is not set"
            return
            
        simulations: list[FileInfo] = CleanupTaskManager.task_read_folders_marked_for_cleanup(self.task.id)
        CleanupTaskManager.task_progress(self.task.id, f"Starting cleanup of {len(simulations)} simulations in mode: {self.clean_mode.value}")
        
        clean_result: CleanupResult = None
        if len(simulations) > 0:
            clean_result: CleanupResult = self.clean_simulations(simulations)
            # Report summary
            measures = clean_result.measures
            CleanupTaskManager.task_progress( self.task.id,
                f"Cleanup completed: {measures.simulations_processed} processed, "
                f"{measures.simulations_cleaned} cleaned, {measures.simulations_issue} issues, "
                f"{measures.simulations_skipped} skipped"
            )
            # Update simulations in database with cleanup results
            if len(clean_result.results) > 0:
                result: dict[str, str] = CleanupTaskManager.task_insert_or_update_simulations_in_db( self.task.id, clean_result.results)

                CleanupTaskManager.task_progress( self.task.id, f"Insertion completed: {len(clean_result.results)} simulations inserted" )
        self.success_message = f"Cleaned {len(simulations)} simulations in rootfolder {self.task.rootfolder_id}"

    def clean_simulations(self, simulations: list[FileInfo]) -> CleanupResult | None:
        
        # Create output path for logs
        root_folder_name: str = os.path.basename(self.task.path)
        date_time_str: str    = as_date_time(time_module.time())
        output_path: str      = os.path.join(self.temporary_result_folder, f"{date_time_str}_{root_folder_name}_clean_logs")

        # Create progress reporter
        progress_reporter: CleanProgressReporter = AgentCleanProgressWriter( self, seconds_between_update=10, seconds_between_filelog=60 )
        progress_reporter.open(output_path)

        clean_result: CleanupResult = None
        try:
            clean_result = clean_main( simulations          = simulations,
                                       progress_reporter    = progress_reporter,
                                       output_path          = output_path,
                                       clean_mode           = self.clean_mode,
                                       num_sim_workers      = self.nb_clean_sim_workers,
                                       num_deletion_workers = self.nb_clean_deletion_workers )
        except Exception as e:
            self.error_message = f"Failed to clean simulations: {str(e)}"
            clean_result = None
        finally:
            progress_reporter.close()
        
        return clean_result
