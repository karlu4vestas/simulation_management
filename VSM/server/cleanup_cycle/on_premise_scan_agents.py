import os
from datetime import date
from cleanup_cycle.cleanup_dtos import ActionType 
from server.cleanup_cycle.cleanup_scheduler import AgentInterfaceMethods
from server.cleanup_cycle.internal_agents import AgentTemplate
from datamodel.dtos import ExternalRetentionTypes, FileInfo, FolderTypeEnum

class AgentScanRootFolder(AgentTemplate):
    temporary_result_folder: str | None

    def __init__(self):
        super().__init__("AgentScanRootfolder", [ActionType.SCAN_ROOTFOLDER.value])
        self.temporary_result_folder: str = os.getenv('TEMPORARY_SCAN_RESULTS', '')  # where should the file and folder meta data be placed
        if len(self.temporary_result_folder) == 0 or not os.path.exists(self.temporary_result_folder):
            self.error_message = f"TEMPORARY_SCAN_RESULTS environment variable is not set or the path does not exist: {self.temporary_result_folder}"
            self.temporary_result_folder = None

    def execute_task(self):
        if self.temporary_result_folder is None:
            return

        root_folder_name: str = os.path.basename(self.task.path)
        date_time_str: str = date.today().strftime("%Y%m%d:%H%M%S")
        metadata_file: str = os.path.join(self.temporary_result_folder, date_time_str+"_"+root_folder_name+"_metadata.csv")

        success, msg = self.scan_metadata(self.task.path, self.temporary_result_folder)
        if not success:
            self.error_message = f"Failed to scan metadata: {msg}"
            return

        simulations: list[FileInfo] = self.extract_simulations(metadata_file)

        result: dict[str, str] = AgentInterfaceMethods.task_insert_or_update_simulations_in_db(self.task.id, self.task.rootfolder_id, simulations)
        self.success_message = f"CalendarCreation done: {result}"

class AgentCleanRootFolder(AgentTemplate):

    def __init__(self):
        super().__init__("AgentCleanRootFolder", [ActionType.CLEAN_ROOTFOLDER.value])

    def execute_task(self):
        simulations:  list[str] = AgentInterfaceMethods.read_simulations_marked_for_cleanup(self.task.id, self.task.rootfolder_id)    
        
        clean_simulations_result: tuple[bool, str] = self.clean_simulations(simulations)
        if not clean_simulations_result[0]:
            self.error_message = f"Failed to clean simulations: {clean_simulations_result[1]}"
            return


def scan_metadata(self, path: str, meta_file_path: str) -> tuple[bool, str]:
    success: bool = False
    msg: str = ""

    return success, msg 

def extract_simulations(self, meta_file_path: str) -> list[FileInfo]:
    vts_nodetype:       FolderTypeEnum = FolderTypeEnum.VTS_SIMULATION
    clean_retention:    ExternalRetentionTypes = ExternalRetentionTypes.Clean.value
    issue_retention:    ExternalRetentionTypes = ExternalRetentionTypes.Issue.value
    withdata_retention: ExternalRetentionTypes = ExternalRetentionTypes.Unknown.value
    pass


def clean_simulations(self, simulations:  list[str]) -> tuple[bool, str]:
    pass