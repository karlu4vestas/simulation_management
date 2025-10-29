import os
from datetime import date
from cleanup_cycle.cleanup_dtos import ActionType 
from cleanup_cycle.cleanup_scheduler import AgentInterfaceMethods
from cleanup_cycle.internal_agents import AgentTemplate
from datamodel.dtos import ExternalRetentionTypes, FileInfo, FolderTypeEnum

class AgentCleanRootFolder(AgentTemplate):

    def __init__(self):
        super().__init__("AgentCleanRootFolder", [ActionType.CLEAN_ROOTFOLDER.value])

    def execute_task(self):
        simulations:  list[str] = AgentInterfaceMethods.read_simulations_marked_for_cleanup(self.task.id, self.task.rootfolder_id)    
        
        clean_simulations_result: tuple[bool, str] = self.clean_simulations(simulations)
        if not clean_simulations_result[0]:
            self.error_message = f"Failed to clean simulations: {clean_simulations_result[1]}"
            return


    def clean_simulations(self, simulations:  list[str]) -> tuple[bool, str]:
        pass