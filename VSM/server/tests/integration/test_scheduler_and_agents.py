import os
import shutil
import pytest
from datetime import date
from dataclasses import dataclass
from datetime import timedelta

from sqlmodel import Session

from datamodel.retention_validators import ExternalToInternalRetentionTypeConverter, RetentionCalculator
from datamodel.dtos import FolderNodeDTO, FolderTypeEnum, Retention, RetentionTypeDTO, FolderRetention, RootFolderDTO, ExternalRetentionTypes

from app.web_api import run_scheduler_tasks
from db.db_api import change_retentions, get_cleanup_configuration_by_rootfolder_id, insert_or_update_simulations_in_db, normalize_path, read_folders_marked_for_cleanup, read_folders, read_retentiontypes_by_domain_id, read_folders_marked_for_cleanup, read_rootfolder_retentiontypes_dict
from db.db_api import read_retentiontypes_by_domain_id, read_folder_type_dict_pr_domain_id, read_simulation_domains, read_folder_types_pr_domain_id, read_cleanupfrequency_by_domain_id, read_cycle_time_by_domain_id   
from db.db_api import insert_rootfolder,insert_cleanup_configuration
from db.db_api import FileInfo
from cleanup_cycle.cleanup_db_actions import cleanup_cycle_prepare_next_cycle, cleanup_cycle_start, CleanupProgress
from cleanup_cycle.on_premise_scan_agent import AgentScanVTSRootFolder
from cleanup_cycle.on_premise_clean_agent import AgentCleanVTSRootFolder
from cleanup_cycle.cleanup_dtos import ActionType, CleanupConfigurationDTO, CleanupTaskDTO, TaskStatus
from cleanup_cycle.internal_agents import (
    AgentTemplate,
    AgentCalendarCreation,
    AgentCleanupCycleStart,
    AgentNotification,
    AgentCleanupCycleFinishing,
    AgentCleanupCyclePrepareNext
)
from cleanup_cycle.agent_runner import InternalAgentFactory
from cleanup_cycle.cleanup_scheduler import AgentInterfaceMethods, CleanupScheduler

from datamodel.vts_create_meta_data import insert_vts_metadata_in_db
from tests.generate_vts_simulations.GenerateTimeseries import SimulationType
from tests.generate_vts_simulations.main_GenerateSimulation import GeneratedSimulationsResult, SimulationTestSpecification, generate_simulations
from .testdata_for_import import InMemoryFolderNode, RootFolderWithMemoryFolders,CleanupConfiguration
from tests import test_storage



TEST_STORAGE_LOCATION = test_storage.LOCATION
    
# class LocalFileStorage:
#     def __init__(self, base_path: str):
#         self.base_path = base_path

#     def write_folder(self, folder_path: str):
#         full_path = os.path.join(self.base_path, folder_path.lstrip("/"))
#         os.makedirs(full_path, exist_ok=True)

#     def write_file(self, file_path: str, content: str = ""):
#         full_path = os.path.join(self.base_path, file_path.lstrip("/"))
#         dir_path = os.path.dirname(full_path)
#         os.makedirs(dir_path, exist_ok=True)
#         with open(full_path, 'w') as f:
#             f.write(content)
class ForTestAgentCleanupCyclePrepareNextAndStop(AgentTemplate):
    def __init__(self):
        super().__init__("TestAgentCleanupCyclePrepareNextAndStop", [ActionType.STOP_AFTER_CLEANUP_CYCLE.value])

    def reserve_task(self):
        self.task = AgentInterfaceMethods.reserve_task(self.agent_info)

    def execute_task(self):
        cleanup_cycle_prepare_next_cycle(self.task.rootfolder_id, prepare_next_cycle_and_stop=True)
        self.success_message = f"Next cleanup cycle prepared for rootfolder {self.task.rootfolder_id} but the Cleanup cycle is stopped here by setting cleanup_start_date=None"

class ForTestAgentCalendarCreation(AgentTemplate):
    # this ia a fake agent because it does not require a task and will always be run when called
    # In fact the agent calls the scheduler to create calendars and tasks for rootfolder that are ready to start cleanup cycles
    def __init__(self):
        super().__init__("AgentCalendarCreation", [ActionType.CREATE_CLEANUP_CALENDAR.value])

    def run(self):
        self.execute_task()

    def execute_task(self):
        msg: str = CleanupScheduler.create_calendars_for_cleanup_configuration_ready_to_start(stop_after_cleanup_cycle=True)
        self.success_message = f"CalendarCreation done with: {msg}"

@pytest.mark.integration
@pytest.mark.cleanup_workflow
@pytest.mark.slow
class TestSchedulerAndAgents:
    @staticmethod
    def import_rootfolder_and_cleanup_configuration(session:Session, rootfolder:RootFolderDTO, in_memory_config:CleanupConfiguration)-> tuple[RootFolderDTO, CleanupConfigurationDTO]:

        # Step 0: Set up a new database and verify that it is empty apart from VTS metadata
        simulation_domain_id = read_simulation_domains()[0].id
        assert simulation_domain_id is not None and simulation_domain_id > 0

        #save the rootfolder
        rootfolder.simulationdomain_id = simulation_domain_id
        rootfolder = insert_rootfolder(rootfolder)
        assert rootfolder is not None
        assert rootfolder.id is not None and rootfolder.id > 0
        assert rootfolder.path == rootfolder.path
        
        # Create CleanupConfigurationDTO from the in-memory CleanupConfiguration
        # The cleanup_scenario_data fixture uses the old CleanupConfiguration dataclass for in-memory setup
        # Now we create the corresponding CleanupConfigurationDTO database record
        cleanup_config:CleanupConfigurationDTO = in_memory_config.to_dto(rootfolder_id=rootfolder.id)
        cleanup_config:CleanupConfigurationDTO = insert_cleanup_configuration(rootfolder.id, cleanup_config)
        return rootfolder, cleanup_config
    
    def test_scheduler_and_agents_with_full_cleanup_round(self, integration_session, cleanup_scenario_data):
        # The purpose of this test is to test 
        # 1) the scheduling of tasks 
        # 2) that agent tasks are executed as expected
        # 3) that the results are as expected:
        #    - vts simulations are imported 
        #    - htc simulation are not imported
        #    - we will run with no active path protections so the retention will be numeric in the database
        #    - simulations are marked for cleanup and cleaned as expected and cleanup after the cleanup round is completed

        # Notice 
        #  1) that the condition to ensure that some simulations get marked for cleanup is that.
        #     The date of the CleanupConfigurationDTO' cleanup_start_date + cycletime is before the simulations modified date
        #     The easiest way to use modified date = today and cleanup_start_date = today-cycletime-1
        #  2) A cleanup_round required the passage of time equal to the cleanupfrequency to finalize the round. 
        #     That is we must either simulate or let time pass. cleanupfrequency was changed to float in order to allow a second to pass (that is 1/(24*60*60) of a day) in this way 
        #     we can let time pass in the test by running the scheduler and wait a second before next run of the scheduler

        # The plan is as follows:
        #   step 1: prepare the storage with simulations to be scanned
        #           a) create the necessary folder structure and files for a proper vts-simulation/htc-simulation We can do so
        #              - by using the cleanup_scenario_data fixture which contains one rootfolder with multiple leaf folders configured for cleanup.
        #              - extract the leaf folders 
        #              - use tests/generate_vts_simulation to create the simulation subfolder and files as seen in the integration test_cleanup_with_ondisk_simulations::test_cleanup_cycle_for_htc_simulation_ondisk
        #           b) we must set the files modified date so that we can simulate the passage of time => we must modify tests/generate_vts_simulation so that we can provide modified dates to be created
        #   setp 2: create the rootfolder and cleanup configuration in the database
        #   step 3: run the scheduler to create the scan tasks and execute the scan
        #   step 4: verify that all scan tasks are finalized
        #   step 5: verify that the simulations have been marked for cleanup as expected
        #   step 6: run the scheduler to create the cleanup rounds calendar of tasks
        #   step 7: run the scheduler to activate tasks that will execute agent
        #   step 8: verify that all tasks are finalized
        #   step 9: verify that the simulations have been cleaned up as expected


        #new db created by pytest_fixture so we only need to populate the metadata
        insert_vts_metadata_in_db(integration_session)

        #Get one rootfolders and it list of leaf folders
        rootfolder_data:RootFolderWithMemoryFolders = cleanup_scenario_data["first_rootfolder"]
        leaf_folders:list[InMemoryFolderNode] = [folder for folder in rootfolder_data.folders if folder.is_leaf]
        
        # setup folder for the test
        io_dir_for_storage_test: str = os.path.join(os.path.normpath(TEST_STORAGE_LOCATION),"test_integrationphase_5_scheduler_and_agents")
        #remove old test data
        if os.path.isdir(io_dir_for_storage_test):
            shutil.rmtree(io_dir_for_storage_test)

        # generate the simulations but before that all paths must be adjusted to point to the test storage location
        path_to_loadcases_configuration_file:str        = os.path.normpath("tests/generate_vts_simulations/loadcases/loadcases_ranges.json")
        rootfolder_storage_path: str = os.path.join( io_dir_for_storage_test, "rootfolder" )
        rootfolder_data.rootfolder.path = os.path.join( rootfolder_storage_path, rootfolder_data.rootfolder.path)
        simulation_folders: SimulationTestSpecification = [SimulationTestSpecification(os.path.join(rootfolder_storage_path, folder.path), SimulationType.VTS) for folder in leaf_folders]        
        gen_sim_results: GeneratedSimulationsResult     = generate_simulations(io_dir_for_storage_test, simulation_folders, path_to_loadcases_configuration_file)

        mem_cleanup_config: CleanupConfiguration = CleanupConfiguration( 
            cycletime=7,
            cleanupfrequency=1./(24*60*60),  # set to one second for the test
            cleanup_start_date=date.today() - timedelta(days=8),  #8 = cycletime+1 ensure that simulations are marked for cleanup
            cleanup_progress=CleanupProgress.ProgressEnum.INACTIVE
        )
        rootfolder:RootFolderDTO=None
        cleanup_config: CleanupConfigurationDTO=None
        rootfolder, cleanup_config = TestSchedulerAndAgents.import_rootfolder_and_cleanup_configuration(session=integration_session, rootfolder=rootfolder_data.rootfolder, in_memory_config=mem_cleanup_config)

        # Now we are ready for the test. We have:simuulations on desk and rootfolder with an inactive cleanup configuration in the db
        # Lets define the environment variables needed by the scheduler and agents

        # for the AgentScanVTSRootFolder
        os.environ['SCAN_TEMP_FOLDER'] = os.path.join(io_dir_for_storage_test, "temp_for_scanning")  # where should the meta data for file and folders be placed
        os.environ['SCAN_THREADS'] = str(1)  # number of scanning threads

        #for the cleanup agent
        os.environ['CLEAN_TEMP_FOLDER'] = os.path.join(io_dir_for_storage_test, "temp_for_cleaning")
        os.environ['CLEAN_SIM_WORKERS'] = str(1)
        os.environ['CLEAN_DELETION_WORKERS'] = str(2)
        os.environ['CLEAN_MODE'] = 'ANALYSE'

        # Create test-specific agents (they will pick up the environment variables set above)
        test_agents = [
            ForTestAgentCalendarCreation(),
            AgentScanVTSRootFolder(),       # Uses SCAN_TEMP_FOLDER, SCAN_THREADS env vars
            AgentCleanupCycleStart(),
            AgentNotification(),
            AgentCleanVTSRootFolder(),      # Uses CLEAN_TEMP_FOLDER, CLEAN_SIM_WORKERS, etc. env vars
            AgentCleanupCycleFinishing(),
            #AgentCleanupCyclePrepareNext(),
            ForTestAgentCleanupCyclePrepareNextAndStop()
        ]

        # Use context manager to inject test agents
        with InternalAgentFactory.with_agents(test_agents):
            # Step 3: Run scheduler to create scan tasks and execute the scan
            run_scheduler_tasks()
            # extract all task
            calendar, tasks = CleanupScheduler.extract_active_calendar_for_rootfolder(rootfolder)
            task_dict: dict[ActionType, CleanupTaskDTO] = {ActionType(task.action_type): task for task in tasks}
            assert task_dict[ActionType.SCAN_ROOTFOLDER].status == TaskStatus.ACTIVATED.value, "SCAN_ROOTFOLDER task should be ACTIVATED"

            run_scheduler_tasks()
            # extract all task
            calendar, tasks = CleanupScheduler.extract_active_calendar_for_rootfolder(rootfolder)
            task_dict: dict[ActionType, CleanupTaskDTO] = {ActionType(task.action_type): task for task in tasks}
            assert task_dict[ActionType.START_RETENTION_REVIEW].status == TaskStatus.ACTIVATED.value, "START_RETENTION_REVIEW task should be ACTIVATED"

            run_scheduler_tasks()
            calendar, tasks = CleanupScheduler.extract_active_calendar_for_rootfolder(rootfolder)
            task_dict: dict[ActionType, CleanupTaskDTO] = {ActionType(task.action_type): task for task in tasks}
            assert task_dict[ActionType.SEND_INITIAL_NOTIFICATION].status == TaskStatus.ACTIVATED.value, "SEND_INITIAL_NOTIFICATION task should be ACTIVATED"

            run_scheduler_tasks()
            calendar, tasks = CleanupScheduler.extract_active_calendar_for_rootfolder(rootfolder)
            task_dict: dict[ActionType, CleanupTaskDTO] = {ActionType(task.action_type): task for task in tasks}
            assert task_dict[ActionType.SEND_FINAL_NOTIFICATION].status == TaskStatus.ACTIVATED.value, "SEND_FINAL_NOTIFICATION task should be ACTIVATED"

            run_scheduler_tasks()
            calendar, tasks = CleanupScheduler.extract_active_calendar_for_rootfolder(rootfolder)
            task_dict: dict[ActionType, CleanupTaskDTO] = {ActionType(task.action_type): task for task in tasks}
            assert task_dict[ActionType.CLEAN_ROOTFOLDER].status == TaskStatus.ACTIVATED.value, "CLEAN_ROOTFOLDER task should be ACTIVATED"

            run_scheduler_tasks()
            calendar, tasks = CleanupScheduler.extract_active_calendar_for_rootfolder(rootfolder)
            task_dict: dict[ActionType, CleanupTaskDTO] = {ActionType(task.action_type): task for task in tasks}
            assert task_dict[ActionType.FINISH_CLEANUP_CYCLE].status == TaskStatus.ACTIVATED.value, "FINISH_CLEANUP_CYCLE task should be ACTIVATED"

            run_scheduler_tasks()
            calendar, tasks = CleanupScheduler.extract_active_calendar_for_rootfolder(rootfolder)
            task_dict: dict[ActionType, CleanupTaskDTO] = {ActionType(task.action_type): task for task in tasks}
            assert task_dict[ActionType.STOP_AFTER_CLEANUP_CYCLE].status == TaskStatus.ACTIVATED.value, "STOP_AFTER_CLEANUP_CYCLE task should be ACTIVATED"

            run_scheduler_tasks()
            calendar, tasks = CleanupScheduler.extract_active_calendar_for_rootfolder(rootfolder)
            task_dict: dict[ActionType, CleanupTaskDTO] = {ActionType(task.action_type): task for task in tasks}
            assert len(task_dict)  == 0, f"all active tasks should be COMPLETED but there is still {len(task_dict)} tasks active"
            cleanup_configuration: CleanupConfigurationDTO = get_cleanup_configuration_by_rootfolder_id(rootfolder.id)

            assert cleanup_configuration is not None, "Cleanup configuration should be found"
            assert cleanup_configuration.cleanup_start_date is None, "Cleanup configuration cleanup_start_date should be None after stopping cleanup cycle in this test"

            pass
        