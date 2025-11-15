import os
import shutil
import pytest
from datetime import date
from dataclasses import dataclass
from datetime import timedelta

from sqlmodel import Session

from datamodel import dtos

from app.web_api import run_scheduler_tasks
from db import db_api
from cleanup_cycle import cleanup_db_actions, cleanup_dtos
from cleanup_cycle.agent_on_premise_scan import AgentScanVTSRootFolder
from cleanup_cycle.agent_on_premise_clean import AgentCleanVTSRootFolder
from cleanup_cycle.scheduler_dtos import ActionType, CleanupTaskDTO, TaskStatus
from cleanup_cycle.agents_internal import (
    AgentTemplate,
    AgentCalendarCreation,
    AgentCleanupCycleStart,
    AgentNotification,
    AgentCleanupCycleFinishing,
    #AgentCleanupCyclePrepareNext
)
from cleanup_cycle.agent_runner import InternalAgentFactory
from cleanup_cycle.scheduler_db_actions import CleanupScheduler

from datamodel.vts_create_meta_data import insert_vts_metadata_in_db
from tests.generate_vts_simulations.GenerateTimeseries import SimulationType
from tests.generate_vts_simulations.main_GenerateSimulation import GeneratedSimulationsResult, SimulationTestSpecification, generate_simulations
from .testdata_for_import import InMemoryFolderNode, RootFolderWithMemoryFolders,CleanupConfiguration
from tests import test_storage
from cleanup_cycle import cleanup_db_actions



TEST_STORAGE_LOCATION = test_storage.LOCATION
    
# class ForTestAgentCleanupCyclePrepareNextAndStop(AgentTemplate):
#     def __init__(self):
#         super().__init__("TestAgentCleanupCyclePrepareNextAndStop", [ActionType.STOP_AFTER_CLEANUP_CYCLE.value])

#     def reserve_task(self):
#         self.task = CleanupTaskManager.reserve_task(self.agent_info)

#     def execute_task(self):
#         cleanup_db_actions.cleanup_cycle_prepare_next_cycle(self.task.rootfolder_id, prepare_next_cycle_and_stop=True)
#         self.success_message = f"Next cleanup cycle prepared for rootfolder {self.task.rootfolder_id} but the Cleanup cycle is stopped here by setting cleanup_start_date=None"

class ForTestAgentCalendarCreation(AgentTemplate):
    # this ia a fake agent because it does not require a task and will always be run when called
    # In fact the agent calls the scheduler to create calendars and tasks for rootfolder that are ready to start cleanup cycles
    def __init__(self):
        super().__init__("AgentCalendarCreation", [])

    # run without reservation because not calendar exists for this agent
    def run(self):
        self.execute_task()

    def execute_task(self):
        msg: str = CleanupScheduler.create_calendars_for_cleanup_configuration_ready_to_start()
        CleanupScheduler.update_calendars_and_tasks() # prepare so the next task can be activated right away
        self.success_message = f"CalendarCreation done with: {msg}"

# class ForTestAgentCalendarClosure(AgentTemplate):
#     # this ia a fake agent because it does not require a task and will always be run when called
#     # In fact the agent calls the scheduler to close calendars that finishes
#     def __init__(self):
#         super().__init__("ForTestAgentCalendarClosure", [ActionType.CLOSE_FINISHED_CALENDARS.value])
#     # run without reservation because not calendar exists for this agent
#     def run(self):
#          self.execute_task()

#     def execute_task(self):
#         msg: str = cleanup_db_actions.close_finished_calenders()
#         self.success_message = f"ForTestAgentCalendarClosure done with: {msg}"

#implementation of class AgentCallbackHandler(ABC):
# the implementation also get the CleanupState.cleanup_progress by load it using rootfolder_id from CleanupTaskDTO
# Data are saved to a two list of AgentExecutionRecord for later verification of the pre and post run data in the test  

@dataclass
class AgentExecutionRecord:
    """Record of agent execution for testing purposes"""
    calendar_id: int | None
    agent_id: str
    action_types: list[str]
    task_id: int | None
    rootfolder_id: int | None
    cleanup_progress: dtos.CleanupProgress.ProgressEnum | None
    error_message: str | None
    success_message: str | None

from cleanup_cycle.agent_runner import AgentCallbackHandler
from cleanup_cycle.scheduler_dtos import AgentInfo

class TestAgentCallbackHandler(AgentCallbackHandler):
    """Callback handler for testing that collects agent execution data"""
    
    def __init__(self):
        self.prerun_records: list[AgentExecutionRecord] = []
        self.postrun_records: list[AgentExecutionRecord] = []
    
    def on_agent_prerun(self, agent_info: AgentInfo, task: CleanupTaskDTO | None) -> None:
        """Collect data before agent runs"""
        cleanup_progress = None
        rootfolder_id = None
        calendar_id = None
        
        if task:
            calendar_id = task.calendar_id if hasattr(task, 'calendar_id') else None
            if task.rootfolder_id:
                rootfolder_id = task.rootfolder_id
                cleanup_config = db_api.get_cleanup_configuration_by_rootfolder_id(task.rootfolder_id)
                if cleanup_config:
                    cleanup_progress = dtos.CleanupProgress.ProgressEnum(cleanup_config.cleanup_progress)
        
        record = AgentExecutionRecord(
            agent_id=agent_info.agent_id,
            action_types=agent_info.action_types,
            task_id=task.id if task else None,
            calendar_id=calendar_id,
            rootfolder_id=rootfolder_id,
            cleanup_progress=cleanup_progress,
            error_message=None,
            success_message=None
        )
        self.prerun_records.append(record)
    
    def on_agent_postrun(self, agent_info: AgentInfo, task: CleanupTaskDTO | None, 
                         error_message: str | None, success_message: str | None) -> None:
        """Collect data after agent completes"""
        cleanup_progress = None
        rootfolder_id = None
        calendar_id = None
        
        if task:
            calendar_id = task.calendar_id if hasattr(task, 'calendar_id') else None
            if task.rootfolder_id:
                rootfolder_id = task.rootfolder_id
                cleanup_config = db_api.get_cleanup_configuration_by_rootfolder_id(task.rootfolder_id)
                if cleanup_config:
                    cleanup_progress = dtos.CleanupProgress.ProgressEnum(cleanup_config.cleanup_progress)
        
        record = AgentExecutionRecord(
            agent_id=agent_info.agent_id,
            action_types=agent_info.action_types,
            task_id=task.id if task else None,
            calendar_id=calendar_id,
            rootfolder_id=rootfolder_id,
            cleanup_progress=cleanup_progress,
            error_message=error_message,
            success_message=success_message
        )
        self.postrun_records.append(record)
    
    def save_records_to_csv(self, prerun_path: str, postrun_path: str) -> None:
        """Save prerun and postrun records to CSV files
        
        Args:
            prerun_path: Path where prerun records should be saved
            postrun_path: Path where postrun records should be saved
        """
        import csv
        
        # Ensure directories exist
        os.makedirs(os.path.dirname(prerun_path), exist_ok=True)
        os.makedirs(os.path.dirname(postrun_path), exist_ok=True)
        
        # Define CSV headers
        headers = ['calendar_id', 'agent_id', 'action_types', 'task_id', 'rootfolder_id', 
                   'cleanup_progress', 'error_message', 'success_message']
        
        # Save prerun records
        with open(prerun_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(headers)
            for record in self.prerun_records:
                writer.writerow([
                    record.calendar_id or '',
                    record.agent_id,
                    '|'.join(record.action_types) if record.action_types else '',
                    record.task_id or '',
                    record.rootfolder_id or '',
                    record.cleanup_progress.value if record.cleanup_progress else '',
                    record.error_message or '',
                    record.success_message or ''
                ])
        
        # Save postrun records
        with open(postrun_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(headers)
            for record in self.postrun_records:
                writer.writerow([
                    record.calendar_id or '',
                    record.agent_id,
                    '|'.join(record.action_types) if record.action_types else '',
                    record.task_id or '',
                    record.rootfolder_id or '',
                    record.cleanup_progress.value if record.cleanup_progress else '',
                    record.error_message or '',
                    record.success_message or ''
                ])

@pytest.mark.integration
@pytest.mark.cleanup_workflow
@pytest.mark.slow
class TestSchedulerAndAgents:
    @staticmethod
    def import_rootfolder_and_cleanup_configuration(session:Session, rootfolder:dtos.RootFolderDTO, in_memory_config:CleanupConfiguration)-> tuple[dtos.RootFolderDTO, dtos.CleanupConfigurationDTO]:

        # Step 0: Set up a new database and verify that it is empty apart from VTS metadata
        simulation_domain_id = db_api.read_simulation_domains()[0].id
        assert simulation_domain_id is not None and simulation_domain_id > 0

        #save the rootfolder
        rootfolder.simulationdomain_id = simulation_domain_id
        rootfolder = db_api.insert_rootfolder(rootfolder)
        assert rootfolder is not None
        assert rootfolder.id is not None and rootfolder.id > 0
        assert rootfolder.path == rootfolder.path
        
        # Create CleanupConfigurationDTO from the in-memory CleanupConfiguration
        # The cleanup_scenario_data fixture uses the old CleanupConfiguration dataclass for in-memory setup
        # Now we create the corresponding CleanupConfigurationDTO database record
        cleanup_config:dtos.CleanupConfigurationDTO = in_memory_config.to_dto(rootfolder_id=rootfolder.id)
        cleanup_config:dtos.CleanupConfigurationDTO = db_api.insert_cleanup_configuration(rootfolder.id, cleanup_config)
        return rootfolder, cleanup_config
    
    @staticmethod
    def generate_simulations_folder_and_files(rootdir: str, rootfolder_data:RootFolderWithMemoryFolders) -> GeneratedSimulationsResult:
        #remove old test data
        if os.path.isdir(rootdir):
            shutil.rmtree(rootdir)

        # generate the simulations but before that all paths must be adjusted to point to the test storage location
        rootfolder_storage_path: str                    = os.path.join( rootdir, "rootfolder" )
        rootfolder_data.rootfolder.path                 = os.path.join( rootfolder_storage_path, rootfolder_data.rootfolder.path)
        leaf_folders:list[InMemoryFolderNode]           = [folder for folder in rootfolder_data.folders if folder.is_leaf]
        simulation_folders: SimulationTestSpecification = [SimulationTestSpecification( os.path.join(rootfolder_storage_path, folder.path), 
                                                                                        SimulationType.VTS) for folder in leaf_folders]        
        gen_sim_results: GeneratedSimulationsResult     = generate_simulations(rootdir, simulation_folders)
        return gen_sim_results

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

        mem_cleanup_config: CleanupConfiguration = CleanupConfiguration( 
            cycletime=7,
            cleanupfrequency=1./(24*60*60),  # set to one second for the test
            cleanup_start_date=date.today() - timedelta(days=8),  #8 = cycletime+1 ensure that simulations are marked for cleanup
            cleanup_progress=dtos.CleanupProgress.ProgressEnum.INACTIVE
        )
        
        runtime_callback: TestAgentCallbackHandler = TestAgentCallbackHandler()
        #Get one rootfolders and it list of leaf folders
        rootfolder_data:RootFolderWithMemoryFolders = cleanup_scenario_data["first_rootfolder"]        
        # setup folder for the test
        io_dir_for_storage_test: str = os.path.join(os.path.normpath(TEST_STORAGE_LOCATION),"test_integrationphase_5_scheduler_and_agents")
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

        for i in range(1):
            gen_sim_results: GeneratedSimulationsResult = TestSchedulerAndAgents.generate_simulations_folder_and_files(io_dir_for_storage_test, rootfolder_data)
            root_cleanup_config:tuple[dtos.RootFolderDTO, dtos.CleanupConfigurationDTO] = TestSchedulerAndAgents.import_rootfolder_and_cleanup_configuration(session=integration_session, rootfolder=rootfolder_data.rootfolder, in_memory_config=mem_cleanup_config)
                  
            # Create fresh test-specific agents for each iteration to avoid stale state
            # (they will pick up the environment variables set above)
            test_agents = [
                #ForTestAgentCalendarClosure(),
                ForTestAgentCalendarCreation(),
                AgentScanVTSRootFolder(),       # Uses SCAN_TEMP_FOLDER, SCAN_THREADS env vars
                AgentCleanupCycleStart(),
                AgentNotification(),
                AgentNotification(),
                AgentCleanVTSRootFolder(),      # Uses CLEAN_TEMP_FOLDER, CLEAN_SIM_WORKERS, etc. env vars
                AgentCleanupCycleFinishing(),
                #AgentCleanupCyclePrepareNext(),
                #ForTestAgentCleanupCyclePrepareNextAndStop()
            ]
            with InternalAgentFactory.with_agents(test_agents):
                # Step 3: Run scheduler to create scan tasks and execute the scan
                run_scheduler_tasks(runtime_callback)

        runtime_callback.save_records_to_csv(
            prerun_path=os.path.join(io_dir_for_storage_test, "agent_prerun_records.csv"),
            postrun_path=os.path.join(io_dir_for_storage_test, "agent_postrun_records.csv")
        )