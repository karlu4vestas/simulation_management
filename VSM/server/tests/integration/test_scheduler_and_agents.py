from multiprocessing import Queue
import os
import csv
import shutil
import pytest
from datetime import date, datetime
from dataclasses import dataclass
from datetime import timedelta
from app.clock import SystemClock

from cleanup.clean_agent.simulation_file_registry import SimulationFileRegistry
from tests.generate_vts_simulations.main_validate_cleanup import validate_cleanup
from sqlmodel import Session, select
from db import db_api
from db.database import Database

from datamodel import dtos
from datamodel.vts_create_meta_data import insert_vts_metadata_in_db

from app.web_api import run_scheduler_tasks
from cleanup.agent_on_premise_scan  import AgentScanVTSRootFolder
from cleanup.agent_on_premise_clean import AgentCleanVTSRootFolder
from cleanup.scheduler_dtos import AgentInfo, CalendarStatus, CleanupCalendarDTO, CleanupTaskDTO
from cleanup.agent_runner import AgentCallbackHandler, InternalAgentFactory
from cleanup.agents_internal import (
    AgentCalendarCreation,
    AgentMarkSimulationsPreReview,
    AgentNotification,
    AgentUnmarkSimulationsPostReview,
    AgentFinaliseCleanupCycle
)

from tests.generate_vts_simulations.GenerateTimeseries import SimulationType
from tests.generate_vts_simulations.main_GenerateSimulation import GeneratedSimulationsResult, SimulationTestSpecification, generate_simulations
from .testdata_for_import import InMemoryFolderNode, RootFolderWithMemoryFolders,CleanupConfiguration
from tests import test_storage
from app.app_config import AppConfig

TEST_STORAGE_LOCATION = test_storage.LOCATION
AppConfig.set_test_mode(AppConfig.Mode.INTEGRATION_TEST)  # Avoid unused import warning
#implementation of class AgentCallbackHandler(ABC):
# the implementation also get the CleanupState.progress by load it using rootfolder_id from CleanupTaskDTO
# Data are saved to a two list of AgentExecutionRecord for later verification of the pre and post run data in the test  

@dataclass
class AgentExecutionRecord:
    """Record of agent execution for testing purposes"""
    calendar_id: int | None
    agent_id: str
    action_types: list[str]
    task_id: int | None
    rootfolder_id: int | None
    progress: dtos.CleanupProgress.Progress | None
    error_message: str | None
    success_message: str | None

class AgentCallbackHandlerForTesting(AgentCallbackHandler):
    """Callback handler for testing that collects agent execution data"""
    
    def __init__(self):
        self.postrun_records: list[AgentExecutionRecord] = []
    
    def on_agent_postrun(self, agent_info: AgentInfo, task: CleanupTaskDTO | None, 
                         error_message: str | None, success_message: str | None) -> None:
        """Collect data after agent completes"""
        progress = None
        rootfolder_id = None
        calendar_id = None
        
        if task:
            calendar_id = task.calendar_id if hasattr(task, 'calendar_id') else None
            if task.rootfolder_id:
                rootfolder_id  = task.rootfolder_id
                cleanup_config = db_api.get_cleanup_configuration_by_rootfolder_id(task.rootfolder_id)
                if cleanup_config:
                    progress = dtos.CleanupProgress.Progress(cleanup_config.progress)
        
        record = AgentExecutionRecord(
            agent_id=agent_info.agent_id,
            action_types=agent_info.action_types,
            task_id=task.id if task else None,
            calendar_id=calendar_id,
            rootfolder_id=rootfolder_id,
            progress=progress,
            error_message=error_message,
            success_message=success_message
        )
        self.postrun_records.append(record)
    
    def save_records_to_csv(self, postrun_path: str) -> None:
        # Save prerun and postrun records to CSV files        
        # Args:
        #     postrun_path: Path where postrun records should be saved
       
        # Ensure directories exist
        os.makedirs(os.path.dirname(postrun_path), exist_ok=True)
        
        # Define CSV headers
        headers = ['calendar_id', 'agent_id', 'action_types', 'task_id', 'rootfolder_id', 
                   'progress', 'error_message', 'success_message']
        
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
                    record.progress.value if record.progress else '',
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
    def generate_simulations_folder_and_files(rootdir: str, rootfolder_data:RootFolderWithMemoryFolders, remove_rootdir:bool=True) -> GeneratedSimulationsResult:
        #remove old test data
        if remove_rootdir and os.path.isdir(rootdir):
            shutil.rmtree(rootdir)

        # generate the simulations but before that all paths must be adjusted to point to the test storage location
        rootfolder_storage_path: str                    = rootdir #os.path.join( rootdir, "rootfolder" )
        rootfolder_data.rootfolder.path                 = os.path.join( rootfolder_storage_path, rootfolder_data.rootfolder.path)
        leaf_folders:list[InMemoryFolderNode]           = [folder for folder in rootfolder_data.folders if folder.is_leaf]
        simulation_folders: SimulationTestSpecification = [SimulationTestSpecification( os.path.join(rootfolder_storage_path, folder.path), SimulationType.VTS, folder.modified_date) 
                                                                                        for folder in leaf_folders]        
        gen_sim_results: GeneratedSimulationsResult     = generate_simulations(rootdir, simulation_folders)
        return gen_sim_results
    
    @staticmethod
    def get_completed_calendars(rootfolder_id:int=None) -> list[CleanupCalendarDTO]:
        with Session(Database.get_engine()) as session:
            # Get all active calendars for this rootfolder
            if rootfolder_id is None:
                active_calendars = session.exec( select(CleanupCalendarDTO).where(
                    (CleanupCalendarDTO.status == CalendarStatus.COMPLETED.value) )).all()
            else:
                active_calendars = session.exec( select(CleanupCalendarDTO).where(
                        (CleanupCalendarDTO.rootfolder_id == rootfolder_id) &
                        (CleanupCalendarDTO.status == CalendarStatus.COMPLETED.value) )).all()
            return active_calendars

    @staticmethod
    def get_active_calendars(rootfolder_id:int=None) -> list[CleanupCalendarDTO]:
        with Session(Database.get_engine()) as session:
            # Get all active calendars for this rootfolder
            if rootfolder_id is None:
                active_calendars = session.exec( select(CleanupCalendarDTO).where(
                    (CleanupCalendarDTO.status == CalendarStatus.ACTIVE.value) )).all()
            else:
                active_calendars = session.exec( select(CleanupCalendarDTO).where(
                        (CleanupCalendarDTO.rootfolder_id == rootfolder_id) &
                        (CleanupCalendarDTO.status == CalendarStatus.ACTIVE.value) )).all()
            return active_calendars

    def run_scheduler_and_agents_with_full_cleanup_round(self, integration_session, cleanup_scenario_data, data_keys:list[str], number_of_runs:int, run_random:bool=False):
        # The purpose of this test is to test 
        # 1) the scheduling of tasks 
        # 2) that agent tasks are executed as expected
        # 3) that the results are as expected:
        #    - vts simulations are imported 
        #    - htc simulation are not imported
        #    - we will run with no active path protections so the retention will be numeric in the database
        #    - simulations are marked for cleanup and cleaned as expected and cleanup after the cleanup round is completed

        #Furthermore the test can be used to 
        # 1) verify that each sequntual run (run_random=False) creates a new cleanup calendar and completes it 
        #    because the list of Agents has ben setup in their natural order of execution
        # 2) verify that when run_random=True the same agents can be executed in random order and still complete the cleanup round successfully.
        #    However, when agents are executed out of their natural order their attemtps to reserve a task will often fail because the status of the calender is not ready.
        #    In other word multiple calls to run_scheduler_tasks will be required to complete a calendar 
        #  Notice that option 2 represents that conditions in a production

        # Notice 
        #  1) that the condition to ensure that some simulations get marked for cleanup is that.
        #     The date of the CleanupConfigurationDTO' start_date + leadtime is before the simulations modified date
        #     The easiest way to use modified date = now and start_date = now-leadtime-1
        #  2) A cleanup_round required the passage of time equal to the frequency to finalize the round. 
        #     That is we must either simulate or let time pass. frequency was changed to float in order to allow a second to pass (that is 1/(24*60*60) of a day) in this way 
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

        # mem_cleanup_config: CleanupConfiguration = CleanupConfiguration( 
        #     leadtime=7,
        #     frequency=1./(24*60*60),  # set to one second for the test
        #     start_date=SystemClock.now() - timedelta(days=8),  #8 = leadtime+1 ensure that simulations are marked for cleanup
        #     progress=dtos.CleanupProgress.Progress.INACTIVE
        # )
        
        runtime_callback: AgentCallbackHandlerForTesting = AgentCallbackHandlerForTesting()
        # setup folder for the test
        io_dir_for_storage_test: str = os.path.join(os.path.normpath(TEST_STORAGE_LOCATION),"test_integrationphase_5_scheduler_and_agents")
        if os.path.isdir(io_dir_for_storage_test):
            shutil.rmtree(io_dir_for_storage_test)

        # Now we are ready for the test. We have:simuulations on desk and rootfolder with an inactive cleanup configuration in the db
        # Lets define the environment variables needed by the scheduler and agents

        # for the AgentScanVTSRootFolder
        os.environ['SCAN_TEMP_FOLDER'] = os.path.join(io_dir_for_storage_test, "temp_for_scanning")  # where should the meta data for file and folders be placed
        os.environ['SCAN_THREADS'] = str(1)  # number of scanning threads

        #for the cleanup agent
        os.environ['CLEAN_TEMP_FOLDER'] = os.path.join(io_dir_for_storage_test, "temp_for_cleaning")
        os.environ['CLEAN_SIM_WORKERS'] = str(1)
        os.environ['CLEAN_DELETION_WORKERS'] = str(2)
        os.environ['CLEAN_MODE'] = 'DELETE' #'ANALYSE'
        n_calendars_completed: int=0;
        class DataSet:
            data_key:str
            sim_dir:str
            rootfolder_data:RootFolderWithMemoryFolders
            gen_sim_results: GeneratedSimulationsResult
            sim_registry:dict[str,SimulationFileRegistry] = None
            rootfolder:dtos.RootFolderDTO
            cleanup_config:dtos.CleanupConfigurationDTO
            def __init__(self, data_key, sim_dir, rootfolder_data, gen_sim_results, root_cleanup_config):
                self.data_key = data_key
                self.sim_dir = sim_dir
                self.rootfolder_data = rootfolder_data
                self.gen_sim_results = gen_sim_results
                self.root_cleanup_config = root_cleanup_config
                self.rootfolder = root_cleanup_config[0]
                self.cleanup_config = root_cleanup_config[1]

            def validate_cleanup_of_all(self) -> tuple[str, list[str]]:
                scope_before_leafs:list[str] = [in_mem_folder.path for in_mem_folder in self.rootfolder_data.leafs]
                return validate_cleanup(gen_sim_results.validation_csv_file, scope_before_leafs)    

            def getBeforeLeafFiles(self) -> tuple[str, list[str]]:
                # scope the folders to those modified before the cleanup.start_date-cleanup.leadtime
                scope_before_leafs:list[str] = [in_mem_folder.path for in_mem_folder in self.rootfolder_data.before_leafs]
                return validate_cleanup(gen_sim_results.validation_csv_file, scope_before_leafs)    
            
        data_sets: list[DataSet] = []
        n_calendars_completed:int = 0
        n_actual_runs:int = 0
        for n_actual_runs in range(number_of_runs):

            # generate new data for each iteration
            for data_key in data_keys:
                rootfolder_data:RootFolderWithMemoryFolders = cleanup_scenario_data[data_key]
                sim_dir = os.path.join(io_dir_for_storage_test, "sim_dir", data_key)        
                gen_sim_results: GeneratedSimulationsResult = TestSchedulerAndAgents.generate_simulations_folder_and_files(sim_dir, rootfolder_data, remove_rootdir=False)
                root_cleanup_config:tuple[dtos.RootFolderDTO, dtos.CleanupConfigurationDTO] = TestSchedulerAndAgents.import_rootfolder_and_cleanup_configuration(session=integration_session, 
                                                                                                                                                                 rootfolder=rootfolder_data.rootfolder, 
                                                                                                                                                                 in_memory_config=rootfolder_data.cleanup_configuration #mem_cleanup_config
                                                                                                                                                                 )
                data_sets.append( DataSet(data_key, sim_dir, rootfolder_data, gen_sim_results, root_cleanup_config) )    
                  
            # Create fresh test-specific agents for each iteration to avoid stale state
            # (they will pick up the environment variables set above)
            test_agents = [
                AgentCalendarCreation(),
                AgentScanVTSRootFolder(),       # Uses SCAN_TEMP_FOLDER, SCAN_THREADS env vars
                AgentMarkSimulationsPreReview(),
                AgentNotification(),
                AgentNotification(),
                AgentCleanVTSRootFolder(),      # Uses CLEAN_TEMP_FOLDER, CLEAN_SIM_WORKERS, etc. env vars
                AgentUnmarkSimulationsPostReview(),
                AgentFinaliseCleanupCycle(),
            ]

            if run_random:
                with InternalAgentFactory.with_agents(test_agents):
                    run_scheduler_tasks(runtime_callback, run_randomized=True)
                    
                    dataset = data_sets[0] #only update the SystemClock once. this will work because all datasets uses the same cleanupconfiguration and ofcause the same SystemClock 
                    if len(TestSchedulerAndAgents.get_active_calendars()) >= len(data_keys) :
                        #The calenders block at the first ofset > 0. When both calendars are active then advance the time so that they can finish
                        SystemClock.set_offset_days(SystemClock.get_offset_days()+dataset.cleanup_config.frequency + 0.01) 
                    
                    n_calendars_completed = len(TestSchedulerAndAgents.get_completed_calendars() )
                    if n_calendars_completed >= len(data_keys): #cannot pervent the creation of a new calendar while the we await that at least two gets completed 
                        break

            else:    
                with InternalAgentFactory.with_agents(test_agents):
                    for dataset in data_sets:
                        print( f"cleanup configuration: {dataset.cleanup_config} " )
                        print( f"all leafs, before_leafs, mid_leafs, after_leafs: {len(dataset.rootfolder_data.before_leafs)}, {len(dataset.rootfolder_data.mid_leafs)}, {len(dataset.rootfolder_data.after_leafs)}" )
                        from cleanup.scheduler import CleanupScheduler
                        task_defs:list[dict[str,object]] = CleanupScheduler._get_task_definitions(retention_review_duration=dataset.cleanup_config.frequency)
                        #calendar : CleanupCalendarDTO = CleanupScheduler.get_active_calendar_by_rootfolder_id(dataset.rootfolder.id)
                        #print( f"now: {SystemClock.now()} - calendar.start_date: {calendar.start_date}" )
                        print( f"now: {SystemClock.now()} - cleanup_config.start_date: {dataset.cleanup_config.start_date}" )
                        print( f"task_defs:" )
                        for task_def in task_defs:
                            print(f"    {task_def['action_type']} - {task_def['task_offset']} first start: {dataset.cleanup_config.start_date + timedelta(days=task_def['task_offset'])}" )

                    run_scheduler_tasks(runtime_callback)
                    # the first round will stop at the first tash with an offset > 0 like the final notification and the AgentCleanVTSRootFolder
                    # By advancing the system clock to just after the current cleanup round the next run_scheduler_tasks() can finish the round
                    SystemClock.set_offset_days(SystemClock.get_offset_days()+dataset.cleanup_config.frequency + 0.01) 
                    run_scheduler_tasks(runtime_callback)
                    for dataset in data_sets:
                        # as long as we are not able to set the modified date on the files, the clean up will clean all files because the integration session sets the  
                        # SystemClock offset one day after the leadtime used in the cleanup_scenario_data' cleanupconfiguration. see conftest.py 
                        # def integration_session():
                        #     # Create a SystemClock offset by more than cleanup configuration leadtime 
                        #     SystemClock.set_offset_days(leadtime + 1)

                        #if ever we can set the modifed date on teh file system then we can verify that leafs_before get cleaned and the rest does not
                        filepath_to_validation_results, failed_filepaths = dataset.validate_cleanup_of_all()
                        assert len(failed_filepaths) == 0, f"The cleanup of for data_key:{dataset.data_key} missed {len(failed_filepaths)}. See validation results here:{filepath_to_validation_results}."
                    


        runtime_callback.save_records_to_csv(
            postrun_path=os.path.join(io_dir_for_storage_test, "agent_postrun_records.csv")
        )

        n_calendars_completed = len(TestSchedulerAndAgents.get_completed_calendars() )
        return n_calendars_completed,n_actual_runs+1
        pass #just for breakpoint so we can inspect the database after the test run

    def test_scheduler_and_agents_with_full_cleanup_round(self, integration_session, cleanup_scenario_data):
        number_of_runs=1
        n_calendars_completed,n_actual_runs = self.run_scheduler_and_agents_with_full_cleanup_round(integration_session, cleanup_scenario_data, 
                                                                                                    data_keys=["first_rootfolder"], 
                                                                                                    number_of_runs=number_of_runs, run_random=False)
        assert n_calendars_completed == number_of_runs

    def test_random_scheduler_and_agents_with_full_cleanup_round(self, integration_session, cleanup_scenario_data):
        #data_keys = ["first_rootfolder"]
        data_keys:list[str] = ["first_rootfolder", "second_rootfolder_part_one"]
        number_of_runs=7 * len(data_keys) # in the worst case the maximum number of agents required to complete a calendar is 7
        n_calendars_completed,n_actual_runs = self.run_scheduler_and_agents_with_full_cleanup_round(integration_session, cleanup_scenario_data, 
                                                                                                    data_keys=data_keys, 
                                                                                                    number_of_runs=number_of_runs, run_random=True)
        print(f"n_calendars_completed, n_actual_runs: {n_calendars_completed}, {n_actual_runs}")
        assert n_calendars_completed == len(data_keys)
