#Unit tests for on_premise_clean_agent.py
#Tests the AgentCleanRootFolder and AgentCleanProgressWriter classes.

import os
import sys
import shutil
import tempfile
import unittest
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime, date

# Add server directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from cleanup_cycle.on_premise_clean_agent import AgentCleanProgressWriter, AgentCleanVTSSimulations
from cleanup_cycle.clean_agent.clean_main import CleanupResult
from cleanup_cycle.clean_agent.clean_parameters import CleanMeasures, CleanMode
from datamodel.dtos import FileInfo, FolderTypeEnum, ExternalRetentionTypes
from tests import test_storage

TEST_STORAGE_LOCATION = test_storage.LOCATION

class TestAgentCleanProgressWriter(unittest.TestCase):
    """Test the nested AgentCleanProgressWriter class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a mock agent
        self.mock_agent = Mock(spec=AgentCleanVTSSimulations)
        self.mock_agent.task = Mock()
        self.mock_agent.task.id = 123
        
        # Create progress writer
        self.progress_writer = AgentCleanProgressWriter(
            self.mock_agent,
            seconds_between_update=1,
            seconds_between_filelog=60
        )
    
    def test_initialization(self):
        """Test progress writer initializes correctly"""
        self.assertEqual(self.progress_writer.agentCleanRootFolder, self.mock_agent)
        self.assertEqual(self.progress_writer.seconds_between_update, 1)
        self.assertEqual(self.progress_writer.seconds_between_filelog, 60)
    
    @patch('cleanup_cycle.on_premise_clean_agent.AgentInterfaceMethods')
    def test_update_reports_progress(self, mock_interface):
        """Test that update() calls AgentInterfaceMethods.task_progress()"""
        # Create measures
        measures = CleanMeasures(
            simulations_processed=10,
            simulations_cleaned=8,
            simulations_issue=1,
            simulations_skipped=1,
            files_deleted=100,
            bytes_deleted=1000000,
            error_count=0
        )
        
        # Call update
        self.progress_writer.update(measures, deletion_queue_size=50, active_threads=4)
        
        # Verify task_progress was called
        mock_interface.task_progress.assert_called_once()
        call_args = mock_interface.task_progress.call_args
        
        # Check task_id
        self.assertEqual(call_args[0][0], 123)
        
        # Check message contains key metrics
        message = call_args[0][1]
        self.assertIn("Processed: 10", message)
        self.assertIn("Cleaned: 8", message)
        self.assertIn("Issue: 1", message)
        self.assertIn("Skipped: 1", message)
        self.assertIn("Queue: 50", message)
        self.assertIn("Threads: 4", message)
    
    @patch('cleanup_cycle.on_premise_clean_agent.AgentInterfaceMethods')
    def test_update_with_zero_values(self, mock_interface):
        """Test update with all zero values"""
        measures = CleanMeasures(
            simulations_processed=0,
            simulations_cleaned=0,
            simulations_issue=0,
            simulations_skipped=0,
            files_deleted=0,
            bytes_deleted=0,
            error_count=0
        )
        
        self.progress_writer.update(measures, deletion_queue_size=0, active_threads=0)
        
        mock_interface.task_progress.assert_called_once()
        message = mock_interface.task_progress.call_args[0][1]
        self.assertIn("Processed: 0", message)


class TestAgentCleanRootFolder(unittest.TestCase):
    """Test the AgentCleanRootFolder class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Set environment variables for testing
        self.temp_dir = tempfile.mkdtemp()
        os.environ['TEMPORARY_CLEAN_RESULTS'] = self.temp_dir
        os.environ['CLEAN_SIM_WORKERS'] = '4'
        os.environ['CLEAN_DELETION_WORKERS'] = '1'
        os.environ['CLEAN_MODE'] = 'ANALYSE'
        
        # Create agent
        self.agent = AgentCleanVTSSimulations()
        
        # Mock task
        self.agent.task = Mock()
        self.agent.task.id = 456
        self.agent.task.path = os.path.join(TEST_STORAGE_LOCATION, "test_agent/root/folder")
    
    def tearDown(self):
        """Clean up after tests"""
        # Remove temp directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        
        # Clean up environment variables
        for key in ['TEMPORARY_CLEAN_RESULTS', 'CLEAN_SIM_WORKERS', 
                    'CLEAN_DELETION_WORKERS', 'CLEAN_MODE']:
            if key in os.environ:
                del os.environ[key]
    
    def test_initialization_success(self):
        """Test successful initialization"""
        self.assertEqual(self.agent.temporary_result_folder, self.temp_dir)
        self.assertEqual(self.agent.nb_clean_sim_workers, 4)
        self.assertEqual(self.agent.nb_clean_deletion_workers, 1)
        self.assertEqual(self.agent.clean_mode, CleanMode.ANALYSE)
        self.assertIsNone(self.agent.error_message)
    
    def test_initialization_invalid_temp_folder(self):
        """Test initialization with invalid temp folder"""
        os.environ['TEMPORARY_CLEAN_RESULTS'] = '/nonexistent/path'
        agent = AgentCleanVTSSimulations()
        
        self.assertIsNone(agent.temporary_result_folder)
        self.assertIsNotNone(agent.error_message)
        self.assertIn("TEMPORARY_CLEAN_RESULTS", agent.error_message)
    
    def test_initialization_invalid_clean_mode(self):
        """Test initialization with invalid clean mode"""
        os.environ['CLEAN_MODE'] = 'INVALID_MODE'
        agent = AgentCleanVTSSimulations()
        
        self.assertEqual(agent.clean_mode, CleanMode.ANALYSE)  # Falls back to ANALYSE
        self.assertIsNotNone(agent.error_message)
        self.assertIn("Invalid CLEAN_MODE", agent.error_message)
    
    def test_initialization_delete_mode(self):
        """Test initialization with DELETE mode"""
        os.environ['CLEAN_MODE'] = 'DELETE'
        agent = AgentCleanVTSSimulations()
        
        self.assertEqual(agent.clean_mode, CleanMode.DELETE)
    
    @patch('cleanup_cycle.on_premise_clean_agent.AgentInterfaceMethods')
    @patch('cleanup_cycle.on_premise_clean_agent.clean_main')
    def test_execute_task_success(self, mock_clean_main, mock_interface):
        """Test successful task execution"""
        # Mock task_read_folders_marked_for_cleanup
        simulation_paths = [
            os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim1"),
            os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim2"),
            os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim3")
        ]
        mock_interface.task_read_folders_marked_for_cleanup.return_value = simulation_paths
        
        # Mock clean_main result
        result_fileinfos = [
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim1"),
                modified_date=date(2024, 1, 1),
                nodetype=FolderTypeEnum.VTS_SIMULATION,
                external_retention=ExternalRetentionTypes.CLEAN.value
            ),
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim2"),
                modified_date=date(2024, 1, 2),
                nodetype=FolderTypeEnum.VTS_SIMULATION,
                external_retention=ExternalRetentionTypes.CLEAN.value
            ),
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim3"),
                modified_date=date(2024, 1, 3),
                nodetype=FolderTypeEnum.VTS_SIMULATION,
                external_retention=ExternalRetentionTypes.ISSUE.value
            )
        ]
        
        measures = CleanMeasures(
            simulations_processed=3,
            simulations_cleaned=2,
            simulations_issue=1,
            simulations_skipped=0,
            files_deleted=50,
            bytes_deleted=500000,
            error_count=0
        )
        
        mock_clean_main.return_value = CleanupResult(
            results=result_fileinfos,
            measures=measures
        )
        
        # Execute task
        self.agent.execute_task()
        
        # Verify task_read_folders_marked_for_cleanup was called
        mock_interface.task_read_folders_marked_for_cleanup.assert_called_once_with(456)
        
        # Verify progress messages
        self.assertEqual(mock_interface.task_progress.call_count, 2)
        
        # Check start message
        start_call = mock_interface.task_progress.call_args_list[0]
        self.assertEqual(start_call[0][0], 456)
        self.assertIn("Starting cleanup of 3 simulations", start_call[0][1])
        
        # Check completion message
        completion_call = mock_interface.task_progress.call_args_list[1]
        self.assertIn("Cleanup completed", completion_call[0][1])
        self.assertIn("3 processed", completion_call[0][1])
        self.assertIn("2 cleaned", completion_call[0][1])
        
        # Verify clean_main was called
        mock_clean_main.assert_called_once()
        
        # Verify task_insert_or_update_simulations_in_db was called
        mock_interface.task_insert_or_update_simulations_in_db.assert_called_once_with(
            456,
            result_fileinfos
        )
    
    @patch('cleanup_cycle.on_premise_clean_agent.AgentInterfaceMethods')
    def test_execute_task_no_simulations(self, mock_interface):
        """Test execute_task with no simulations to clean"""
        mock_interface.task_read_folders_marked_for_cleanup.return_value = []
        
        self.agent.execute_task()
        
        # Verify error message is set
        self.assertIsNotNone(self.agent.error_message)
        self.assertIn("No simulations marked for cleanup", self.agent.error_message)
    
    def test_execute_task_invalid_temp_folder(self):
        """Test execute_task when temp folder is invalid"""
        self.agent.temporary_result_folder = None
        
        self.agent.execute_task()
        
        # Should return early without doing anything
        # No exception should be raised
    
    @patch('cleanup_cycle.on_premise_clean_agent.clean_main')
    def test_clean_simulations_success(self, mock_clean_main):
        """Test clean_simulations method"""
        simulation_paths = [
            os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim1"),
            os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim2")
        ]
        
        # Mock return value
        measures = CleanMeasures(
            simulations_processed=2,
            simulations_cleaned=2,
            simulations_issue=0,
            simulations_skipped=0,
            files_deleted=25,
            bytes_deleted=250000,
            error_count=0
        )
        
        result_fileinfos = [
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim1"),
                modified_date=date(2024, 1, 1),
                nodetype=FolderTypeEnum.VTS_SIMULATION,
                external_retention=ExternalRetentionTypes.CLEAN.value
            )
        ]
        
        mock_clean_main.return_value = CleanupResult(
            results=result_fileinfos,
            measures=measures
        )
        
        # Call clean_simulations
        result = self.agent.clean_simulations(simulation_paths)
        
        # Verify result
        self.assertIsNotNone(result)
        self.assertEqual(len(result.results), 1)
        self.assertEqual(result.measures.simulations_processed, 2)
        
        # Verify clean_main was called with correct parameters
        mock_clean_main.assert_called_once()
        call_kwargs = mock_clean_main.call_args[1]
        
        self.assertEqual(len(call_kwargs['simulations']), 2)
        self.assertEqual(call_kwargs['clean_mode'], CleanMode.ANALYSE)
        self.assertEqual(call_kwargs['num_sim_workers'], 4)
        self.assertEqual(call_kwargs['num_deletion_workers'], 1)
        self.assertIsNotNone(call_kwargs['output_path'])
        self.assertIsNotNone(call_kwargs['progress_reporter'])
    
    @patch('cleanup_cycle.on_premise_clean_agent.clean_main')
    def test_clean_simulations_exception(self, mock_clean_main):
        """Test clean_simulations when clean_main raises exception"""
        simulation_paths = [os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim1")]
        
        # Mock exception
        mock_clean_main.side_effect = Exception("Clean failed")
        
        # Call clean_simulations
        result = self.agent.clean_simulations(simulation_paths)
        
        # Verify result is None and error is set
        self.assertIsNone(result)
        self.assertIsNotNone(self.agent.error_message)
        self.assertIn("Failed to clean simulations", self.agent.error_message)
        self.assertIn("Clean failed", self.agent.error_message)
    

class TestAgentIntegration(unittest.TestCase):
    """Integration tests for the complete workflow"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        os.environ['TEMPORARY_CLEAN_RESULTS'] = self.temp_dir
        os.environ['CLEAN_SIM_WORKERS'] = '2'
        os.environ['CLEAN_DELETION_WORKERS'] = '1'
        os.environ['CLEAN_MODE'] = 'ANALYSE'
    
    def tearDown(self):
        """Clean up after tests"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        
        for key in ['TEMPORARY_CLEAN_RESULTS', 'CLEAN_SIM_WORKERS', 
                    'CLEAN_DELETION_WORKERS', 'CLEAN_MODE']:
            if key in os.environ:
                del os.environ[key]
    
    @patch('cleanup_cycle.on_premise_clean_agent.AgentInterfaceMethods')
    @patch('cleanup_cycle.on_premise_clean_agent.clean_main')
    def test_full_workflow(self, mock_clean_main, mock_interface):
        """Test the complete workflow from execute_task to DB update"""
        # Create agent
        agent = AgentCleanVTSSimulations()
        agent.task = Mock()
        agent.task.id = 789
        agent.task.path = os.path.join(TEST_STORAGE_LOCATION, "test_agent/root")
        
        # Mock responses
        simulation_paths = [
            os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim1"),
            os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim2")
        ]
        mock_interface.task_read_folders_marked_for_cleanup.return_value = simulation_paths
        
        result_fileinfos = [
            FileInfo(
                filepath=os.path.join(TEST_STORAGE_LOCATION, "test_agent/sim1"),
                modified_date=date(2024, 1, 1),
                nodetype=FolderTypeEnum.VTS_SIMULATION,
                external_retention=ExternalRetentionTypes.CLEAN.value
            )
        ]
        
        measures = CleanMeasures(
            simulations_processed=2,
            simulations_cleaned=1,
            simulations_issue=0,
            simulations_skipped=1,
            files_deleted=10,
            bytes_deleted=100000,
            error_count=0
        )
        
        mock_clean_main.return_value = CleanupResult(
            results=result_fileinfos,
            measures=measures
        )
        
        # Execute
        agent.execute_task()
        
        # Verify the complete call chain
        mock_interface.task_read_folders_marked_for_cleanup.assert_called_once_with(789)
        mock_clean_main.assert_called_once()
        mock_interface.task_insert_or_update_simulations_in_db.assert_called_once_with(
            789,
            result_fileinfos
        )
        
        # Verify no errors
        self.assertIsNone(agent.error_message)


if __name__ == '__main__':
    unittest.main()
