#Unit tests for on_premise_clean_agent.py
#Tests the AgentCleanRootFolder and AgentCleanProgressWriter classes.

import os
import sys
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch
from datetime import date

# Add server directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from cleanup.agent_on_premise_clean import AgentCleanProgressWriter, AgentCleanVTSRootFolder
from cleanup.clean_agent.clean_main import CleanupResult
from cleanup.clean_agent.clean_parameters import CleanMeasures, CleanMode
from datamodel.dtos import FileInfo, FolderTypeEnum
from datamodel.retentions import ExternalRetentionTypes
from tests import test_storage

TEST_STORAGE_LOCATION = test_storage.LOCATION

        
class MockAgentCleanVTSRootFolder(AgentCleanVTSRootFolder):
    def __init__(self):
        super().__init__()
        self.extracted_simulations: list[FileInfo] = []

    def insert_or_update_simulations_in_db(self, task_id: int, extracted_simulations: list[FileInfo]) -> dict[str, str]:
        #Override to capture simulations locally instead of inserting to DB.
        self.extracted_simulations = extracted_simulations
        return {"status": "success", "count": str(len(extracted_simulations))}

class TestAgentCleanProgressWriter(unittest.TestCase):
    """Test the nested AgentCleanProgressWriter class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a mock agent
        self.mock_agent = Mock(spec=MockAgentCleanVTSRootFolder)
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

class TestAgentCleanRootFolder(unittest.TestCase):
    """Test the AgentCleanRootFolder class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Set environment variables for testing
        self.temp_dir = tempfile.mkdtemp()
        os.environ['CLEAN_TEMP_FOLDER'] = self.temp_dir
        os.environ['CLEAN_SIM_WORKERS'] = '4'
        os.environ['CLEAN_DELETION_WORKERS'] = '1'
        os.environ['CLEAN_MODE'] = 'ANALYSE'
        
        # Create agent
        self.agent = MockAgentCleanVTSRootFolder()
        
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
        for key in ['CLEAN_TEMP_FOLDER', 'CLEAN_SIM_WORKERS', 'CLEAN_DELETION_WORKERS', 'CLEAN_MODE']:
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
        """Test initialization with invalid temp folder that cannot be created"""
        # Use a path that cannot be created due to permission/existence issues
        # This will cause os.makedirs to fail and the agent to set error_message
        invalid_path = '/nonexistent_root_path_cannot_be_created/subfolder'
        os.environ['CLEAN_TEMP_FOLDER'] = invalid_path
        agent = MockAgentCleanVTSRootFolder()
        
        # Since the path cannot be created, temporary_result_folder should be None
        self.assertIsNone(agent.temporary_result_folder)
        self.assertIsNotNone(agent.error_message)
        self.assertIn("Failed to create temporary result folder", agent.error_message)
    
    def test_initialization_invalid_clean_mode(self):
        """Test initialization with invalid clean mode"""
        os.environ['CLEAN_MODE'] = 'INVALID_MODE'
        agent = MockAgentCleanVTSRootFolder()
        
        self.assertEqual(agent.clean_mode, CleanMode.ANALYSE)  # Falls back to ANALYSE
        self.assertIsNotNone(agent.error_message)
        self.assertIn("Invalid CLEAN_MODE", agent.error_message)
    
    def test_initialization_delete_mode(self):
        """Test initialization with DELETE mode"""
        os.environ['CLEAN_MODE'] = 'DELETE'
        agent = MockAgentCleanVTSRootFolder()
        
        self.assertEqual(agent.clean_mode, CleanMode.DELETE)
    
    def test_execute_task_invalid_temp_folder(self):
        """Test execute_task when temp folder is invalid"""
        self.agent.temporary_result_folder = None
        
        self.agent.execute_task()        
        # Should return early without doing anything
        # No exception should be raised

if __name__ == '__main__':
    unittest.main()
