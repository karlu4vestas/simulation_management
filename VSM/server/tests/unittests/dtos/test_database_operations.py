import pytest
from sqlmodel import Session, select
from datamodel.dtos import RootFolderDTO, FolderNodeDTO, SimulationDomainDTO
from db.database import Database


class TestDatabaseOperations:
    """Test database operations with DTOs"""

    def test_create_and_retrieve_root_folder(self, database_with_tables, sample_root_folder_data):
        """Test creating and retrieving a RootFolderDTO from database"""
        engine = Database.get_engine()
        
        with Session(engine) as session:
            # Create and save
            folder = RootFolderDTO(**sample_root_folder_data)
            session.add(folder)
            session.commit()
            session.refresh(folder)
            
            # Verify it was saved with an ID
            assert folder.id is not None
            saved_id = folder.id
            
            # Retrieve and verify
            statement = select(RootFolderDTO).where(RootFolderDTO.id == saved_id)
            retrieved_folder = session.exec(statement).first()
            
            assert retrieved_folder is not None
            assert retrieved_folder.path == "/test/folder"
            assert retrieved_folder.owner == "JD"
