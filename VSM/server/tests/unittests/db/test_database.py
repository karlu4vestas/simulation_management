import pytest
from sqlmodel import SQLModel, Session
from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeDTO, RetentionTypeDTO       
from db.database import Database


class TestDatabase:
    """Test the Database singleton class"""

    def test_database_singleton_creation(self, clean_database):
        """Test that Database creates a singleton instance"""
        db1 = Database()
        db2 = Database()
        
        # Both should be the same instance
        assert db1 is db2
        assert Database._instance is not None

    def test_database_get_engine_class_method(self, clean_database):
        """Test that Database.get_engine() works as a class method"""
        engine = Database.get_engine()
        
        assert engine is not None
        assert Database._instance is not None
        assert Database._instance._engine is engine

    def test_database_get_engine_returns_same_engine(self, clean_database):
        """Test that multiple calls to get_engine return the same engine"""
        engine1 = Database.get_engine()
        engine2 = Database.get_engine()
        
        assert engine1 is engine2

    def test_database_get_db_method(self, clean_database):
        """Test the Database.get_db() class method"""
        db_instance = Database.get_db()
        
        assert db_instance is not None
        assert isinstance(db_instance, Database)
        assert Database._instance is db_instance
        
        # Second call should return the same instance
        db_instance2 = Database.get_db()
        assert db_instance is db_instance2

    def test_database_is_empty_with_no_tables(self, clean_database):
        """Test that is_empty returns True when no tables exist"""
        db = Database.get_db()
        
        # Without creating tables, database should be considered empty
        is_db_empty = db.is_empty()
        assert is_db_empty is True


    def test_database_engine_creates_tables(self, clean_database):
        """Test that the database engine creates all SQLModel tables"""
        engine = Database.get_engine()
        
        # Get table names from metadata
        table_names = [table.name for table in SQLModel.metadata.tables.values()]
        
        # Check that our DTO tables are created
        expected_tables = [
            "rootfolderdto",
            "foldernodedto", 
            "foldertypedto",
            "retentiontypedto"
        ]
        
        for expected_table in expected_tables:
            assert expected_table in table_names
