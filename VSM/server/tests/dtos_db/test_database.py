import pytest
from sqlmodel import SQLModel
from datamodel.db import Database


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

    def test_database_engine_creates_tables(self, clean_database):
        """Test that the database engine creates all SQLModel tables"""
        engine = Database.get_engine()
        
        # Check that tables are created by inspecting metadata
        from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeDTO, RetentionTypeDTO
        
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

    def test_database_uses_memory_sqlite_by_default(self, clean_database):
        """Test that the database uses in-memory SQLite by default"""
        engine = Database.get_engine()
        
        # Check the URL contains the memory database indicator
        assert engine is not None
        assert ":memory:" in str(engine.url)

    def test_database_singleton_state_persistence(self, clean_database):
        """Test that the singleton maintains state across different access methods"""
        # First access via class method
        engine1 = Database.get_engine()
        
        # Second access via instance
        db_instance = Database()
        engine2 = db_instance._engine
        
        # Third access via class method again
        engine3 = Database.get_engine()
        
        # All should be the same engine
        assert engine1 is engine2 is engine3

    def test_database_get_db_method(self, clean_database):
        """Test the Database.get_db() class method"""
        db_instance = Database.get_db()
        
        assert db_instance is not None
        assert isinstance(db_instance, Database)
        assert Database._instance is db_instance
        
        # Second call should return the same instance
        db_instance2 = Database.get_db()
        assert db_instance is db_instance2

    def test_database_create_db_and_tables_method(self, clean_database):
        """Test the create_db_and_tables() method"""
        db = Database.get_db()
        
        # Tables should be created
        db.create_db_and_tables()
        
        # Verify tables exist by checking metadata
        from datamodel.dtos import RootFolderDTO, FolderNodeDTO
        table_names = [table.name for table in SQLModel.metadata.tables.values()]
        
        assert "rootfolderdto" in table_names
        assert "foldernodedto" in table_names

    def test_database_integration_get_db_and_engine(self, clean_database):
        """Test that get_db() and get_engine() work together correctly"""
        # Get database instance
        db = Database.get_db()
        
        # Get engine via class method
        engine = Database.get_engine()
        
        # They should be related
        assert engine is db._engine
        
        # Create tables
        db.create_db_and_tables()
        
        # Engine should still be the same
        assert Database.get_engine() is engine

    def test_database_is_empty_with_no_tables(self, clean_database):
        """Test that is_empty returns True when no tables exist"""
        db = Database.get_db()
        
        # Without creating tables, database should be considered empty
        assert db.is_empty() is True

    def test_database_is_empty_with_empty_tables(self, database_with_tables):
        """Test that is_empty returns True when tables exist but are empty"""
        db = database_with_tables
        
        # With tables created but no data, database should be empty
        assert db.is_empty() is True

    def test_database_is_not_empty_with_data(self, database_with_tables):
        """Test that is_empty returns False when tables contain data"""
        from sqlmodel import Session
        from datamodel.dtos import RootFolderDTO
        
        db = database_with_tables
        
        # Add some data to make database non-empty
        with Session(db.get_engine()) as session:
            root_folder = RootFolderDTO(
                path="/test/folder",
                owner="testuser"
                ##cleanupfrequency=""
            )
            session.add(root_folder)
            session.commit()
        
        # Now database should not be empty
        assert db.is_empty() is False

    def test_database_is_empty_engine_none(self):
        """Test that is_empty returns True when engine is None"""
        # Create a database instance but don't initialize the engine
        Database._instance = None
        Database._engine = None
        
        db = Database.__new__(Database)
        db._engine = None
        
        assert db.is_empty() is True
