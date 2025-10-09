from sqlmodel import SQLModel, create_engine, Session, select
from sqlalchemy import Engine
from typing import Optional
from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeDTO, RetentionTypeDTO, SimulationDomainDTO, CleanupFrequencyDTO, CycleTimeDTO
from app.app_config import AppConfig

class Database:
    _instance: Optional["Database"] = None
    _engine: Optional[Engine] = None
    sqlite_url: str = ""

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            # Get the database URL from AppConfig based on test mode
            config = AppConfig()
            sqlite_url = config.get_db_url()
            cls._instance._initialize(sqlite_url)
            print(f"create new db instance at: {cls._instance.sqlite_url}")
        return cls._instance

    def _initialize(self, sqlite_url: str):
        self._engine = create_engine(sqlite_url, echo=False)
        self.sqlite_url = sqlite_url

    #only call this if we need to create the tables
    def create_db_and_tables(self):
        if self.get_engine() is not None:
            SQLModel.metadata.create_all(self._engine)

    @classmethod
    def get_db(cls) -> "Database":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def get_engine(cls) -> Engine:
        return Database.get_db()._engine
    
    def is_empty(self) -> bool:
        """
        Check if the database is empty by verifying if all tables have zero rows.
        Returns True if the database is empty (no data in any table), False otherwise.
        """
        if self._engine is None:
            return True
            
        # List of all table models that should be checked (including metadata tables)
        table_models = [RootFolderDTO, FolderNodeDTO, FolderTypeDTO, RetentionTypeDTO, SimulationDomainDTO, CleanupFrequencyDTO, CycleTimeDTO]
        
        try:
            with Session(self._engine) as session:
                for model in table_models:
                    # Check if table exists and has any rows
                    statement = select(model).limit(1)
                    result = session.exec(statement).first()
                    if result is not None:
                        return False
                return True
        except Exception:
            # If there's an error (e.g., tables don't exist), consider it empty
            return True

    def clear_all_tables_and_schemas(self):
        """
        Clear all tables and drop all schemas from the database.
        This will completely reset the database structure and data.
        """
        if self._engine is None:
            return
            
        try:
            # Drop all tables defined in SQLModel metadata
            SQLModel.metadata.drop_all(self._engine)
            print("All tables and schemas have been cleared from the database")
        except Exception as e:
            print(f"Error clearing tables and schemas: {e}")
            raise
    def delete_db (self):
        #delte the db-file
        import os
        if self.sqlite_url.startswith("sqlite:///"):
            db_file = self.sqlite_url.replace("sqlite:///", "")
            if os.path.exists(db_file):
                os.remove(db_file)
                print(f"Database file {db_file} deleted.")
            else:
                print(f"Database file {db_file} does not exist.")
        else:
            print("Database deletion is only supported for SQLite databases.")

if __name__ == "__main__":
    db:Database = Database.get_db()
    db.create_db_and_tables()
