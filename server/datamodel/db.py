from sqlmodel import SQLModel, create_engine, Session, select
from sqlalchemy import Engine, text
from typing import Optional
from .dtos import RootFolderDTO, FolderNodeDTO, FolderTypeDTO, RetentionTypeDTO

class Database:
    sqlite_url: str = "sqlite:///:memory:"  # in memory db and disk the name could be db.sqlite
    _instance: Optional["Database"] = None
    _engine: Optional[Engine] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            # at some point we will read the sqlite_url from the environment or elsewhere. For now just create it in memory
            cls._instance._initialize(cls.sqlite_url)
        return cls._instance

    def _initialize(self, sqlite_url: str):
        self._engine = create_engine(sqlite_url, echo=False)


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
            
        # List of all table models that should be checked
        table_models = [RootFolderDTO, FolderNodeDTO, FolderTypeDTO, RetentionTypeDTO]
        
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

if __name__ == "__main__":
    db:Database = Database.get_db()
    db.create_db_and_tables()
