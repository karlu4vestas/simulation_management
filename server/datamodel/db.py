from sqlmodel import SQLModel, create_engine

class Database:
    sqlite_url: str = "sqlite:///:memory:"  # in memory db and disk the name could be db.sqlite
    _instance = None
    _engine = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            # at some point we will read the sqlite_url from the environment or elsewhere. For now just create it in memory
            cls._instance._initialize(cls.sqlite_url)
        return cls._instance

    def _initialize(self, sqlite_url: str):
        self._engine = create_engine(sqlite_url, echo=False)
        SQLModel.metadata.create_all(self._engine)

    @classmethod
    def get_engine(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance._engine
