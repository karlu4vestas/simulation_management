from sqlmodel import Session
from datamodel.dtos import RetentionTypeDTO

def insert_test_data_in_db(engine):
    with Session(engine) as session:
        """
        Create RetentionTypeDTO swith the following names:
            Cleaned
            MarkedForCleanup 
            CleanupIssue    
            New
            +1Next
            +Q1
            +Q3
            +Q6
            +1Y
            +2Y
            +3Y
            longterm
            path protected
        """

        retention_type = RetentionTypeDTO(name="Test Retention", description="Test Description")
        session.add(retention_type)
        session.commit()
