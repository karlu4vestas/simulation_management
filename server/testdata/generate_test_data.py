from sqlmodel import Session, select
from datamodel.dtos import RetentionTypeDTO

def insert_test_data_in_db(engine):
    with Session(engine) as session:
        session.add(RetentionTypeDTO(name="Cleaned",          display_rank=1,  is_system_managed=True ))
        session.add(RetentionTypeDTO(name="MarkedForCleanup", display_rank=2,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="CleanupIssue",     display_rank=3,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="New",              display_rank=4,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+1Next",           display_rank=5,  is_system_managed=False ))  
        session.add(RetentionTypeDTO(name="+Q1",              display_rank=6,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+Q3",              display_rank=7,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+Q6",              display_rank=8,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+1Y",              display_rank=9,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+2Y",              display_rank=10, is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+3Y",              display_rank=11, is_system_managed=False ))
        session.add(RetentionTypeDTO(name="longterm",         display_rank=12, is_system_managed=False ))
        session.add(RetentionTypeDTO(name="path protected",   display_rank=13, is_system_managed=False ))

        session.commit()

        retentions = session.exec(select(RetentionTypeDTO)).all()
        print("Test data inserted successfully:")
        for retention in retentions:
            print(f" - {retention.name} (ID: {retention.id})")
