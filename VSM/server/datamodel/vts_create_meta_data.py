from sqlmodel import Session, select
from datamodel.dtos import FolderTypeEnum, SimulationDomainDTO, FolderTypeDTO, CleanupFrequencyDTO, LeadTimeDTO, RetentionTypeEnum, RetentionTypeDTO

def insert_vts_metadata_in_db(session:Session):
    # ensure that redundant metadata is not present 
    session.add(SimulationDomainDTO(name="vts" ))
    session.commit()

    vts_simulation_domain = session.exec(select(SimulationDomainDTO).where(SimulationDomainDTO.name == "vts")).first()
    #print(f"vts_simulation_domain created. id={vts_simulation_domain.id} name={vts_simulation_domain.name}")
    sim_id = vts_simulation_domain.id if vts_simulation_domain and vts_simulation_domain.id else 0
    if sim_id == 0:
        raise ValueError("vts simulation domain not found")

    # **Retention Catalog** (key=retention_label, value=days_to_cleanup):
    # - **`marked`** (0 days): Mandatory state. This state is for simulation were the retention expired (`retention_expiration_date <= cleanup_round_start_date`). Changing to this retention sets `retention_expiration_date = cleanup_round_start_date`
    # - **`next`** (frequency days): Mandatory state. New simulations created in the current cleanup round, that are not path-protected, will be marked for cleanup in the next cleanup round by setting setting `retention_expiration_date = modified_date` for new simulations. Changing the retention of other simulations to this state sets `retention_expiration_date = cleanup_round_start_date + frequency`
    # - **`90d`** (90 days): Changing to this retention sets `retention_expiration_date = cleanup_round_start_date + 90 days`
    # - **`180d`** (180 days): Changing to this retention sets `retention_expiration_date = cleanup_round_start_date + 180 days`
    # - **`365d`** (365 days): Changing to this retention sets `retention_expiration_date = cleanup_round_start_date + 365 days`
    # - **`730d`** (730 days): Changing to this retention sets `retention_expiration_date = cleanup_round_start_date + 730 days`
    # - **`1095d`** (1095 days): Changing to this retention sets `retention_expiration_date = cleanup_round_start_date + 1095 days`
    # - **`path`**: (null) `this state is for path protected simulations`
    # - **`clean`**: (null) `this state is for clean simulation so the user can see all simulations`
    # - **`issue`**: (null) `this state is for simulation with a cleanup issue so the user can see all simulations`
    # - **`Missing`**: (null) `this state is for simulation that are no longer found in the root folder for any reason but most likely due to the user renaming or deleting the folder
    session.add(RetentionTypeDTO(name="?",         days_to_cleanup=None,  simulationdomain_id=sim_id, display_rank=0,  is_endstage=False ))
    session.add(RetentionTypeDTO(name="marked",    days_to_cleanup=0,     simulationdomain_id=sim_id, display_rank=1,  is_endstage=False ))
    session.add(RetentionTypeDTO(name="+7d(Next)", days_to_cleanup=7,     simulationdomain_id=sim_id, display_rank=2,  is_endstage=False ))  # 7 days after start of current cleanup cycle is enough
    session.add(RetentionTypeDTO(name="+90d",      days_to_cleanup=90,    simulationdomain_id=sim_id, display_rank=3,  is_endstage=False ))
    session.add(RetentionTypeDTO(name="+180",      days_to_cleanup=180,   simulationdomain_id=sim_id, display_rank=4,  is_endstage=False ))
    session.add(RetentionTypeDTO(name="+365",      days_to_cleanup=365,   simulationdomain_id=sim_id, display_rank=5,  is_endstage=False ))
    session.add(RetentionTypeDTO(name="+730",      days_to_cleanup=730,   simulationdomain_id=sim_id, display_rank=6,  is_endstage=False ))
    session.add(RetentionTypeDTO(name="+1095",     days_to_cleanup=1095,  simulationdomain_id=sim_id, display_rank=7,  is_endstage=False ))
    session.add(RetentionTypeDTO(name=RetentionTypeEnum.PATH.value,      days_to_cleanup=None,  simulationdomain_id=sim_id, display_rank=8,  is_endstage=False ))
    session.add(RetentionTypeDTO(name=RetentionTypeEnum.ISSUE.value,     days_to_cleanup=None,  simulationdomain_id=sim_id, display_rank=9,  is_endstage=True ))
    session.add(RetentionTypeDTO(name=RetentionTypeEnum.CLEAN.value,     days_to_cleanup=None,  simulationdomain_id=sim_id, display_rank=10, is_endstage=True  ))
    session.add(RetentionTypeDTO(name=RetentionTypeEnum.MISSING.value,   days_to_cleanup=None,  simulationdomain_id=sim_id, display_rank=11, is_endstage=True  ))
    session.commit()
    #retentions = session.exec(select(RetentionTypeDTO)).all()
    #print("Test data inserted successfully:")
    #for retention in retentions:
    #    print(f" - {retention.name} (ID: {retention.id})")

    session.add(FolderTypeDTO(name=FolderTypeEnum.INNERNODE, simulationdomain_id=sim_id ))
    session.add(FolderTypeDTO(name=FolderTypeEnum.SIMULATION, simulationdomain_id=sim_id ))
    session.commit()
    #folder_types = session.exec(select(FolderTypeDTO)).all()
    #print("Test data for folder_types inserted successfully:")
    #for folder in folder_types:
    #    print(f" - {folder.name} (ID: {folder.id})")

    session.add(CleanupFrequencyDTO(name="inactive", days=-1, simulationdomain_id=sim_id )) # need the inactive entry to have an id=1
    session.add(CleanupFrequencyDTO(name="1 week",   days= 7, simulationdomain_id=sim_id ))
    session.add(CleanupFrequencyDTO(name="2 weeks",  days=14, simulationdomain_id=sim_id ))
    session.add(CleanupFrequencyDTO(name="3 weeks",  days=21, simulationdomain_id=sim_id ))
    session.add(CleanupFrequencyDTO(name="4 weeks",  days=28, simulationdomain_id=sim_id ))
    session.add(CleanupFrequencyDTO(name="5 weeks",  days=35, simulationdomain_id=sim_id ))
    session.add(CleanupFrequencyDTO(name="6 weeks",  days=42, simulationdomain_id=sim_id ))
    session.commit()
    #cleanup_frequencies = session.exec(select(CleanupFrequencyDTO)).all()
    #print("Test data for cleanup frequencies inserted successfully:")
    #for cleanup in cleanup_frequencies:
    #    print(f" - {cleanup.name} (ID: {cleanup.id})")


    session.add(LeadTimeDTO(name="inactive", days=-1, simulationdomain_id=sim_id ))
    session.add(LeadTimeDTO(name="1 week",   days= 7, simulationdomain_id=sim_id ))
    session.add(LeadTimeDTO(name="2 weeks",  days=14, simulationdomain_id=sim_id ))
    session.add(LeadTimeDTO(name="3 weeks",  days=21, simulationdomain_id=sim_id ))
    session.add(LeadTimeDTO(name="4 weeks",  days=28, simulationdomain_id=sim_id ))
    session.add(LeadTimeDTO(name="6 weeks",  days=42, simulationdomain_id=sim_id ))
    session.add(LeadTimeDTO(name="8 weeks",  days=56, simulationdomain_id=sim_id ))
    session.commit()

    #days_to_analyse = session.exec(select(LeadTimeDTO)).all()
    #print("Test data for days to analyse inserted successfully:")
    #for days in days_to_analyse:
    #    print(f" - {days.name} (ID: {days.id})")