import io
from contextlib import asynccontextmanager
from typing import Literal, Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlmodel import Session, func, select
from datamodel.dtos import CleanupConfigurationDTO, CleanupProgress, CleanupFrequencyDTO, CycleTimeDTO, RetentionTypeDTO, FolderTypeDTO
from datamodel.dtos import RootFolderDTO, FolderNodeDTO, PathProtectionDTO, SimulationDomainDTO, RetentionUpdateDTO 
from db.database import Database
from app.app_config import AppConfig
from datamodel.vts_create_meta_data import insert_vts_metadata_in_db

from testdata.vts_generate_test_data import insert_test_folder_hierarchy_in_db
from db.db_api import read_simulation_domains,read_simulation_domain_by_name, read_retentiontypes_by_domain_id, read_folder_types_pr_domain_id, read_cycle_time_by_domain_id
from db.db_api import read_cleanupfrequency_by_domain_id, read_rootfolders_by_domain_and_initials, update_rootfolder_cleanup_configuration
from db.db_api import read_rootfolder_retentiontypes, read_folders
from db.db_api import read_pathprotections, add_path_protection, delete_path_protection
from db.db_api import change_retentions, FileInfo 

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    db = Database.get_db()

    if db.is_empty() :
        engine = db.get_engine()
        db.create_db_and_tables()

        if AppConfig.is_client_test():
            with Session(engine) as session:
                insert_vts_metadata_in_db(session)
                insert_test_folder_hierarchy_in_db(session)
    #else:
    #    db.clear_all_tables_and_schemas()
    
    yield
    # Shutdown (if needed in the future)

app = FastAPI(lifespan=lifespan)

origins = [
    "http://localhost:5173",
    "localhost:5173",
    "http://127.0.0.1:5173",
    "127.0.0.1:5173"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Handle favicon requests to avoid 404 logs
@app.get("/favicon.ico")
async def favicon():
    from fastapi.responses import Response
    return Response(status_code=204)

# Get the current test mode configuration.
@app.get("/", tags=["root"])
async def get_current_test_mode() -> dict:
    return {
        "is_unit_test": AppConfig.is_unit_test(),
        "is_client_test": AppConfig.is_client_test(),
        "is_production": AppConfig.is_production()
    }

#-----------------start retrieval of metadata for a simulation domain -------------------
@app.get("/v1/simulationdomains/", response_model=list[SimulationDomainDTO])
def fs_read_simulation_domains():
    return read_simulation_domains()

@app.get("/v1/simulationdomains/{domain_name}", response_model=SimulationDomainDTO)
def fs_read_simulation_domain_by_name(domain_name: str):
    return read_simulation_domain_by_name(domain_name)

@app.get("/v1/simulationdomains/{simulationdomain_id}/retentiontypes/", response_model=list[RetentionTypeDTO])
def fs_read_retentiontypes_by_domain_id(simulationdomain_id: int):
    return read_retentiontypes_by_domain_id(simulationdomain_id)


@app.get("/v1/simulationdomains/{simulationdomain_id}/foldertypes/", response_model=list[FolderTypeDTO])
def fs_read_folder_types_pr_domain_id(simulationdomain_id: int):
    return read_folder_types_pr_domain_id(simulationdomain_id)

# The cycle time for one simulation is the time from initiating the simulation, til cleanup the simulation. 
@app.get("/v1/simulationdomains/{simulationdomain_id}/cycletimes/", response_model=list[CycleTimeDTO])
def fs_read_cycle_time_by_domain_id(simulationdomain_id: int):
    return read_cycle_time_by_domain_id(simulationdomain_id)


# The cycle time for one simulation is the time from initiating the simulation, til cleanup the simulation. 
@app.get("/v1/simulationdomains/{simulationdomain_id}/cleanupfrequencies/", response_model=list[CleanupFrequencyDTO])
def fs_read_cleanupfrequency_by_domain_id(simulationdomain_id: int):
    return read_cleanupfrequency_by_domain_id(simulationdomain_id)

#-----------------end retrieval of metadata for a simulation domain -------------------


#-----------------start maintenance of rootfolders and information under it -------------------

# we must only allow the webclient to read the RootFolders
@app.get("/v1/rootfolders/", response_model=list[RootFolderDTO])
def fs_read_rootfolders(simulationdomain_id: int, initials: Optional[str] = Query(default="")):
    return read_rootfolders_by_domain_and_initials(simulationdomain_id, initials)



# update a rootfolder's cleanup_configuration
@app.post("/v1/rootfolders/{rootfolder_id}/cleanup_configuration")
def fs_update_rootfolder_cleanup_configuration(rootfolder_id: int, cleanup_configuration: CleanupConfigurationDTO):
    return update_rootfolder_cleanup_configuration(rootfolder_id, cleanup_configuration)



@app.get("/v1/rootfolders/{rootfolder_id}/retentiontypes", response_model=list[RetentionTypeDTO])
def fs_read_rootfolder_retentiontypes(rootfolder_id: int):
    return read_rootfolder_retentiontypes(rootfolder_id)


#develop a @app.get("/folders/")   that extract send all FolderNodeDTOs as csv
@app.get("/v1/rootfolders/{rootfolder_id}/folders/", response_model=list[FolderNodeDTO])
def fs_read_folders( rootfolder_id: int ):
    return read_folders( rootfolder_id )


# Endpoint to extract and send all FolderNodeDTOs as CSV
#@app.get("/v1/rootfolders/{rootfolder_id}/folders/csv")
#def fs_read_folders_csv(rootfolder_id: int):
#    return read_folders_csv(rootfolder_id)
    


# Endpoint to extract and send all FolderNodeDTOs as compressed CSV using zstd
#@app.get("/v1/rootfolders/{rootfolder_id}/folders/csv-zstd")
#def fs_read_folders_csv_zstd(rootfolder_id: int):
#    return read_folders_csv_zstd(rootfolder_id)



#get all path protections for a specific root folder
@app.get("/v1/rootfolders/{rootfolder_id}/pathprotections", response_model=list[PathProtectionDTO])
def fs_read_pathprotections( rootfolder_id: int ):
    return read_pathprotections( rootfolder_id )


# Add a new path protection to a specific root folder
@app.post("/v1/rootfolders/{rootfolder_id}/pathprotection")
def fs_add_path_protection(rootfolder_id:int, path_protection:PathProtectionDTO):
    return add_path_protection(rootfolder_id, path_protection)



# Delete a path protection from a specific root folder
@app.delete("/v1/rootfolders/{rootfolder_id}/pathprotection")
def fs_delete_path_protection(rootfolder_id: int, protection_id: int):
    return delete_path_protection(rootfolder_id, protection_id)

    
# The following can be called when the securefolder' cleanup configuration is fully defined meaning that rootfolder.cleanupfrequency and rootfolder.cycletime msut be set
# it will adjust the expiration dates to the user selected retention categories in the webclient
#   the expiration date for non-numeric retentions is set to None
#   the expiration date for numeric retention is set to cleanup_round_start_date + days_to_cleanup for the user selected retention type
@app.post("/v1/rootfolders/{rootfolder_id}/retentions")
def fs_change_retentions(rootfolder_id: int, retentions: list[RetentionUpdateDTO]):
    return change_retentions(rootfolder_id, retentions)




#-----------------end maintenance of rootfolders and information under it -------------------

#-----------------Agents API -------------------
from scheduler.cleanup_scheduler import AgentInfo, CleanupScheduler, CleanupTaskDTO
@app.get("/v1/agent/reserve_task", response_model=CleanupTaskDTO| None)
def fs_agent_reserve_task(agent: AgentInfo) -> CleanupTaskDTO| None:
    return AgentInfo.reserve_task(agent)

@app.post("/v1/agent/task/{task_id}/task_completion")
def fs_agent_task_completion(task_id: int, status: str, status_message: str|None = None) -> dict[str,str]:
    return AgentInfo.task_completion(task_id, status, status_message)

@app.post("/v1/agent/task/{task_id}/insert_or_update_simulations")
def fs_agent_insert_or_update_simulations_in_db(task_id: int, rootfolder_id: int, simulations: list[FileInfo]) -> dict[str, str]:
    return AgentInfo.insert_or_update_simulations_in_db(task_id, rootfolder_id, simulations)

@app.get("/v1/agent/task/{task_id}/marked_for_cleanup", response_model=list[str])
def fs_agent_read_folders_marked_for_cleanup(task_id: int, rootfolder_id: int) -> list[str]:
    return AgentInfo.read_simulations_marked_for_cleanup(task_id, rootfolder_id)

#-----------------Scheduler API -------------------
@app.post("/v1/scheduler/update_calendars_and_tasks")
def fs_schedule_calendars_and_tasks():
    CleanupScheduler.update_calendars_and_tasks()
    CleanupScheduler.call_internal_agents()
    return {"message": "Scheduler updated successfully"}