from sqlmodel import Session, func, select
from fastapi import FastAPI, Query, HTTPException
from db.database import Database
from datamodel.dtos import CleanupConfigurationDTO, CleanupFrequencyDTO, CycleTimeDTO, RetentionTypeDTO, FolderTypeDTO
from datamodel.dtos import RootFolderDTO, FolderNodeDTO, PathProtectionDTO, SimulationDomainDTO, RetentionUpdateDTO
 

def insert_rootfolder(rootfolder:RootFolderDTO):
    if (rootfolder is None) or (rootfolder.simulationdomain_id is None) or (rootfolder.simulationdomain_id == 0):
        raise HTTPException(status_code=404, detail="You must provide a valid simulationdomain_id to create a rootfolder")

    with Session(Database.get_engine()) as session:
        #verify if the rootfolder already exists
        existing_rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(
                (RootFolderDTO.simulationdomain_id == rootfolder.simulationdomain_id) & 
                (RootFolderDTO.path == rootfolder.path)
            )).first()
        if existing_rootfolder:
            return existing_rootfolder

        session.add(rootfolder)
        session.commit()
        #session.refresh(rootfolder)

        if (rootfolder.id is None) or (rootfolder.id == 0):
            raise HTTPException(status_code=404, detail=f"Failed to provide {rootfolder.path} with an id")
        return rootfolder

#insert the cleanup configuration for a rootfolder and update the rootfolder to point to the cleanup configuration
def insert_cleanup_configuration(rootfolder_id:int, cleanup_config: CleanupConfigurationDTO):
    if (rootfolder_id is None) or (rootfolder_id == 0):
        raise HTTPException(status_code=404, detail="You must provide a valid rootfolder_id to create a cleanup configuration")
    
    with Session(Database.get_engine()) as session:
        #verify if the rootfolder already exists
        existing_rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where(
                (RootFolderDTO.id == rootfolder_id)
            )).first()
        if not existing_rootfolder:
            raise HTTPException(status_code=404, detail=f"Failed to find rootfolder with id {rootfolder_id} to create a cleanup configuration")

        #verify if a cleanup configuration already exists for this rootfolder
        existing_cleanup_config:CleanupConfigurationDTO = session.exec(select(CleanupConfigurationDTO).where(
                (CleanupConfigurationDTO.rootfolder_id == rootfolder_id)
            )).first()
        if existing_cleanup_config:
            return existing_cleanup_config
        
        cleanup_config.rootfolder_id = rootfolder_id
        session.add(cleanup_config)
        session.commit()
        session.refresh(cleanup_config)
        existing_rootfolder.cleanup_config_id = cleanup_config.id
        session.add(existing_rootfolder)
        session.commit()

        if (cleanup_config.id is None) or (cleanup_config.id == 0):
            raise HTTPException(status_code=404, detail=f"Failed to provide cleanup configuration for rootfolder_id {rootfolder_id} with an id")
        return cleanup_config   
