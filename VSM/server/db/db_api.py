from sqlmodel import Session, func, select
from fastapi import FastAPI, Query, HTTPException
from db.database import Database
from datamodel.dtos import CleanupConfiguration, CleanupFrequencyDTO, CycleTimeDTO, RetentionTypeDTO, FolderTypeDTO
from datamodel.dtos import RootFolderDTO, FolderNodeDTO, PathProtectionDTO, SimulationDomainDTO, RetentionUpdateDTO 

def insert_rootfolder(rootfolder:RootFolderDTO):
    if rootfolder.simulationdomain_id is None or rootfolder.simulationdomain_id == 0:
        raise HTTPException(status_code=404, detail="You must provide a valid simulationdomain_id to create a rootfolder")

    with Session(Database.get_engine()) as session:
        #verify if the rootfolder already exists
        existing_rootfolder:RootFolderDTO = session.exec(select(RootFolderDTO).where( RootFolderDTO.simulationdomain_id == rootfolder.simulationdomain_id and 
                                                                             RootFolderDTO.path == rootfolder.path)).first()
        if existing_rootfolder:
            return existing_rootfolder

        session.add(rootfolder)
        session.commit()
        session.refresh(rootfolder)
        return rootfolder
