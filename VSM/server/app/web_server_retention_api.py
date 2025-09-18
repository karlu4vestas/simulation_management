from datetime import date
from os import path
from fastapi import FastAPI, Depends, Query, HTTPException
from sqlmodel import Session, select
from datamodel.dtos import RetentionTypeDTO, RootFolderDTO, FolderNodeDTO, RetentionUpdateDTO
from datamodel.db import Database
from sqlalchemy import create_engine, text, or_, func
from testdata.vts_generate_test_data import insert_test_data_in_db


# -------------------------- all calculations for expiration dates, retentions are done in below functions ---------
# The consistency of these calculations is highly critical to the system working correctly

# The following is called when the user has selected a retention category for a folder in the webclient
# change retentions category for a list of folders in a rootfolder and update the corresponding expiration date
# the expiration date of non-numeric retention is set to None
# the expiration date of numeric retention is set to cleanup_status_date + days_to_cleanup of the retention type
@app.post("/rootfolder/{rootfolder_id}/retentions")
def change_retention_category( rootfolder_id: int, retentions: list[RetentionUpdateDTO]):
    print(f"start change_retention_category rootfolder_id{rootfolder_id} changing number of retention {len(retentions)}")

    # Get retention types for calculations
    retention_types:list[RetentionTypeDTO] = read_root_folder_cleanup_frequencies(rootfolder_id) 
    #convert the numeric retentions to a dict for fast lookup
    retention_types_dict:dict[int,RetentionTypeDTO] = {retention.id:retention for retention in retention_types}
    
    with Session(Database.get_engine()) as session:
        root_folder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        cleanup_status_date = root_folder.cleanup_status_date

        for retention in retentions:
            retention.expiration_date = cleanup_status_date+retention_types_dict[retention.retention_id].days_to_cleanup if (retention_types_dict[retention.retention_id].days_to_cleanup is not None) else None

        # Prepare bulk update data - much more efficient than Python loops
        # update with the retention information and reset the days_to_cleanup to 0 
        bulk_updates = [
            {
                "id": retention.folder_id,
                "retention_id": retention.retention_id,
                "pathprotection_id": retention.pathprotection_id,
                "expiration_date": retention.expiration_date
            }
            for retention in retentions
        ]
        session.bulk_update_mappings(FolderNodeDTO, bulk_updates)
        session.commit()
        
        print(f"end changeretentions rootfolder_id{rootfolder_id} changing number of retention {len(retentions)}")
        return {"message": f"Updated rootfolder {rootfolder_id} with {len(retentions)} retentions"}

# The following is called by the scheduler when it starts a new cleanup round
# extract all folders with an numeric retentiontypes 
# fail is any of the folders have a missing expiration date 
# if all ok then calculate the retention category for each folder with a numeric retention type based on days_until_expiration = (folder.expiration_date - root_folder.cleanup_status_date).days
def start_new_cleanup_cycle(rootfolder_id: int):
    with Session(Database.get_engine()) as session:
        root_folder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not root_folder:
            raise HTTPException(status_code=404, detail="RootFolder not found")
        
        # Update the cleanup_status_date to current date
        root_folder.cleanup_status_date = func.current_date()
        session.add(root_folder)
        session.commit()
        session.refresh(root_folder)

        #get the retention types for the rootfolder' simulation domain
        retention_types:list[RetentionTypeDTO] = read_retention_types(rootfolder_id)
        #get the numeric retention types in a dict for fast lookup
        retention_types_dict:dict[int,RetentionTypeDTO] = {retention.id:retention for retention in retention_types if retention.days_to_cleanup is not None}

        # extract all folder with rootfolder_id and a a numeric retentiontype
        folders = session.exec(
            select(FolderNodeDTO).where(
                (FolderNodeDTO.rootfolder_id == rootfolder_id) &
                (FolderNodeDTO.retention_id.in_(retention_types_dict.keys()))
            )
        ).all()

        # find all with missing expiration date and set to cleanup_status_date + days_to_analyse for the retention type
        # this should not happen so throw an exception
        folders_missing_expiration_date = [ folder for folder in folders if folder.expiration_date is None ]
        if len(folders_missing_expiration_date)>0:
            raise HTTPException(status_code=404, detail="Path protection not found")

        # now use the expiration dates to calculate the retention categories that the user will see in the webclient
        # @todo: this double loop will be very slow for large number of folders
        for folder in folders:
            days_until_expiration = (folder.expiration_date - root_folder.cleanup_status_date).days
            i = 0
            while( i<len(retention_types.values) and days_until_expiration > retention_types.values[i]) :
                i += 1
            folder.retention_id = retention_types.values[i].id if i<len(retention_types.values) else retention_types.values[-1].id           

        print(f"start_new_cleanup_cycle rootfolder_id: {rootfolder_id} updated retention of {len(folders)} folders" )

# The following is called when inserting or modifying simulations after a scan.
# The operations are: insert new folders, update existing folders, delete folders that no longer exist
# we will start by doing it in python, which will be slow for large number of folders - but this is not a user facing operation 
# We will later optimise using that "SQL Server has native support via hierarchyid". But do not have time now.

# The source data is a table with the following information pr simulation:
# [ filepath, last_modified_date, status ] the field status can be: "issue", "clean", ""

# Insert simulation:  is the most difficult part because the folder hierarchy must be resolved or created first
#   For the leaf FolderDTO set:
#   - retention_id to the None or the id for "clean", "issue" according to the status of each simulation
#   - path to the "filepath"
#   - the simulation' modified date
#   - the expiration_date to  modified_date + rootfolder.days_to_analyse

# Update the simulation: This case is for simulations where the filepath can be matched in caseinsensitive compare 
#  For the leaf FolderDTO set:
#   - retention_id to None or the id for "clean", "issue" according to the status of each simulation
#   - the simulation' modified date
#   - the expiration_date to modified_date + rootfolder.days_to_analyse

# Delete simulation: This case is for simulations in the database that were not found on disk in a full scan of the root folder
#   Set the retention_id to the id for "missing"

def insert_or_update_simulations(rootfolder_id: int, simulations: list[dict]):
    with Session(Database.get_engine()) as session:
        root_folder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not root_folder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        if root_folder.days_to_analyse is None:
            raise HTTPException(status_code=404, detail="Missing root_folder.days_to_analyse")

        #get the retention types for the rootfolder' simulation domain and convert to dict for fast lookup
        retention_types_dict:dict[str,RetentionTypeDTO] = {retention.name.lower():retention for retention in read_retention_types(rootfolder_id)}

        # extract all existing folders for the rootfolder
        existing_folders = session.exec(
            select(FolderNodeDTO).where(
                (FolderNodeDTO.rootfolder_id == rootfolder_id)
            )
        ).all()
        existing_folders_dict:dict[str,FolderNodeDTO] = {folder.path.lower():folder for folder in existing_folders}

        # Prepare bulk update and insert data
        class UpdateFolderDTO:
            id: int             #the folders id    
            retention_id: int 
            modified_date: str
            expiration_date: str 
        bulk_updates:list[UpdateFolderDTO] = []
        bulk_inserts:list[FolderNodeDTO] = []

        for sim in simulations:
            filepath = sim['filepath']
            last_modified_date = sim['last_modified_date']
            status = sim['status'].lower() if 'status' in sim and sim['status'] is not None else ""

            # Determine retention based on status
            if status == "clean":
                retention = retention_types_dict.get("clean")
            elif status == "issue":
                retention = retention_types_dict.get("issue")
            else:
                retention = None  # Default retention for normal simulations where the downstream cleanup will calculate the retention based on the expiration date

            # Calculate expiration date based on days_to_analyse
            expiration_date = last_modified_date + root_folder.days_to_analyse

            # Check if folder already exists (case-insensitive)
            existing_folder = existing_folders_dict.get(filepath.lower())

            if existing_folder:
                upd=UpdateFolderDTO(
                        id              = existing_folder.id,
                        retention_id    = retention.id if retention else None,
                        modified_date   = last_modified_date,
                        expiration_date = expiration_date
                    )
                bulk_updates.append(upd)
            else:
                # Insert new folder
                new_folder: FolderNodeDTO = FolderNodeDTO(
                    rootfolder_id=rootfolder_id,
                    name=path.basename(filepath),
                    path=filepath,
                    retention_id=retention.id if retention else None,
                    modified_date=last_modified_date,
                    expiration_date=expiration_date
                )
                bulk_inserts.append(new_folder)

        session.bulk_update_mappings(FolderNodeDTO, bulk_updates)
        session.commit()

        # Bulk insert new folders
        # @todo need to findout how to make hierarchical inserts
        insert_simulations(rootfolder_id, bulk_inserts)


#todo implement hierarchical inserts
def insert_simulations(rootfolder_id: int, inserts:list[FolderNodeDTO]):
    print(f"TBD start insert_simulations rootfolder_id{rootfolder_id} inserting number of folders {len(inserts)}")
    pass
