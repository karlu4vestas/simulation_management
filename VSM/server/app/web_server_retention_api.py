from os import path
from typing import NamedTuple, Literal
from datetime import date
from fastapi import FastAPI, Depends, Query, HTTPException
from sqlmodel import Session, select
from datamodel.dtos import RetentionTypeDTO, RootFolderDTO, FolderNodeDTO, RetentionUpdateDTO
from datamodel.db import Database
from sqlalchemy import create_engine, text, or_, func, case
from testdata.vts_generate_test_data import insert_test_data_in_db
from web_api import app, read_retention_types_by_domain_name, read_retention_types_by_domain_id, read_root_folder_cleanup_frequencies


#return retention ids that are used to classify simulations in a simulation domain during scanning
# for the "vts" domain these are "clean", "issue" and "missing" 
def get_scan_retentions() -> dict[str, RetentionTypeDTO]:
    retentions = read_retention_types_by_domain_name(simulation_domain_name="vts")
    scan_retentions = {retention.name.lower(): retention.id for retention in retentions if retention.name.lower() in ["clean", "issue", "missing"]}
    return scan_retentions

def get_retention_dict() -> dict[str, RetentionTypeDTO]:
    retentions = read_retention_types_by_domain_name(simulation_domain_name="vts")
    retention_dict = {retention.name.lower(): retention.id for retention in retentions}
    return retention_dict

# -------------------------- all calculations for expiration dates, retentions are done in below functions ---------
# The consistency of these calculations is highly critical to the system working correctly

# The following is called when the user has selected a retention category for a folder in the webclient
# change retentions category for a list of folders in a rootfolder and update the corresponding expiration date
# the expiration date of non-numeric retention is set to None
# the expiration date of numeric retention is set to cleanup_status_date + days_to_cleanup of the retention type
@app.post("/rootfolder/{rootfolder_id}/retentions")
def change_retention_category( rootfolder_id: int, retentions: list[RetentionUpdateDTO]):
    print(f"start change_retention_category rootfolder_id{rootfolder_id} changing number of retention {len(retentions)}")
    rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
    if not rootfolder:
        raise HTTPException(status_code=404, detail="RootFolder not found")

    # Get retention types for calculations
    retention_types:list[RetentionTypeDTO] = read_root_folder_cleanup_frequencies(rootfolder_id) 
    #convert the numeric retentions to a dict for fast lookup
    retention_types_dict:dict[int,RetentionTypeDTO] = {retention.id:retention for retention in retention_types}
    
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        cleanup_status_date = rootfolder.cleanup_status_date

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

# now use the expiration dates to calculate the retention categories that the user will see in the webclient
# @todo: this double loop will be very slow for large number of folders
def calc_numeric_retention_id(numeric_retention_types: dict[str,RetentionTypeDTO], rootfolder: RootFolderDTO, folder: FolderNodeDTO) -> int:
    days_until_expiration = (folder.expiration_date - rootfolder.cleanup_status_date).days
    i = 0
    while( i<len(numeric_retention_types.values) and days_until_expiration > numeric_retention_types.values[i]) :
        i += 1
    return numeric_retention_types.values[i].id if i<len(numeric_retention_types.values) else numeric_retention_types.values[-1].id           

# The following is called by the scheduler when it starts a new cleanup round
# extract all folders with an numeric retentiontypes 
# fail is any of the folders have a missing expiration date 
# if all ok then calculate the retention category for each folder with a numeric retention type based on days_until_expiration = (folder.expiration_date - root_folder.cleanup_status_date).days
def start_new_cleanup_cycle(rootfolder_id: int):
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")
        
        # Update the cleanup_status_date to current date
        rootfolder.cleanup_status_date = func.current_date()
        session.add(rootfolder)
        session.commit()
        session.refresh(rootfolder)

        #get the retention types for the rootfolder' simulation domain
        retention_types:list[RetentionTypeDTO] = read_retention_types_by_domain_id(rootfolder.simulation_domain_id)

        #get the numeric retention types in a dict for fast lookup
        numeric_retention_types_dict:dict[int,RetentionTypeDTO] = {retention.id:retention for retention in retention_types if retention.days_to_cleanup is not None}

        # extract all folders with rootfolder_id and a numeric retentiontype
        folders = session.exec(
            select(FolderNodeDTO).where(
                (FolderNodeDTO.rootfolder_id == rootfolder_id) &
                (FolderNodeDTO.retention_id.in_(numeric_retention_types_dict.keys()))
            )
        ).all()

        # raise an exception if there is any simulations wit a numeric retentiontype with missing expiration dates
        if len([ folder for folder in folders if folder.expiration_date is None ])>0:
            raise HTTPException(status_code=404, detail="Path protection not found")


        for folder in folders:
            folder.retention_id = calc_numeric_retention_id(numeric_retention_types_dict, rootfolder, folder)


        print(f"start_new_cleanup_cycle rootfolder_id: {rootfolder_id} updated retention of {len(folders)} folders" )


class FileInfo(NamedTuple):
    filepath: str
    modified: date
    id: int = None   # will be used during updates
    retention_id: int | None = None

def insert_or_update_simulation_in_db(rootfolder_id: int, simulations: list[FileInfo]):
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        if rootfolder.days_to_analyse is None:
            raise HTTPException(status_code=404, detail="Missing root_folder.days_to_analyse")

        # and the filepaths from the list of simulations
        existing_folders = session.exec(
            select(FolderNodeDTO).where(
                (FolderNodeDTO.rootfolder_id == rootfolder_id) &
                (FolderNodeDTO.path.in_( [sim.filepath.lower() for sim in simulations] ))
            )
        ).all()
        
        # Create a mapping from filepath to existing folder for fast lookup
        existing_folders: set[str] = set([ folder.path.lower() for folder in existing_folders ])
        insert_simulations: list[FileInfo] = [sim for sim in simulations if not sim.filepath.lower() in existing_folders]
        existing_folders   = None

        insertion_results:dict[str, int] = insert_simulations_in_db(rootfolder, insert_simulations)
        insert_simulations=None

        print(insertion_results)
        if {insertion_results.get("failed_path_count",0)} > 0:
            #print(f"Failed to insert some simulations for rootfolder {rootfolder_id}: {results.get('failed_paths', [])}")
            raise HTTPException(status_code=404, detail=f"Failed to insert some new paths: {insertion_results.get('failed_paths', [])}")


        update_results:dict[str, int] = update_simulation_attributes_in_db(rootfolder, simulations)

        # Commit all changes
        session.commit()

        print(update_results)
        insertion_results.update(update_results)
        return insertion_results

# The requirement to call update_simulation_attributes_in_db is that all simulations.filepath exist
def update_simulation_attributes_in_db(session:Session, rootfolder: RootFolderDTO, simulations: list[FileInfo]):
    if len(simulations) == 0: 
        return

    # Now: all the simulations filepaths exist in the database
    # We need to: refresh the attributes of the folder where simulations.filepath==folder.path
    # Assume also: path_retention_id is the id for the retention with the name "path"


    # 1) ALL: we need to update the modified_date
    #    because this is the date that is used to calculate the expiration_date

    # 2) ALL: we need to update the expiration_date if it is None or < modified_date + days_to_analyse because we are conservative about cleanup
    # if we update the expiration date then also update numeric retention types 

    # 3) update retention_id on subset: 
    #    3.1 for all retentions where ((sim.retention_id is not None) and (folder.retention_id!=path_retention_id)) we assign sim.retention_id to folder.retention_id
    #        because for all, except those under path protection, the user must be able to see if the simulation is clean or has an issue


    # Create a query using rootfolder.id and the filepaths from the list of simulations. 
    # Order the results to match the same order as the simulations list.
    # This is important for the subsequent update operation to maintain consistency.

    query = (
        select(FolderNodeDTO)
        .where(
            (FolderNodeDTO.rootfolder_id == rootfolder.id) &
            (FolderNodeDTO.path.in_([sim.filepath.lower() for sim in simulations]))
        )
        .order_by(
            case(
                {sim.filepath.lower(): index for index, sim in enumerate(simulations)},
                value=FolderNodeDTO.path,
                else_=len(simulations)
            )
        )
    )

    # Execute the ordered query to get results in the same order as simulations
    existing_folders = session.exec(query).all()
    #verify ordering as i am a little uncertain about the query
    equals_ordering = all(folder.path.lower() == sim.filepath.lower() for folder, sim in zip(existing_folders, simulations))
    if not equals_ordering:
        raise HTTPException(status_code=500, detail=f"Ordering of existing folders does not match simulations for rootfolder {rootfolder.id}")


    path_retention_id         = get_retention_dict().get("path")
    update_expiration_date    = [(folder.expiration_date is None or (sim.modified + rootfolder.days_to_analyse > folder.expiration_date)) 
                                 for folder, sim in zip(existing_folders, simulations)]
    update_numeric_retentions = [expiration_update and (folder.retention_id != path_retention_id) and (sim.retention_id is None)
                                 for folder, sim, expiration_update in zip(existing_folders, simulations, update_expiration_date)]

    #get the numeric retention types in a dict for fast lookup
    numeric_retention_types_dict:dict[int,RetentionTypeDTO] = {retention.id:retention for retention in retention_types if retention.days_to_cleanup is not None}

    # Prepare bulk update data for existing folders
    bulk_updates = [
        {
            "id": folder.id,
            "modified_date": sim.modified,
            "expiration_date": sim.modified + rootfolder.days_to_analyse if update_expiration else folder.expiration_date,
            "retention_id": sim.retention_id 
                            if (sim.retention_id is not None and folder.retention_id != path_retention_id) 
                            else folder.retention_id 
                            if not update_numeric_retention 
                            else calc_numeric_retention_id(numeric_retention_types_dict, rootfolder, folder),
        }
        for folder, sim, update_expiration, update_numeric_retention in zip(existing_folders, simulations, update_expiration_date, update_numeric_retentions)  
    ]
            
    # Execute bulk update
    session.bulk_update_mappings(FolderNodeDTO, bulk_updates)

    return {"updated_count": len(bulk_updates)}


# Helper functions for hierarchical insert

def normalize_and_split_path(filepath: str) -> list[str]:
    """
    Normalize path to forward slashes and split into segments.
    Handles edge cases like empty paths, root paths, trailing slashes.
    
    Examples:
    - "/root/child/grandchild" -> ["root", "child", "grandchild"]
    - "\\root\\child\\" -> ["root", "child"]
    - "root/child/" -> ["root", "child"]
    - "" -> []
    """
    if not filepath or filepath.strip() == "":
        return []
    
    # Normalize to forward slashes
    normalized = filepath.replace("\\", "/")
    
    # Remove leading and trailing slashes, then split
    segments = [segment for segment in normalized.strip("/").split("/") if segment.strip()]
    
    return segments


def find_existing_node(session: Session, rootfolder_id: int, parent_id: int, name: str) -> FolderNodeDTO | None:
    """
    Find existing node by parent_id and name (case-insensitive) within a rootfolder.
    
    Args:
        session: Database session
        rootfolder_id: ID of the root folder
        parent_id: ID of the parent node (0 for root level)
        name: Name of the node to find (case-insensitive)
    
    Returns:
        FolderNodeDTO if found, None otherwise
    """
    return session.exec(
        select(FolderNodeDTO).where(
            (FolderNodeDTO.rootfolder_id == rootfolder_id) &
            (FolderNodeDTO.parent_id == parent_id) &
            (func.lower(FolderNodeDTO.name) == name.lower())
        )
    ).first()


def generate_path_ids(parent_path_ids: str, node_id: int) -> str:
    """
    Generate path_ids for a new node based on parent information.
    Format: parent_ids + "/" + node_id (e.g., "0/1/2")
    For root nodes (parent_id=0), just return the node_id as string.
    
    Args:
        parent_path_ids: The path_ids of the parent node
        node_id: The ID of the current node
    
    Returns:
        String representation of the path_ids
    """
    if not parent_path_ids or parent_path_ids == "0":  # is this the root node?
        return str(node_id)
    else:
        return f"{parent_path_ids}/{node_id}"


def insert_hierarchy_for_path(session: Session, rootfolder_id: int, filepath: str) -> int:
    """
    Insert missing hierarchy for a single filepath and return the leaf node ID.
    
    Args:
        session: Database session
        rootfolder_id: ID of the root folder
        filepath: The complete file path to ensure exists
    
    Returns:
        ID of the leaf node (final segment in the path)
    
    Raises:
        ValueError: If filepath is invalid or empty
        HTTPException: If database constraints are violated
    """
    segments = normalize_and_split_path(filepath)
    
    if not segments:
        raise ValueError(f"Invalid or empty filepath: {filepath}")
    
    if rootfolder_id is None or rootfolder_id <= 0:
        raise ValueError(f"Invalid rootfolder_id: {rootfolder_id}")
    
    current_parent_id = 0  # Start from root level
    current_parent_path_ids = "0"
    current_path_segments = []
    
    for segment in segments:
        # Validate segment name
        if not segment or segment.strip() == "":
            raise ValueError(f"Invalid empty segment in filepath: {filepath}")
            
        current_path_segments.append(segment)
        
        # Check if node already exists at this level
        existing_node = find_existing_node(session, rootfolder_id, current_parent_id, segment)
        
        if existing_node:
            # Node exists, move to next level
            current_parent_id = existing_node.id
            current_parent_path_ids = existing_node.path_ids
        else:
            # Node doesn't exist, create it
            new_node = FolderNodeDTO(
                rootfolder_id=rootfolder_id,
                parent_id=current_parent_id,
                name=segment,
                path="/".join(current_path_segments),  # Full path up to this segment
                path_ids=""  # Will be set after getting the ID
            )
            
            try:
                session.add(new_node)
                session.flush()  # Flush to get the ID without committing
                
                # Verify we got an ID
                if new_node.id is None:
                    raise HTTPException(status_code=500, detail=f"Failed to generate ID for new node: {segment}")
                
                # Now set the path_ids using the generated ID
                new_node.path_ids = generate_path_ids(current_parent_path_ids, new_node.id)
                
                # Move to next level
                current_parent_id = new_node.id
                current_parent_path_ids = new_node.path_ids
                
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Database error creating node '{segment}': {str(e)}")
    
    return current_parent_id  # Return the ID of the leaf node


#todo implement hierarchical inserts
def insert_simulations_in_db(rootfolder: RootFolderDTO, simulations: list[FileInfo]):
    """
    Insert missing hierarchy for all simulation filepaths.
    This function only creates the folder structure, attributes will be updated separately.
    """
    if not simulations:
        return {"inserted_hierarchy_count": 0, "failed_path_count": 0, "failed_paths": []}
        
    print(f"start insert_simulations rootfolder_id {rootfolder.id} inserting hierarchy for {len(simulations)} folders")
    
    with Session(Database.get_engine()) as session:
        inserted_count = 0
        failed_paths = []
        
        for sim in simulations:
            try:
                # Insert hierarchy for this filepath (creates missing nodes)
                leaf_node_id = insert_hierarchy_for_path(session, rootfolder.id, sim.filepath)
                inserted_count += 1
                
            except Exception as e:
                print(f"Error inserting hierarchy for path '{sim.filepath}': {e}")
                failed_paths.append(sim.filepath)
                # Continue with other paths rather than failing completely
                continue
        
        # Commit all insertions
        session.commit()
        
        print(f"end insert_simulations rootfolder_id {rootfolder.id} - successfully inserted hierarchy for {inserted_count}/{len(simulations)} paths, {len(failed_paths)} failed")
        
    return {"inserted_hierarchy_count": inserted_count, "failed_path_count": len(failed_paths), "failed_paths": failed_paths}
