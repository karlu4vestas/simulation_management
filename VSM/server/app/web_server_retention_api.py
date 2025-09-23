from dataclasses import dataclass
from typing import List, Optional
from datetime import date, timedelta
from bisect import bisect_left
from fastapi import HTTPException
from sqlmodel import Session, select
from datamodel.dtos import FolderTypeDTO, RetentionTypeDTO, RootFolderDTO, FolderNodeDTO, RetentionUpdateDTO
from datamodel.db import Database
from sqlalchemy import func, case
from app.web_api import app, read_folder_type_dict_pr_domain_id, read_pathprotections, read_rootfolder_retention_type_dict, read_rootfolder_numeric_retentiontypes_dict 
from datamodel.dtos import PathProtectionDTO, Retention 

class ConvertExpirationDateToRetentionCalculator:
    def __init__(self, numeric_retention_types: dict[str, RetentionTypeDTO], cleanup_status_date:date):
        self.retention_id_dict      = {retention.id: retention for retention in numeric_retention_types.values()}
        self.retention_ids          = [retention.id for retention in numeric_retention_types.values()]
        self.retention_durations    = [retention.days_to_cleanup for retention in numeric_retention_types.values()]
        self.cleanup_status_date    = cleanup_status_date

    def convert_expiration_date_to_retention_id(self, expiration_date: date) -> int:
        days_to_expiration = (expiration_date - self.cleanup_status_date).days
        # find first index where retention_duration[idx] >= days_until_expiration
        idx = bisect_left(self.retention_durations, days_to_expiration)
        # if days_until_expiration is greater than every threshold, return last index
        return self.retention_ids[idx] if idx < len(self.retention_durations) else self.retention_ids[len(self.retention_durations) - 1]


class ConvertRetentionIdToExpirationDateCalculator:
    def __init__(self, numeric_retention_types: dict[str, RetentionTypeDTO], cleanup_status_date:date):
        self.retention_id_dict      = {retention.id: retention for retention in numeric_retention_types.values()}
        self.retention_ids          = [retention.id for retention in numeric_retention_types.values()]
        self.retention_durations    = [retention.days_to_cleanup for retention in numeric_retention_types.values()]
        self.cleanup_status_date    = cleanup_status_date

    def convert_id_to_expiration_date(self, retention_id: int) -> date | None:
        retention = self.retention_id_dict.get(retention_id, None)
        return self.cleanup_status_date + timedelta(days=retention.days_to_cleanup) if retention else None

class path_protection_engine:
    sorted_protections:List[tuple[str, int]]
    def __init__(self, protections: List[PathProtectionDTO], path_retention_id:int, default_path_protection_id: int = 0):
        #self.match_prefix_id = make_prefix_id_matcher(protections)
        self.sorted_protections = [(dto.path.lower().replace('\\', '/').rstrip('/'), dto.id) for dto in protections]
        self.path_retention_id = path_retention_id
        self.default_protection_id = default_path_protection_id

        self.sorted_protections: List[tuple[str, int]] = sorted(
            self.sorted_protections,
            key=lambda item: item[0].count('/'),
            reverse=True
        )

    # returns: Retention(retention_id, pathprotection_id) or None if not found
    def match(self, path:str) -> Optional[Retention]:
        """
        Returns a function that, given a path, returns the id of the longest
        protection that is a prefix of the path (with '/' boundary) or None.
        """
        path = (path or "").rstrip('/').lower().replace('\\', '/')
        for pat, pid in self.sorted_protections:  # short-circuits on first (longest) match
            if path == pat or path.startswith(pat + "/"):  # avoids "R1" matching "R10/..."
                return Retention( self.path_retention_id, pid)
        return None
    
@dataclass    
class FileInfo:
    filepath: str
    modified: date
    id: int = None   # will be used during updates
    retention_id: int | None = None



# -------------------------- all calculations for expiration dates, retentions are done in below functions ---------
# The consistency of these calculations is highly critical to the system working correctly

# The following is called when the user has selected a retention category for a folder in the webclient
# change retentions category for a list of folders in a rootfolder and update the corresponding expiration date
# the expiration date of non-numeric retention is set to None
# the expiration date of numeric retention is set to cleanup_status_date + days_to_cleanup of the retention type
@app.post("/rootfolder/{rootfolder_id}/retentions")
def change_retention_category(rootfolder_id: int, retentions: list[RetentionUpdateDTO]):
    print(f"start change_retention_category rootfolder_id{rootfolder_id} changing number of retention {len(retentions)}")
    
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        # the cleanup_status_date must be set for calculation of retention.expiration_date. It could make sens to set path retentions before the first cleanup round. 
        # However, when a cleanup round is started they have time at "rootfolder.cycletime" to adjust retention
        if not rootfolder.cleanup_status_date:
            raise HTTPException(status_code=400, detail="RootFolder cleanup_status_date not set")

        # Get retention types for calculations
        numeric_retention_calculator: ConvertExpirationDateToRetentionCalculator = ConvertExpirationDateToRetentionCalculator(read_rootfolder_numeric_retentiontypes_dict(rootfolder_id), rootfolder.cleanup_status_date) 

        # Update expiration dates. 
        # if the retention is numeric then the conversion set the expiration_date to status_date+the retentions.days_to_cleanup
        # if not it returns None
        for retention in retentions:
            retention.expiration_date = numeric_retention_calculator.convert_id_to_expiration_date(retention.retention_id)
           
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
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")
        
        # Update the cleanup_status_date to current date
        rootfolder.cleanup_status_date = func.current_date()
        session.add(rootfolder)
        session.commit()
        session.refresh(rootfolder)

        #get the numeric retention types in a dict for fast lookup
        numeric_retention_types_dict:dict[int,RetentionTypeDTO] = read_rootfolder_numeric_retentiontypes_dict(rootfolder.id)

        # extract all folders with rootfolder_id and a numeric retentiontype
        folders = session.exec(
            select(FolderNodeDTO).where(
                (FolderNodeDTO.rootfolder_id == rootfolder_id) &
                (FolderNodeDTO.retention_id.in_(numeric_retention_types_dict.keys()))
            )
        ).all()

        # raise an exception if there is any simulations with a numeric retentiontype with missing expiration dates
        if any(folder.expiration_date is None for folder in folders):
            raise HTTPException(status_code=404, detail="Path protection not found")


        # Prepare calculation of numeric retention_id
        # Here we are sure that because it was just set above to current_date
        numeric_retention_calculator = ConvertExpirationDateToRetentionCalculator(read_rootfolder_numeric_retentiontypes_dict(rootfolder.id), rootfolder.cleanup_status_date)
        for folder in folders:
            folder.retention_id = numeric_retention_calculator.convert_expiration_date_to_retention_id(folder.expiration_date)

        #@TODO  must we make a bulk commit here to save the modified retention ids ?
        session.commit()
        print(f"start_new_cleanup_cycle rootfolder_id: {rootfolder_id} updated retention of {len(folders)} folders" )

# In this function we will reduce the upda to those simulations that have changed or are new
# The call can therefore call with all simulations modified or not since last scan
def insert_or_update_simulation_in_db(rootfolder_id: int, simulations: list[FileInfo]):
    #here we can remove all exising simulation if nothing chnges for them
    return insert_or_update_simulation_in_db_internal(rootfolder_id, simulations)



#the function is slow so before calling this function remove all path that does not provide new information. new simulation, new modified date, new retention
def insert_or_update_simulation_in_db_internal(rootfolder_id: int, simulations: list[FileInfo]):
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        if rootfolder.cycletime is None:
            raise HTTPException(status_code=400, detail="Missing root_folder.days_to_analyse")

        # and the filepaths from the list of simulations
        existing_folders_query = session.exec(
            select(FolderNodeDTO).where(
                (FolderNodeDTO.rootfolder_id == rootfolder_id) &
                (FolderNodeDTO.path.in_([sim.filepath.lower() for sim in simulations]))
            )
        ).all()
        
        # Create a mapping from filepath to existing folder for fast lookup
        existing_folders: set[str] = set([folder.path.lower() for folder in existing_folders_query])
        insert_simulations: list[FileInfo] = [sim for sim in simulations if sim.filepath.lower() not in existing_folders]
        existing_folders = None

        insertion_results: dict[str, int] = insert_simulations_in_db(rootfolder, insert_simulations)
        insert_simulations = None

        print(insertion_results)
        if insertion_results.get("failed_path_count", 0) > 0:
            #print(f"Failed to insert some simulations for rootfolder {rootfolder_id}: {results.get('failed_paths', [])}")
            raise HTTPException(status_code=500, detail=f"Failed to insert some new paths: {insertion_results.get('failed_paths', [])}")

        update_results: dict[str, int] = update_simulation_attributes_in_db_internal(session, rootfolder, simulations)

        # Commit all changes
        session.commit()

        print(update_results)
        insertion_results.update(update_results)
        return insertion_results


# Do not use this function directly . use update_simulation_attributes_in_db so that it can filter out all simulation that chagnes nothing
def update_simulation_attributes_in_db_internal(session: Session, rootfolder: RootFolderDTO, simulations: list[FileInfo]):
    if len(simulations) == 0: 
        return {"updated_count": 0}
    # Now: all the simulations filepaths exist in the database

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
    #verify ordering as i am a little uncertain about the behaviour of the query
    equals_ordering = all(folder.path.lower() == sim.filepath.lower() for folder, sim in zip(existing_folders, simulations))
    if not equals_ordering:
        raise HTTPException(status_code=500, detail=f"Ordering of existing folders does not match simulations for rootfolder {rootfolder.id}")

    folder_types:dict[str,FolderTypeDTO] = read_folder_type_dict_pr_domain_id(rootfolder.simulation_domain_id)
    vts_simulation_nodetype_id =folder_types.get("vts_simulation", None).id
    if not vts_simulation_nodetype_id:
        raise HTTPException(status_code=500, detail=f"Unable to retrieve node_type_id=vts_simulation for {rootfolder.id}")


    # prepare the path_protection_engine. it returns the id to the PathProtectionDTO and prioritizes the most specific path (most segments) if any.
    path_retention_dict:dict[str, RetentionTypeDTO] = read_rootfolder_retention_type_dict(rootfolder.id)
    if path_retention_dict.get("path", None) is None:
        raise HTTPException(status_code=500, detail=f"Unable to retrieve node_type_id=vts_simulation for {rootfolder.id}")
    path_matcher:path_protection_engine = path_protection_engine(read_pathprotections(rootfolder.id), path_retention_dict.get("path", None).id)

    #Prepare calculation of numeric retention_id.  
    #@TODO as implemented now this requires a cleanup_status_date which is not always the set. because we will often scan before the cleanup is activated
    # ???????? may if not set then we can use the current date as a proxy like in start_new_cleanup_cycle
    numeric_retention_calculator = ConvertExpirationDateToRetentionCalculator( read_rootfolder_numeric_retentiontypes_dict(rootfolder.id), rootfolder.cleanup_status_date)

    # We need to: refresh the attributes of the folders where rootfolder.id match and simulations.filepath==folder.path
    # 1) ALL: we need to update the modified_date
    #    because this is the date that is used to calculate the expiration_date

    # 2) ALL: we need to update the expiration_date if it is None or < modified_date + days_to_analyse because we are conservative about cleanup
    # if we update the expiration date and the retention type is numeric then update it   

    # 3) update retention_id on subset: 
    #    Notice 1) that the caller will come with retention_id for "clean" and "issue" else retention_id=None. 
    #           2) at the sametime pathretention has priority over all other retentions
    #    That is use caller' retention_id if ((sim.retention_id is not None) and (folder.retention_id!=path_retention_id))
    #    else if the (retention_id is not numeric) then recalculate the existing retention_id if expiration date changed

    # Prepare bulk update data for existing folders. this will be slow ???
    bulk_updates = []
    for folder, sim in zip(existing_folders, simulations):
        retention:Retention = path_matcher.match(folder.path)
        #retention:Retention = Retention(folder.retention_id if path_protection_id==0 else path_retention_id , path_protection_id)

        # i do not see how folder.expiration_date can be None but just in case
        expiration_date = max(folder.expiration_date, sim.modified + timedelta(days=rootfolder.cycletime)) if folder.expiration_date is not None else sim.modified + timedelta(days=rootfolder.cycletime)

        if retention is None: #if not path_retention
            if sim.retention_id is not None: #clean or issue from caller
                retention = Retention(sim.retention_id)
            elif expiration_date != folder.expiration_date and retention.retention_id in numeric_retention_calculator.retention_ids:
                # recalculate the existing retention if expiration_date changed and the retention is numeric
                retention = Retention(numeric_retention_calculator.calc_retention_id((expiration_date - rootfolder.cleanup_status_date).days))
            else: # use the existing retention
                retention = Retention(folder.retention_id)


        bulk_updates.append({
            "id": folder.id,
            "nodetype_id": vts_simulation_nodetype_id,
            "modified_date": sim.modified,
            "expiration_date": expiration_date,
            "retention_id": retention.retention_id,
            "path_protection_id": retention.path_protection_id
        })
    
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
                leaf_node_id = insert_hierarchy_for_one_filepath(session, rootfolder.id, sim.filepath)
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


def insert_hierarchy_for_one_filepath(session: Session, rootfolder_id: int, filepath: str) -> int:
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
    if rootfolder_id is None or rootfolder_id <= 0:
        raise ValueError(f"Invalid rootfolder_id: {rootfolder_id}")

    segments = normalize_and_split_path(filepath) #can probably be optimized to: pathlib.Path(filepath).as_posix().split('/') because the domains part of the folder should be in the rootfolder  
    if not segments:
        raise ValueError(f"Invalid or empty filepath: {filepath}")
    
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