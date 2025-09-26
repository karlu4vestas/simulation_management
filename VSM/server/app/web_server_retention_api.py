from dataclasses import dataclass
from typing import List, Optional
from datetime import date, timedelta
from bisect import bisect_left
from fastapi import HTTPException
from sqlmodel import Session, select
from datamodel.dtos import CleanupConfiguration, FolderTypeDTO, FolderTypeEnum, RetentionTypeDTO, RootFolderDTO, FolderNodeDTO, RetentionUpdateDTO
from datamodel.db import Database
from sqlalchemy import func, case
from app.web_api import app, read_folder_type_dict_pr_domain_id, read_pathprotections, read_rootfolder_retention_type_dict, read_rootfolder_numeric_retentiontypes_dict 
from datamodel.dtos import PathProtectionDTO, Retention 

class RetentionCalculator:
    def __init__(self, numeric_retention_types: dict[str, RetentionTypeDTO], cleanup_round_start_date:date, cycletime_days: int):
        if not cleanup_round_start_date or not numeric_retention_types or not cycletime_days or cycletime_days <= 0:
            raise ValueError("cleanup_round_start_date, at least one numeric retention type and cycletime must be set for RetentionCalculator to work")
        self.retention_id_dict        = {retention.id: retention for retention in numeric_retention_types.values()}
        self.retention_ids            = [retention.id for retention in numeric_retention_types.values()]
        self.retention_durations      = [retention.days_to_cleanup for retention in numeric_retention_types.values()]
        self.cleanup_round_start_date = cleanup_round_start_date
        self.cycle_time               = timedelta(days=cycletime_days)

    # adjust the expiration_date using the cleanup_configuration and retentiontype 
    # This is what you what when updating the simulations retentiontype using the webclient
    #
    # if non numeric retention then set expiration_date to None
    # if numeric retention then set expiration_date to cleanup_round_start_date + days_to_cleanup of the retention type
    def adjust_expiration_date_from_cleanup_configuration_and_retentiontype(self, retention: Retention) -> Retention:
        retentiontype = self.retention_id_dict.get(retention.retention_id, None)
        if retentiontype is None: # not a numeric retention
            retention.expiration_date = None
        else:
            retention.expiration_date = self.cleanup_round_start_date + timedelta(days = retentiontype.days_to_cleanup)
        return retention


    # adjust expiration_date before retention_id using the cleanup_configuration and modified_date
    #   - This is what you what when updating the simulation from a scan of its metadata
    # if modified_date is None then the expiration_date is only change for non numeric retentions. for numeric retention we adjust the retention_id
    #
    # if non numeric retention then set expiration_date to None
    # if numeric retention then 
    #   use the modified_date to update expiration_date if it will result in longer retention (expiration_date)
    #   update numeric retention_id to the new expiration date. The retention_id is calculated; even if the expiration date did not change to be sure there is no inconsistency
    def adjust_from_cleanup_configuration_and_modified_date(self, retention:Retention, modified_date:date=None) -> Retention:
        retentiontype = self.retention_id_dict.get(retention.retention_id, None)
        if retentiontype is None: # not a numeric retention
            retention.expiration_date = None
        else:
            if modified_date is not None:
                retention.expiration_date = modified_date + self.cycle_time if retention.expiration_date is None else max(retention.expiration_date, modified_date + self.cycle_time)

            if retention.expiration_date is None:
                raise ValueError("retention.expiration_date is None in RetentionCalculator adjust_to_cleanup_round")
            else:
                days_to_expiration = (retention.expiration_date - self.cleanup_round_start_date).days
                # find first index where retention_duration[idx] >= days_until_expiration
                idx = bisect_left(self.retention_durations, days_to_expiration)

                # if days_until_expiration is greater than every threshold, return last index
                retention.retention_id = self.retention_ids[idx] if idx < len(self.retention_durations) else self.retention_ids[len(self.retention_durations) - 1]
        return retention

    # adjust numeric retention_type to the new cleanup_configuration
    #   - This is what you what when starting a new cleanup round
    #
    # if non numeric retention then set expiration_date to None
    # if numeric retention then update numeric retention_id to the expiration_date and the cleanup_configuration
    def adjust_retentions_from_cleanup_configuration(self, retention:Retention) -> Retention:
        return self.adjust_from_cleanup_configuration_and_modified_date(retention)

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
    modified_date: date
    id: int = None   # will be used during updates
    retention_id: int | None = None
    nodetype_id: int


# -------------------------- all calculations for expiration dates, retentions are done in below functions ---------
# The consistency of these calculations is highly critical to the system working correctly


# The following can be called when the securefolder' cleanup configuration is fully defined meaning that rootfolder.cleanup_frequency and rootfolder.cycletime msut be set
# it will adjust the expiration dates to the user selected retention categories in the webclient
#   the expiration date for non-numeric retentions is set to None
#   the expiration date for numeric retention is set to cleanup_round_start_date + days_to_cleanup for the user selected retention type
@app.post("/rootfolder/{rootfolder_id}/retentions")
def change_retention_category(rootfolder_id: int, retentions: list[RetentionUpdateDTO]):
    print(f"start change_retention_category rootfolder_id{rootfolder_id} changing number of retention {len(retentions)}")
    
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        cleanup_config: CleanupConfiguration = rootfolder.get_cleanup_configuration()
        # the cleanup_round_start_date must be set for calculation of retention.expiration_date. It could make sens to set path retentions before the first cleanup round. 
        # However, when a cleanup round is started they have time at "rootfolder.cycletime" to adjust retention
        if not cleanup_config.can_start_cleanup():
            raise HTTPException(status_code=400, detail="The rootFolder's CleanupConfiguration is is missing cleanup_frequency, cleanup_round_start_date or cycletime ")

        # Get retention types for calculations
        retention_calculator: RetentionCalculator = RetentionCalculator(read_rootfolder_numeric_retentiontypes_dict(rootfolder_id), cleanup_config) 

        # Update expiration dates. 
        for retention in retentions:
            retention.set_retention( retention_calculator.adjust_expiration_date_from_cleanup_configuration_and_retentiontype(retention.get_retention()) )
           
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


# The following is called by the scheduler (or webclient through update_rootfolder_cleanup_configuration at present) to starts a new cleanup round
# That is the retentions must be adapted to the new cleanup_configuration using the existing expiration dates
#
# The following update will happen
#   - folders with numeric retentiontypes: calculation of retention category and expiration dates
#   - folders with non-numeric retentiontypes: The expiration date is set to None in order to show that cleanup must not be done
def start_new_cleanup_cycle(rootfolder_id: int):
    with Session(Database.get_engine()) as session:
        rootfolder = session.exec(select(RootFolderDTO).where(RootFolderDTO.id == rootfolder_id)).first()
        if not rootfolder:
            raise HTTPException(status_code=404, detail="RootFolder not found")

        cleanup_config: CleanupConfiguration = rootfolder.get_cleanup_configuration()
        if not cleanup_config.can_start_cleanup():
            return {"message": f"Cannot start cleanup for rootfolder {rootfolder_id} due to invalid cleanup configuration {cleanup_config}"}
        elif cleanup_config.cleanup_round_start_date is None:
            cleanup_config.cleanup_round_start_date = func.current_date()
            rootfolder.set_cleanup_configuration(cleanup_config)

        # Update the cleanup_round_start_date to current date
        session.add(rootfolder)
        session.commit()
        session.refresh(rootfolder)

        # Get retention types for calculations
        retention_calculator: RetentionCalculator = RetentionCalculator(read_rootfolder_numeric_retentiontypes_dict(rootfolder_id), cleanup_config) 

        # extract all folders with rootfolder_id. 
        # We can possibly optimise by 
        #   setting expiration date to None for non-numeric retention types
        #   only selecting folders with a numeric retentiontype for further processing
        folders = session.exec( select(FolderNodeDTO).where( FolderNodeDTO.rootfolder_id == rootfolder_id) ).all()


        # Update retentions 
        for folder in folders:
            folder.set_retention( retention_calculator.adjust_retentions_from_cleanup_configuration( folder.get_retention()) )
            session.add(folder)

        #@TODO  must we make a bulk commit here to save the modified retention ids ?
        session.commit()
        print(f"start_new_cleanup_cycle rootfolder_id: {rootfolder_id} updated retention of {len(folders)} folders" )

# This function will insert new simulations and update existing simulations
#  - I can be called with all simulations modified or not since last scan, because the list of simulations will be reduced to simulations that have changed or are new
#  - in all cases the caller's change to modify, retention_id (not None retention_id) will be applied and path protection will be applied and takes priority if any
#  - the expiration date and numeric retentions will:
#       if cleanup is active then recalculate them 
#       if cleanup is inactive then ignore them 
def insert_or_update_simulation_in_db(rootfolder_id: int, simulations: list[FileInfo]):
    #here we can remove all existing simulation if nothing changes for them
    return insert_or_update_simulation_in_db_internal(rootfolder_id, simulations)



#the function is slow so before calling this function remove all simulation that does not provide new information. new simulation, new modified date, new retention
def insert_or_update_simulation_in_db_internal(rootfolder_id: int, simulations: list[FileInfo]):
    if len(simulations) == 0: 
        return {"nothing to do. simulation count": 0}

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
        existing_folders: set[str]          = set([folder.path.lower() for folder in existing_folders_query])
        insert_simulations: list[FileInfo]  = [sim for sim in simulations if sim.filepath.lower() not in existing_folders]
        existing_folders                    = None

        insertion_results: dict[str, int]   = insert_simulations_in_db(rootfolder, insert_simulations)
        insert_simulations = None

        print(insertion_results)
        if insertion_results.get("failed_path_count", 0) > 0:
            print(f"Failed to insert some simulations for rootfolder {rootfolder_id}: {insertion_results.get('failed_paths', [])}")
            raise HTTPException(status_code=500, detail=f"Failed to insert some new paths: {insertion_results.get('failed_paths', [])}")

        update_results: dict[str, int] = update_simulation_attributes_in_db_internal(session, rootfolder, simulations)

        # Commit all changes
        session.commit()

        print(update_results)
        insertion_results.update(update_results)
        return insertion_results


# This function update the attributes of existing simulations
# The function is slow so before calling this function remove all simulation that does not provide new information. new simulation, new modified date, new retention
# The update implements the attribute changes described in insert_or_update_simulation_in_db
def update_simulation_attributes_in_db_internal(session: Session, rootfolder: RootFolderDTO, simulations: list[FileInfo]):
    # retrieve the simulations in the rootfolder and ensure that the order is the same as in the simulations list 
    # This is important for the subsequent update operation to maintain consistency.
    query = (
        select(FolderNodeDTO)
        .where(
            (FolderNodeDTO.rootfolder_id == rootfolder.id) &
            (FolderNodeDTO.path.in_([sim.filepath.lower() for sim in simulations])) # must be wrong because parent folders will also match
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
    if len(existing_folders) != len(simulations):
        raise HTTPException(status_code=500, detail=f"Mismatch in existing folders ({len(existing_folders)}) and simulations ({len(simulations)}) for rootfolder {rootfolder.id}")
    
    #verify ordering as i am a little uncertain about the behaviour of the query
    equals_ordering = all(folder.path.lower() == sim.filepath.lower() for folder, sim in zip(existing_folders, simulations))
    if not equals_ordering:
        raise HTTPException(status_code=500, detail=f"Ordering of existing folders does not match simulations for rootfolder {rootfolder.id}")


    # prepare the path_protection_engine. it returns the id to the PathProtectionDTO and prioritizes the most specific path (most segments) if any.
    path_retention_dict:dict[str, RetentionTypeDTO] = read_rootfolder_retention_type_dict(rootfolder.id)
    if path_retention_dict.get("path", None) is None:
        raise HTTPException(status_code=500, detail=f"Unable to retrieve node_type_id=vts_simulation for {rootfolder.id}")
    path_matcher:path_protection_engine = path_protection_engine(read_pathprotections(rootfolder.id), path_retention_dict.get("path", None).id)

    # do we have a cleanup configuration that allows start of cleanup
    can_start_cleanup:bool = rootfolder.get_cleanup_configuration().can_start_cleanup()

    #Prepare calculation of numeric retention_id.  
    retention_calculator = RetentionCalculator( read_rootfolder_numeric_retentiontypes_dict(rootfolder.id), rootfolder.get_cleanup_configuration() ) if can_start_cleanup else None


    # Prepare bulk update data for existing folders
    bulk_updates = []
    for folder, sim in zip(existing_folders, simulations):
        modified_date:date       = folder.modified_date
        retention:Retention      = folder.get_retention()
        path_retention:Retention = path_matcher.match(folder.path)
        sim_retention:Retention  = Retention(sim.retention_id) if sim.retention_id is not None else None

        # step 1: set changes to non numeric retentions with path protection having priority
        if not path_retention is None:
            retention = path_retention
        elif not sim_retention is None: #retention that the caller want to set. Could be #retention_id for "clean" or "issue"
            retention = sim_retention
        elif modified_date != sim.modified_date: 
            modified_date = sim.modified_date
            #step 2: if the modified date changed we need to update the retention because it might be numeric
            if retention_calculator is not None: #is cleanup active then retention_calculator is exist
                retention = retention_calculator.adjust_from_cleanup_configuration_and_modified_date(retention, sim.modified_date)
        else: 
            # all new simulation are created with modified_date=None in insert_simulations_in_db and are therefore handled above 
            # ANYWAY lets process this case just to be sure. we can optimise later
            if retention_calculator is not None:
                retention = retention_calculator.adjust_from_cleanup_configuration_and_modified_date(retention, modified_date)
            
        bulk_updates.append({
            "id": folder.id,
            "modified_date": sim.modified_date,
            "expiration_date": retention.expiration_date,
            "retention_id": retention.retention_id,
            "pathprotection_id": retention.pathprotection_id
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

    nodetypes:dict[str,FolderTypeDTO] = read_folder_type_dict_pr_domain_id(rootfolder.simulation_domain_id)
    innernode_type_id =nodetypes.get(FolderTypeEnum.INNERNODE, None).id
    if not innernode_type_id:
        raise HTTPException(status_code=500, detail=f"Unable to retrieve node_type_id=innernode for {rootfolder.id}")

    with Session(Database.get_engine()) as session:
        inserted_count = 0
        failed_paths = []
        
        for sim in simulations:
            try:
                # Insert hierarchy for this filepath (creates missing nodes)
                leaf_node_id = insert_hierarchy_for_one_filepath(session, rootfolder.id, sim, innernode_type_id)
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


def insert_hierarchy_for_one_filepath(session: Session, rootfolder_id: int, simulation: FileInfo, innernode_type_id: int) -> int:
    """
    Insert missing hierarchy for a single filepath and return the leaf node ID.
    
    Args:
        session: Database session
        rootfolder_id: ID of the root folder
        filepath: The complete file path to ensure exists
        innernode_type_id: The nodetype_id to use for inner nodes where as the node for the full simulation filepath will use simulation.nodetype_id
    Returns:
        ID of the leaf node (final segment in the path)
    
    Raises:
        ValueError: If filepath is invalid or empty
        HTTPException: If database constraints are violated
    """
    if rootfolder_id is None or rootfolder_id <= 0:
        raise ValueError(f"Invalid rootfolder_id: {rootfolder_id}")

    segments = normalize_and_split_path(simulation.filepath) #can probably be optimized to: pathlib.Path(filepath).as_posix().split('/') because the domains part of the folder should be in the rootfolder  
    if not segments:
        raise ValueError(f"Invalid or empty filepath: {simulation.filepath}")
    
    current_parent_id = 0  # Start from root level
    current_parent_path_ids = "0"
    current_path_segments = []

    for index, segment in enumerate(segments):
        # Validate segment name
        if not segment or segment.strip() == "":
            raise ValueError(f"Invalid empty segment in filepath: {simulation.filepath}")
            
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
                nodetype_id = innernode_type_id if index < len(segments) - 1 else simulation.nodetype_id,  # Use innernode_type_id for intermediate nodes
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