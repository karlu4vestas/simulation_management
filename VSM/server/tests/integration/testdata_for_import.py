import random
from enum import Enum
from typing import Optional
from dataclasses import dataclass
from datetime import date, timedelta, datetime
from collections import deque
from db.db_api import normalize_path
from datamodel import dtos
from datamodel.dtos import FolderTypeEnum, FolderTypeDTO, RootFolderDTO, FolderNodeDTO 
from datamodel.retentions import ExternalRetentionTypes

# In-memory dataclass for test data setup - not persisted to database
# Used by test fixtures to configure cleanup settings before database insertion

@dataclass
class CleanupConfiguration: 
    # In-memory cleanup configuration for test data setup.
    lead_time: int            # days from initialization of the simulations til it can be cleaned
    frequency: int           # number of days between cleanup rounds
    start_date: datetime     # at what date did the current cleanup round start
    progress: dtos.CleanupProgress.Progress = dtos.CleanupProgress.Progress.INACTIVE  # current state of the cleanup round
    
    def to_dto(self, rootfolder_id: int) -> dtos.CleanupConfigurationDTO:
        # Convert to CleanupConfigurationDTO for database insertion.
        return dtos.CleanupConfigurationDTO(
            rootfolder_id=rootfolder_id,
            lead_time=self.lead_time,
            frequency=self.frequency,
            start_date=self.start_date,
            progress=self.progress.value
        )


class InMemoryFolderNode:
    class TestCaseEnum(str, Enum):
        BEFORE       = "start_end_before_cleanup_start"
        BEFORE_AFTER = "start_before_end_after_cleanup_start"
        AFTER        = "start_end_after_cleanup_start"

    """In-memory representation of a folder node using pointers for parent-child relationships"""
    def __init__(self, name: str, is_leaf: bool, external_retentiontype: ExternalRetentionTypes):
        self.name:str = name
        self.path:str = ""
        self.is_leaf:bool = is_leaf
        self.parent: Optional['InMemoryFolderNode'] = None
        self.children: list['InMemoryFolderNode'] = []
        self.modified_date: Optional[datetime] = None  # Add modified_date field
        self.testcase_dict:dict[str, str] = {}
        self.retention: ExternalRetentionTypes = external_retentiontype
    
    def add_child(self, child: 'InMemoryFolderNode'):
        """Add a child node and set up parent-child relationship"""
        child.parent = self
        self.children.append(child)
        # Update child's path based on parent
        child.path = f"{self.path}/{child.name}" if self.path else child.name
    
    def to_folder_node_dto(self) -> FolderNodeDTO:
        """Convert to FolderNodeDTO for database insertion"""
        return FolderNodeDTO(
            name=self.name,
            path=self.path,
            rootfolder_id=0,  # Will be set when linking to root folder
            parent_id=0,  # Will be set during database insertion
            nodetype_id=0,  # Will be set based on is_leaf flag
            retention_id=0   # Will be set based on business rules
        )

#-------------------------------------
# helper to select and generate random ExternalRetentionEnum .
class RandomInternalRetentionType:
    def __init__(self, seed:int):
        self.rand_int_generator = random.Random(seed)
        self.retention_types = [ ExternalRetentionTypes.NUMERIC, ExternalRetentionTypes.ISSUE, ExternalRetentionTypes.CLEAN]

    def next(self):
        return self.retention_types[self.rand_int_generator.randint(0, len(self.retention_types) - 1)]

class RandomNodeTypeNames:
    def __init__(self, folder_types: list[FolderTypeDTO], seed:int):
        self.rand_int_generator = random.Random(seed)
        self.folder_types = folder_types

        # Fix: use next() with a generator expression to find the "vts_simulation" folder type
        self.simulation_type = next((x for x in self.folder_types if x.name == FolderTypeEnum.SIMULATION), None)
        self.inner_node_type = next((x for x in self.folder_types if x.name == FolderTypeEnum.INNERNODE), None)
        if self.simulation_type is None or self.inner_node_type is None:
            raise ValueError("Required folder types are not found")
        
    def next(self):
        return self.folder_types[self.rand_int_generator.randint(0, len(self.folder_types) - 1)]

    def get_simulation_type(self) -> FolderTypeDTO:
        if self.simulation_type is None : raise ValueError("vts folder type not found") # for pylance' sake
        return self.simulation_type

    def get_inner_node_type(self) -> FolderTypeDTO:
        if self.inner_node_type is None : raise ValueError("InnerNode folder type not found") # for pylance' sake
        return self.inner_node_type

def generate_node_name( 
                   is_leaf: bool,
                   parent: Optional[InMemoryFolderNode],
                   parent_name: str,
                   sibling_counter: int,
                   random_internal_retention: RandomInternalRetentionType
                 ) -> InMemoryFolderNode:

    external_retentiontype: ExternalRetentionTypes = ExternalRetentionTypes.NUMERIC # default retention is None for inner nodes and can be None for leaf nodes
    if is_leaf:
        name = f"VTS_{parent_name}_{sibling_counter + 1}" if parent is not None else f"VTS_{parent_name}"
        external_retentiontype = random_internal_retention.next()
    else:
        name = f"{parent_name}_{sibling_counter + 1}" if parent is not None else f"{parent_name}"

    child = InMemoryFolderNode(name=name, is_leaf=is_leaf, external_retentiontype=external_retentiontype)

    # Set the path - this will be updated when added to parent
    if parent is not None:
        parent.add_child(child)
    else:
        child.path = name
    
    return child

def generate_folder_tree_names(root_folder_name: str, max_level: int = 1,) -> InMemoryFolderNode:
    rand: random.Random = random.Random(42)
    random_retentiontype: RandomInternalRetentionType = RandomInternalRetentionType(42)
    #print(f"Start GenerateTreeRecursivelyAsync: maxLevel = {max_level}")

    # Generate the root node
    current_parent_name: str = root_folder_name
    root: Optional[InMemoryFolderNode] = generate_node_name(
        is_leaf=False,
        parent=None,
        parent_name=current_parent_name,
        sibling_counter=0,
        random_internal_retention=random_retentiontype
    )

    if root is not None:
        # generate a folder tree under the rootfolder
        current_level_nodes = [root]
        nodes_generated = 1
        
        for level in range(max_level):
            next_level_nodes = []
            for current_parent in current_level_nodes:
                number_of_children = rand.randint(2, 4)
                current_parent_name: str = current_parent.name

                # Generate all siblings and add non-leaf nodes to next_level_nodes
                for i_sibling in range(number_of_children):
                    child_level = level + 1

                    if child_level == max_level:
                        is_leaf = True
                    elif child_level <= 3:
                        is_leaf = False
                    else:
                        is_leaf = rand.choice([True, False])

                    child: Optional[InMemoryFolderNode] = generate_node_name(
                        is_leaf=is_leaf,
                        parent=current_parent,
                        parent_name=current_parent_name,
                        sibling_counter=i_sibling,
                        random_internal_retention=random_retentiontype
                    )
                    
                    if child is not None:
                        if child_level < max_level and not is_leaf:
                            next_level_nodes.append(child)
                        nodes_generated += 1

            current_level_nodes = next_level_nodes
        
    #print(f"GenerateTreeRecursivelyAsync: Total nodes generated = {nodes_generated}, maxLevel = {max_level}")
    return root

def collect_all_nodes(root: InMemoryFolderNode) -> list[InMemoryFolderNode]:
    """Collect all nodes in the tree by traversing recursively"""
    all_nodes = [root]
    
    for child in root.children:
        all_nodes.extend(collect_all_nodes(child))
    
    return all_nodes

class RootFolderWithMemoryFolders:
    """Named tuple for a root folder and its flattened list of folder nodes"""
    rootfolder: RootFolderDTO
    folders: list[InMemoryFolderNode]
    leafs: list[InMemoryFolderNode]
    before_leafs: list[InMemoryFolderNode]=None
    mid_leafs: list[InMemoryFolderNode]=None
    after_leafs: list[InMemoryFolderNode]=None
    cleanup_configuration:CleanupConfiguration
    def __init__(self, rootfolder: RootFolderDTO, folders: list[InMemoryFolderNode], cleanup_configuration: CleanupConfiguration):
        self.rootfolder = rootfolder
        self.folders = folders
        self.leafs = [folder for folder in folders if folder.is_leaf]
        self.cleanup_configuration = cleanup_configuration
        self.before_leafs = []
        self.mid_leafs =  []
        self.after_leafs =  []

    def get_paths_with_most_children(self, minimum_number_of_children:int=2, min_levels:int=2, n_paths: int=0) -> list[str]:
        # return n_paths paths with most children in descending order. 0 means all are returned
        # 1) find all nodes with child nodes each containing more than 1 child. exclude the first folder level
        # 2) sort by number of children descending
        # 3) return the paths of the first n_paths folders
        multiple_child_folders: list[InMemoryFolderNode] = [folder for folder in self.folders if len(folder.children) >= minimum_number_of_children and folder.path.count("/") >= min_levels]
        multiple_child_folders.sort(key=lambda f: len(f.children), reverse=True)
        if n_paths > 0:
            multiple_child_folders = multiple_child_folders[:min(n_paths, len(multiple_child_folders))]

        paths_with_multiple_children: list[str] = [folder.path for folder in multiple_child_folders]
        return paths_with_multiple_children

    def get_two_level_path_protections(self) -> list[tuple[str, str]]:
        """Generate all path protection pairs: a high-level path and a lower-level nested path.        
        Returns:
            A list of tuples (highlevel_path, lower_level_path). Empty list if unable to generate pairs.
        """
        path_decending_n_children: list[str] = self.get_paths_with_most_children()
        
        if len(path_decending_n_children) <= 1:
            return []

        result_pairs: list[tuple[str, str]] = []
        
        # Process each path as a potential high-level path
        for i, path in enumerate(path_decending_n_children):
            highlevel_path = path
            if highlevel_path.count("/") > 1:
                # Take the parent path. We have to do it this way because in this context we only have flat lists
                highlevel_path = "/".join(highlevel_path.split("/")[:-1])

            # Find all paths that are nested under highlevel_path
            for lower_path in path_decending_n_children[i+1:]:
                if highlevel_path in lower_path and len(lower_path) > len(highlevel_path):
                    result_pairs.append((highlevel_path, lower_path))
        
        # Sort by the high-level path (first element of tuple) makes it easier to look at date in the debuggger
        # result_pairs.sort(key=lambda pair: pair[0])
        return result_pairs

    def count_protected_leafs(self, path_protection_paths: list[str]) -> int:
        """Count the number of leaf folders that start with any of the given path protection paths.
        Args:
            path_protection_paths: List of path protection paths to check against
        Returns:
            Number of leafs protected by the path protections. Each leaf is counted only once,
            even if multiple path protections could match.
        """
        return sum(
            1 for leaf in self.leafs 
            if any(normalize_path(leaf.path).startswith(normalize_path(pp_path)) 
                   for pp_path in path_protection_paths)
        )

    def get_leafs_to_be_marked_dict(self, path_protections_paths: list[str] = None) -> dict[str, InMemoryFolderNode]:
        """Get a dictionary of leaf folders that should be marked for cleanup.
        
        A leaf is marked for cleanup if:
        - Its retention case is BEFORE (retention period ended before cleanup start)
        - Its retention type is NUMERIC
        - It is NOT under a path protection (path protections take priority)
        
        Args:
            path_protections_paths: List of path protection paths. Leafs under these paths will be excluded.
        
        Returns:
            Dictionary mapping normalized paths to leaf folders that should be marked for cleanup.
            Same calculation as in line 392 of test_cleanup_workflows.py
        """
        input_leafs_lookup: dict[str, InMemoryFolderNode] = {
            normalize_path(folder.path): folder 
            for folder in self.folders 
            if folder.is_leaf
        }
        
        # If path protections are defined, check if the leaf is under protection
        def is_protected(leaf_path: str) -> bool:
            if not path_protections_paths:
                return False
            return any(
                normalize_path(leaf_path).startswith(normalize_path(pp_path)) 
                for pp_path in path_protections_paths
            )
        
        return {
            path: folder 
            for path, folder in input_leafs_lookup.items()
            if folder.testcase_dict.get("folder_retention_case") == InMemoryFolderNode.TestCaseEnum.BEFORE 
            and folder.retention == ExternalRetentionTypes.NUMERIC
            and not is_protected(path)
        }

    def randomize_modified_dates_of_leaf_folders(self):
        # the following modified date has not effect before we are able to set the modified date in the file system
        # Randomize the modified dates of all leaf folders according to the following rules. Notice that end date is not stored, only the modified date is set:
        # - before_leafs:        retention period starts and ends before the cleanup round start
        #                     => modified date = from "start_date - retention_period - random_interval - 1"
        #                        modified date+retention_period to "start_date - random_interval - 1"

        # - before_after_leafs:  retention period starts before and ends after the cleanup
        #                     => modified date = from "start_date - retention_period/2 - random_interval" 
        #                        modified date+retention_period to "start_date - random_interval - 1"

        # - after_leafs:         retention period starts after the cleanup round
        #                     => modified date = from "start_date + 1 + random_interval" onwards
        #                        modified date + retention_period to  "start_date + retention_period + 1 + random_interval"

        leafs = [folder for folder in self.folders if folder.is_leaf]
        
        # Divide leafs into three equal groups
        group_size = len(leafs) // 3
        self.before_leafs = leafs[:group_size]
        self.mid_leafs    = leafs[group_size:2*group_size]
        self.after_leafs  = leafs[2*group_size:]
        
        rand: random.Random = random.Random()  # Use non-deterministic seed for true randomization
        ran_interval_days = self.cleanup_configuration.lead_time//2
        leadtime_days = self.cleanup_configuration.lead_time
        start_date = self.cleanup_configuration.start_date

        # Group 1: before_leafs - modified dates before retention period (will be cleaned up)
        # Date range: [cleanup_start - retention - random_days - 1] to [cleanup_start - random_days - 1]
        for leaf in self.before_leafs:
            #the case closest to not being marked i the modified_date start date-leadtime_days-1. The rest old. All will be marked if cleanup start at start_date 
            leaf.modified_date = start_date + timedelta(days=-leadtime_days - rand.randint(1, 10) - 1) 
            leaf.testcase_dict["folder_retention_case"] = InMemoryFolderNode.TestCaseEnum.BEFORE
        
        # Group 2: before_after_leafs - retention spans across cleanup (partially in retention)
        # Date range: [cleanup_start - retention/2 - random_days] to [cleanup_start - random_days - 1]
        for leaf in self.mid_leafs:
            #the oldest is start_date-leadtime_days // 2 + 1. The rest is more recent. Non will be marked if cleanup start at start_date
            leaf.modified_date = start_date + timedelta(days= -leadtime_days // 2 + rand.randint(1, ran_interval_days) ) 
            leaf.testcase_dict["folder_retention_case"] = InMemoryFolderNode.TestCaseEnum.BEFORE_AFTER

        # Group 3: after_leafs - modified dates after cleanup starts (will NOT be cleaned up)
        # Date range: [cleanup_start + 1 + random_days] onwards (up to +60 days)
        for leaf in self.after_leafs:
            # The oldest is start_date + 1 + 1 (minimum random_days). The rest is more recent. None will be marked if cleanup start at start_date
            leaf.modified_date = start_date + timedelta(days=1 + rand.randint(1, ran_interval_days) )
            leaf.testcase_dict["folder_retention_case"] = InMemoryFolderNode.TestCaseEnum.AFTER
        


class RootFolderWithMemoryFolderTree:
    """Named tuple for a root folder and its hierarchical tree structure"""
    def __init__(self, rootfolder: RootFolderDTO, folder_tree: InMemoryFolderNode):
        self.rootfolder = rootfolder
        self.folder_tree = folder_tree

def flatten_folder_structure(rootfolder_tuple: RootFolderWithMemoryFolderTree, cleanup_config: CleanupConfiguration) -> RootFolderWithMemoryFolders:
    #Flatten the hierarchical InMemoryFolderNode folder root and return a named tuple with the rootfolder and its list of folders
    return RootFolderWithMemoryFolders(
        rootfolder=rootfolder_tuple.rootfolder,
        folders=collect_all_nodes(rootfolder_tuple.folder_tree),
        cleanup_configuration=cleanup_config
    )

def flatten_multiple_folder_structures(rootfolder_tuples: list[RootFolderWithMemoryFolderTree], cleanup_config: CleanupConfiguration) -> list[RootFolderWithMemoryFolders]:
    #Flatten multiple folder structures
    return [flatten_folder_structure(rootfolder_tuple, cleanup_config) for rootfolder_tuple in rootfolder_tuples]

def print_folder_tree(node: InMemoryFolderNode, indent: str = "", max_levels: Optional[int] = None, current_level: int = 0):
    """Print the folder tree structure recursively using pointer relationships    
    Args:
        node: The current node to print
        indent: String for indentation
        max_levels: Maximum number of levels to print (None for all levels)
        current_level: Current depth level (internal use)
    """
    leaf_indicator = " [LEAF]" if node.is_leaf else ""
    print(f"{indent}- {node.name} (Path: {node.path}){leaf_indicator}")
    
    # Check if we should continue printing deeper levels
    if max_levels is None or current_level < max_levels - 1:
        # Recursively print children using pointer relationships
        for child in node.children:
            print_folder_tree(child, indent + "  ", max_levels, current_level + 1)

def export_folder_tree_to_csv(root: InMemoryFolderNode, root_folder: RootFolderDTO, filename: str = "folder_tree.csv"):
    """Export the folder tree to CSV format with root folder info, isLeaf and path components as columns
    
    Args:
        root: Root node of the tree
        root_folder: Root folder DTO containing owner, approvers, path info
        filename: Output CSV filename
    """
    import csv
    
    # Collect all nodes
    all_nodes = collect_all_nodes(root)
    
    # Find the maximum depth to determine number of columns needed
    max_depth = 0
    for node in all_nodes:
        path_parts = node.path.split('/') if node.path else []
        max_depth = max(max_depth, len(path_parts))
    
    # Create column headers
    headers = ['RootPath', 'Owner', 'Approvers', 'IsLeaf'] + [f'F{i+1}' for i in range(max_depth)]
    
    # Prepare data rows
    rows = []
    for node in all_nodes:
        row = [root_folder.path, root_folder.owner, root_folder.approvers, node.is_leaf]  # Root folder info + IsLeaf
        path_parts = node.path.split('/') if node.path else []
        
        # Add path components to columns F1, F2, F3, etc.
        for i in range(max_depth):
            if i < len(path_parts):
                row.append(path_parts[i])
            else:
                row.append('')  # Empty string for missing path components
        
        rows.append(row)
    
    # Write to CSV file
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerow(headers)
        writer.writerows(rows)
    
    print(f"   -> Exported tree structure to {filename}")
    print(f"   -> CSV columns: {', '.join(headers)}")
    print(f"   -> Total rows: {len(rows)} (including {len([r for r in rows if r[3]])} leaf nodes)")

def export_all_folders_to_csv(rootfolders: list[RootFolderWithMemoryFolderTree], filename: str = "all_folder_trees.csv"):
    """Export all folder trees to a single CSV file with semicolon separator
    
    Args:
        rootfolders: List of RootFolderWithFolderTree named tuples
        filename: Output CSV filename
    """
    import csv
    
    all_rows = []
    max_depth = 0
    
    # Collect all nodes from all root folders and find maximum depth
    for root_folder_with_tree in rootfolders:
        if root_folder_with_tree.folder_tree is not None:
            all_nodes = collect_all_nodes(root_folder_with_tree.folder_tree)
            for node in all_nodes:
                path_parts = node.path.split('/') if node.path else []
                max_depth = max(max_depth, len(path_parts))
                
                # Create row with root folder info, IsLeaf and path components
                row = [root_folder_with_tree.rootfolder.path, root_folder_with_tree.rootfolder.owner, root_folder_with_tree.rootfolder.approvers, node.is_leaf] + path_parts + [''] * (max_depth - len(path_parts))
                all_rows.append(row)
    
    # Adjust all rows to have the same number of columns
    headers = ['RootPath', 'Owner', 'Approvers', 'IsLeaf'] + [f'F{i+1}' for i in range(max_depth)]
    for row in all_rows:
        while len(row) < len(headers):
            row.append('')
    
    # Write to CSV file
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerow(headers)
        writer.writerows(all_rows)
    
    print(f"\n   -> Exported ALL folder trees to {filename}")
    print(f"   -> CSV columns: {', '.join(headers)}")
    print(f"   -> Total rows: {len(all_rows)} (including {len([r for r in all_rows if r[3]])} leaf nodes)")

def generate_root_folder_name(owner: str, approvers: str, path: str, levels: int) -> RootFolderWithMemoryFolderTree:
    rootfolder = RootFolderDTO( owner=owner, approvers=approvers, path=path)
    folder = generate_folder_tree_names(path, levels)
    return RootFolderWithMemoryFolderTree(rootfolder=rootfolder, folder_tree=folder)

def generate_in_memory_rootfolder_and_folder_hierarchies(number_of_rootfolders:int) -> list[RootFolderWithMemoryFolderTree]:
    root_folders: list[RootFolderWithMemoryFolderTree] = []

    for i in range(1, number_of_rootfolders + 1):
        root_folder_with_tree = generate_root_folder_name("jajac", "stefw, misve", f"R{i}", i + 2)
        root_folders.append(root_folder_with_tree)
    
    return root_folders


def generate_cleanup_scenario_data(lead_time=30, frequency=7, start_date=datetime(2000, 1, 1)):
    # Sample data with 3 datasets for 2 root folders:
    #  - part one with the first root folder and a list of all its subfolders in random order
    #  - part two and three with a random split of each of the second rootfolders list of subfolders
    # The root folder's cleanup configuration is not initialised means that assumes default values

    number_of_rootfolders = 2
    cleanup_configuration = CleanupConfiguration(lead_time=lead_time, frequency=frequency, start_date=start_date)

    rootfolders: deque[RootFolderWithMemoryFolderTree] = deque( generate_in_memory_rootfolder_and_folder_hierarchies(number_of_rootfolders) )
    assert len(rootfolders) > 0

    # Split the two root folder in three parts:
    # first rootfolder with all its folders randomized
    first_rootfolder: RootFolderWithMemoryFolders = flatten_folder_structure(rootfolders.popleft(), cleanup_configuration)

    #first_rootfolder.rootfolder.set_cleanup_configuration(cleanup_configuration)
    first_rootfolder.randomize_modified_dates_of_leaf_folders()

    random.shuffle(first_rootfolder.folders)
    #first_rootfolder.folders

    # second RootFolders is split into two datasets for the same rootfolder with an "equal" number of the folders drawn in random order from the second rootfolder
    second_rootfolder: RootFolderWithMemoryFolders = flatten_folder_structure(rootfolders.popleft(), cleanup_configuration)
    random.shuffle(second_rootfolder.folders)
    mid_index = len(second_rootfolder.folders) // 2
    
    second_rootfolder_part_one = RootFolderWithMemoryFolders(rootfolder=second_rootfolder.rootfolder, folders=second_rootfolder.folders[:mid_index], cleanup_configuration=cleanup_configuration)
    second_rootfolder_part_one.randomize_modified_dates_of_leaf_folders()
    second_rootfolder_part_one.folders.sort(key=lambda folder: folder.path)

    second_rootfolder_part_two = RootFolderWithMemoryFolders(rootfolder=second_rootfolder.rootfolder, folders=second_rootfolder.folders[mid_index:], cleanup_configuration=cleanup_configuration)
    second_rootfolder_part_two.randomize_modified_dates_of_leaf_folders()
    second_rootfolder_part_two.folders.sort(key=lambda folder: folder.path)
    return {
        "cleanup_configuration": cleanup_configuration,
        "rootfolder_tuples": rootfolders,
        "first_rootfolder": first_rootfolder,
        "second_rootfolder_part_one": second_rootfolder_part_one,
        "second_rootfolder_part_two": second_rootfolder_part_two
    }


if __name__ == "__main__":  
    rootfolders = generate_in_memory_rootfolder_and_folder_hierarchies(2)
    for i, root_folder_with_tree in enumerate(rootfolders):
        rf = root_folder_with_tree.rootfolder
        folder = root_folder_with_tree.folder_tree
        print(f" - {rf.path} (ID: {rf.id}), Owner: {rf.owner}, Approvers: {rf.approvers}")
        if folder is not None:
            print(f"   -> Folder tree root: {folder.name} Path: {folder.path}")
            
            # Show limited tree structure (top 2 levels)
            print("   -> Tree structure (top 2 levels):")
            print_folder_tree(folder, "      ", max_levels=2)
            
            # Show full tree structure for first folder only to avoid clutter
            if i == 0:
                print("   -> Full tree structure:")
                print_folder_tree(folder, "      ")
            
            # Count total nodes
            all_nodes = collect_all_nodes(folder)
            print(f"   -> Total nodes in tree: {len(all_nodes)}")
            print(f"   -> Leaf nodes: {sum(1 for node in all_nodes if node.is_leaf)}")
            
        else:
            print("   -> No folder tree generated")
        print()  # Add blank line between root folders
    
    # Export all folders to a single CSV file
    export_all_folders_to_csv(rootfolders)
