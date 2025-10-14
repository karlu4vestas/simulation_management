from datetime import timedelta, datetime
from typing import Optional
import random
from enum import Enum
from typing import NamedTuple
from sqlmodel import Session, select
from sqlalchemy import Engine
from datamodel.dtos import CleanupConfiguration, CleanupFrequencyDTO, FolderTypeEnum, RetentionTypeDTO, FolderTypeDTO, RootFolderDTO, FolderNodeDTO, SimulationDomainDTO 
from db.database import Database


class InMemoryFolderNode:
    class TestCaseEnum(str, Enum):
        BEFORE = "start_end_before_cleanup_start"
        BEFORE_AFTER = "start_before_end_after_cleanup_start"
        AFTER = "start_end_after_cleanup_start"

    """In-memory representation of a folder node using pointers for parent-child relationships"""
    def __init__(self, name: str, is_leaf: bool = False):
        self.name = name
        self.path = ""
        self.is_leaf = is_leaf
        self.parent: Optional['InMemoryFolderNode'] = None
        self.children: list['InMemoryFolderNode'] = []
        self.modified_date: Optional[datetime] = None  # Add modified_date field
        self.testcase_dict:dict[str, str] = {}
        self.retention: ExternalRetentionEnum = None
    
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
# helper to generate random retenttype except for the "Path" retention. 
class RandomRetentionNames:
    def __init__(self, retention_types: list[str], seed:int):
        self.rand_int_generator = random.Random(seed)
        self.retention_types = retention_types

    def next(self):
        return self.retention_types[self.rand_int_generator.randint(0, len(self.retention_types) - 1)]

class RandomNodeTypeNames:
    def __init__(self, folder_types: list[FolderTypeDTO], seed:int):
        self.rand_int_generator = random.Random(seed)
        self.folder_types = folder_types

        # Fix: use next() with a generator expression to find the "vts_simulation" folder type
        self.simulation_type = next((x for x in self.folder_types if x.name == FolderTypeEnum.VTS_SIMULATION), None)
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
                   sibling_counter: int
                 ) -> InMemoryFolderNode:
    if is_leaf:
        name = f"VTS_{parent_name}_{sibling_counter + 1}" if parent is not None else f"VTS_{parent_name}"
    else:
        name = f"{parent_name}_{sibling_counter + 1}" if parent is not None else f"{parent_name}"
    
    child = InMemoryFolderNode(name=name, is_leaf=is_leaf)
    
    # Set the path - this will be updated when added to parent
    if parent is not None:
        parent.add_child(child)
    else:
        child.path = name
    
    return child

def generate_folder_tree_names(root_folder_name: str, max_level: int = 1) -> InMemoryFolderNode:
    rand: random.Random = random.Random(42)
    
    print(f"Start GenerateTreeRecursivelyAsync: maxLevel = {max_level}")

    # Generate the root node
    current_parent_name: str = root_folder_name
    root: Optional[InMemoryFolderNode] = generate_node_name(
        is_leaf=False,
        parent=None,
        parent_name=current_parent_name,
        sibling_counter=0
    )

    if root is not None:
        # generate a folder tree under the rootfolder
        current_level_nodes = [root]
        nodes_generated = 1
        
        for level in range(max_level):
            next_level_nodes = []
            for current_parent in current_level_nodes:
                number_of_children = rand.randint(4, 6)
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
                        sibling_counter=i_sibling
                    )
                    
                    if child is not None:
                        if child_level < max_level and not is_leaf:
                            next_level_nodes.append(child)
                        nodes_generated += 1

            current_level_nodes = next_level_nodes
        
    print(f"GenerateTreeRecursivelyAsync: Total nodes generated = {nodes_generated}, maxLevel = {max_level}")
    return root

def collect_all_nodes(root: InMemoryFolderNode) -> list[InMemoryFolderNode]:
    """Collect all nodes in the tree by traversing recursively"""
    all_nodes = [root]
    
    for child in root.children:
        all_nodes.extend(collect_all_nodes(child))
    
    return all_nodes

class RootFolderWithMemoryFolders:
    """Named tuple for a root folder and its flattened list of folder nodes"""
    def __init__(self, rootfolder: RootFolderDTO, folders: list[InMemoryFolderNode]):
        self.rootfolder = rootfolder
        self.folders = folders

class RootFolderWithMemoryFolderTree:
    """Named tuple for a root folder and its hierarchical tree structure"""
    def __init__(self, rootfolder: RootFolderDTO, folder_tree: InMemoryFolderNode):
        self.rootfolder = rootfolder
        self.folder_tree = folder_tree

def flatten_folder_structure(rootfolder_tuple: RootFolderWithMemoryFolderTree) -> RootFolderWithMemoryFolders:
    #Flatten the hierarchical InMemoryFolderNode folder root and return a named tuple with the rootfolder and its list of folders
    return RootFolderWithMemoryFolders(
        rootfolder=rootfolder_tuple.rootfolder,
        folders=collect_all_nodes(rootfolder_tuple.folder_tree)
    )

def flatten_multiple_folder_structures(rootfolder_tuples: list[RootFolderWithMemoryFolderTree]) -> list[RootFolderWithMemoryFolders]:
    #Flatten multiple folder structures
    return [flatten_folder_structure(rootfolder_tuple) for rootfolder_tuple in rootfolder_tuples]

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
    
    rootfolder = RootFolderDTO(
            owner=owner,
            approvers=approvers,
            path=path
    )

    print(f"Root folder created. path={rootfolder.path}")
    folder = generate_folder_tree_names(path, levels)
    return RootFolderWithMemoryFolderTree(rootfolder=rootfolder, folder_tree=folder)

def generate_in_memory_rootfolder_and_folder_hierarchies(number_of_rootfolders:int) -> list[RootFolderWithMemoryFolderTree]:
    root_folders: list[RootFolderWithMemoryFolderTree] = []

    for i in range(1, number_of_rootfolders + 1):
        root_folder_with_tree = generate_root_folder_name("jajac", "stefw, misve", f"R{i}", i + 1)
        root_folders.append(root_folder_with_tree)
    
    return root_folders


def randomize_modified_dates_of_leaf_folders(rootfolder:RootFolderDTO, folders: list[InMemoryFolderNode]):
    """Randomize the modified dates of all leaf folders according to the following rules. Notice that end date is not stored, only the modified date is set:
    - before_leafs:       retention period starts and ends before the cleanup round start
                          => modified date = from "cleanup_start_date - retention_period - random_interval - 1"
                             modified date+retention_period to "cleanup_start_date - random_interval - 1"

    - before_after_leafs: retention period starts before and ends after the cleanup
                          => modified date = from "cleanup_start_date - retention_period/2 - random_interval" 
                             modified date+retention_period to "cleanup_start_date - random_interval - 1"

    - after_leafs:        retention period starts after the cleanup round
                          => modified date = from "cleanup_start_date + 1 + random_interval" onwards
                          modified date + retention_period to  "cleanup_start_date + retention_period + 1 + random_interval"
    """
    leafs = [folder for folder in folders if folder.is_leaf]
    
    # Divide leafs into three equal groups
    total_leafs = len(leafs)
    group_size = total_leafs // 3
    before_leafs = leafs[:group_size]
    before_after_leafs = leafs[group_size:2*group_size]
    after_leafs = leafs[2*group_size:]
    
    cleanup_configuration: CleanupConfiguration = rootfolder.get_cleanup_configuration()
    rand: random.Random = random.Random(42)
    ran_interval_days = 10
    retention_period_days = cleanup_configuration.cycletime
    cleanup_start_date = cleanup_configuration.cleanup_round_start_date

    # Group 1: before_leafs - modified dates before retention period (will be cleaned up)
    # Date range: [cleanup_start - retention - random_days - 1] to [cleanup_start - random_days - 1]
    for leaf in before_leafs:
        leaf.modified_date = cleanup_start_date - timedelta(days=retention_period_days + rand.randint(1, ran_interval_days)  + 1)
        leaf.testcase_dict["folder_retention_case"] = InMemoryFolderNode.TestCaseEnum.BEFORE
    
    # Group 2: before_after_leafs - retention spans across cleanup (partially in retention)
    # Date range: [cleanup_start - retention/2 - random_days] to [cleanup_start - random_days - 1]
    for leaf in before_after_leafs:
        leaf.modified_date = cleanup_start_date - timedelta(days=retention_period_days // 2 + rand.randint(1, ran_interval_days) )
        leaf.testcase_dict["folder_retention_case"] = InMemoryFolderNode.TestCaseEnum.BEFORE_AFTER

    # Group 3: after_leafs - modified dates after cleanup starts (will NOT be cleaned up)
    # Date range: [cleanup_start + 1 + random_days] onwards (up to +60 days)
    for leaf in after_leafs:
        leaf.modified_date = cleanup_start_date + timedelta(days=1 + rand.randint(1, ran_interval_days) )
        leaf.testcase_dict["folder_retention_case"] = InMemoryFolderNode.TestCaseEnum.AFTER
    
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