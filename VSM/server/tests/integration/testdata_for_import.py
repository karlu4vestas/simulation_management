import random
import csv
from sqlmodel import Session, select
from sqlalchemy import Engine
from datamodel.dtos import CleanupFrequencyDTO, FolderTypeEnum, RetentionTypeDTO, FolderTypeDTO, RootFolderDTO, FolderNodeDTO, SimulationDomainDTO 
from db.database import Database
from datamodel.vts_create_meta_data import insert_vts_metadata_in_db
from typing import Optional

class InMemoryFolderNode:
    """In-memory representation of a folder node using pointers for parent-child relationships"""
    def __init__(self, name: str, is_leaf: bool = False):
        self.name = name
        self.path = ""
        self.is_leaf = is_leaf
        self.parent: Optional['InMemoryFolderNode'] = None
        self.children: list['InMemoryFolderNode'] = []
    
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

def flatten_folder_structure(rootfolder_tuple: tuple[RootFolderDTO, InMemoryFolderNode]) -> list[tuple[RootFolderDTO, InMemoryFolderNode]]:
    """Flatten a folder structure by pairing the root folder DTO with each individual node in the tree
    
    Args:
        rootfolder_tuple: Tuple containing a RootFolderDTO and the root InMemoryFolderNode of the tree
        
    Returns:
        List of tuples where each tuple contains the same RootFolderDTO paired with each individual node from the tree
    """
    root_folder_dto, root_node = rootfolder_tuple
    
    # Collect all nodes in the tree (including root)
    all_nodes = collect_all_nodes(root_node)
    
    # Create a list of tuples pairing the root folder DTO with each node
    flattened_list = [(root_folder_dto, node) for node in all_nodes]
    
    return flattened_list

def flatten_multiple_folder_structures(rootfolder_tuples: list[tuple[RootFolderDTO, InMemoryFolderNode]]) -> list[tuple[RootFolderDTO, InMemoryFolderNode]]:
    """Flatten multiple folder structures by pairing each root folder DTO with its individual nodes

    Args:
        rootfolder_tuples: List of tuples containing RootFolderDTO and InMemoryFolderNode for each root folder

    Returns:
        List of tuples where each tuple contains a RootFolderDTO paired with each individual node from its tree
    """
    flattened_list = []
    for rootfolder_tuple in rootfolder_tuples:
        flattened_list.extend(flatten_folder_structure(rootfolder_tuple))
    return flattened_list

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

def export_all_folders_to_csv(rootfolders: list[tuple[RootFolderDTO, InMemoryFolderNode]], filename: str = "all_folder_trees.csv"):
    """Export all folder trees to a single CSV file with semicolon separator
    
    Args:
        rootfolders: List of root folder DTOs and their corresponding in-memory trees
        filename: Output CSV filename
    """
    import csv
    
    all_rows = []
    max_depth = 0
    
    # Collect all nodes from all root folders and find maximum depth
    for rf, folder in rootfolders:
        if folder is not None:
            all_nodes = collect_all_nodes(folder)
            for node in all_nodes:
                path_parts = node.path.split('/') if node.path else []
                max_depth = max(max_depth, len(path_parts))
                
                # Create row with root folder info, IsLeaf and path components
                row = [rf.path, rf.owner, rf.approvers, node.is_leaf] + path_parts + [''] * (max_depth - len(path_parts))
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

def generate_root_folder_name(owner: str, approvers: str, path: str, levels: int) -> tuple[RootFolderDTO, InMemoryFolderNode]:
    
    root_folder = RootFolderDTO(
            owner=owner,
            approvers=approvers,
            path=path
    )

    print(f"Root folder created. path={root_folder.path}")
    folder = generate_folder_tree_names(path, levels)
    return root_folder, folder

def generate_in_memory_rootfolders_and_folder_hierarchy() -> list[tuple[RootFolderDTO, list[InMemoryFolderNode]]]:
    root_folders: list[tuple[RootFolderDTO, list[InMemoryFolderNode]]] = []

    root_folders.append(generate_root_folder_name("jajac", "stefw, misve", "R1",2))
    root_folders.append(generate_root_folder_name("jajac", "stefw, misve", "R2",3))
    #root_folders.append(generate_root_folder_name("jajac", "stefw, misve", "R3",4))
    #root_folders.append(generate_root_folder_name("jajac", "stefw, misve", "R4",5))
    #root_folders.append(generate_root_folder_name("misve", "stefw, arlem", "R5",6))
    #root_folders.append(generate_root_folder_name("karlu", "arlem, caemh", "R6",7))
    #root_folders.append(generate_root_folder_name("jajac", "stefw, misve", "R7",8))
    #root_folders.append(generate_root_folder_name(engine, domain_id, "caemh", "arlem, jajac", False, "R8",9))
    #root_folders.append(generate_root_folder_name(engine, domain_id, "caemh", "arlem, jajac", False, "R9",10))

    return root_folders



if __name__ == "__main__":  
    rootfolders = generate_in_memory_rootfolders_and_folder_hierarchy()
    for i, (rf, folder) in enumerate(rootfolders):
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