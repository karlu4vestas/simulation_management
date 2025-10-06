import random
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
                 ) -> Optional[InMemoryFolderNode]:
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

def generate_folder_tree_names(root_folder_name: str, max_level: int = 1) -> Optional[InMemoryFolderNode]:
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

def print_folder_tree(node: InMemoryFolderNode, indent: str = ""):
    """Print the folder tree structure recursively using pointer relationships"""
    leaf_indicator = " [LEAF]" if node.is_leaf else ""
    print(f"{indent}- {node.name} (Path: {node.path}){leaf_indicator}")
    
    # Recursively print children using pointer relationships
    for child in node.children:
        print_folder_tree(child, indent + "  ")

def collect_all_nodes(root: InMemoryFolderNode) -> list[InMemoryFolderNode]:
    """Collect all nodes in the tree by traversing recursively"""
    all_nodes = [root]
    
    for child in root.children:
        all_nodes.extend(collect_all_nodes(child))
    
    return all_nodes

def generate_root_folder_name(owner: str, approvers: str, path: str, levels: int) -> tuple[RootFolderDTO, Optional[InMemoryFolderNode]]:
    
    root_folder = RootFolderDTO(
            owner=owner,
            approvers=approvers,
            path=path
    )

    print(f"Root folder created. path={root_folder.path}")
    folder = generate_folder_tree_names(path, levels)
    return root_folder, folder

def generate_in_memory_rootfolders_and_folder_hierarchy() -> list[tuple[RootFolderDTO, Optional[InMemoryFolderNode]]]:
    root_folders: list[tuple[RootFolderDTO, Optional[InMemoryFolderNode]]] = []

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
    for rf, folder in rootfolders:
        print(f" - {rf.path} (ID: {rf.id}), Owner: {rf.owner}, Approvers: {rf.approvers}")
        if folder is not None:
            print(f"   -> Folder tree root: {folder.name} Path: {folder.path}")
            print("   -> Full tree structure:")
            print_folder_tree(folder, "      ")
            
            # Count total nodes
            all_nodes = collect_all_nodes(folder)
            print(f"   -> Total nodes in tree: {len(all_nodes)}")
            print(f"   -> Leaf nodes: {sum(1 for node in all_nodes if node.is_leaf)}")            
        else:
            print("   -> No folder tree generated")