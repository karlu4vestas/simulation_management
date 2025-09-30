import random
from sqlmodel import Session, select
from sqlalchemy import Engine
from datamodel.dtos import CleanupFrequencyDTO, FolderTypeEnum, RetentionTypeDTO, FolderTypeDTO, RootFolderDTO, FolderNodeDTO, SimulationDomainDTO 
from db.database import Database
from datamodel.vts_create_meta_data import insert_vts_metadata_in_db

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

def generate_root_folder_name(owner:str, approvers:str, path, levels) -> (RootFolderDTO, FolderNodeDTO):
    
    root_folder = RootFolderDTO(
            owner=owner,
            approvers=approvers,
            path=path
    )

    #commit in order to initialize the root_folder in the db and get a valid id
    print(f"Root folder created. path={root_folder.path}")
    folder = generate_folder_tree_names(path, levels)
    return root_folder, folder


from typing import Optional
def generate_node_name( 
                   is_leaf:bool,
                   parent:FolderNodeDTO,
                   parent_name:str,
                   #node_type:FolderTypeDTO, 
                   sibling_counter:int, 
                   #retention_generator:RandomRetentionNames 
                 ) -> Optional[FolderNodeDTO]:
    child: Optional[FolderNodeDTO] = None
    if is_leaf:
        child = FolderNodeDTO(
            name=f"VTS_{parent_name}_{sibling_counter + 1}" if not parent is None  else f"VTS_{parent_name}",
            #nodetype_id=node_type.id,  
            #retention_id=retention_generator.next().id
        )
    else:
        child = FolderNodeDTO(
            name=f"{parent_name}_{sibling_counter + 1}" if not parent is None  else f"{parent_name}",
            #nodetype_id=node_type.id,
            #retention_id=0
        )
    child.path = f"{parent.path}/{child.name}" if parent and parent.path else child.name

    #if not child is None:
    #    session.add(child)
    #    session.flush()  # Flush to get the ID without committing
    #    child.path_ids = f"{parent.path_ids}/{child.id}" if parent else 0  
    return child 

def generate_folder_tree_names(root_folder_name:str, max_level:int=1) -> FolderNodeDTO:
    #retention_generator:RandomRetentionNames = RandomRetentionNames(["clean","issue",""], 0)
    rand:random.Random = random.Random(42)
    #random_node_type:RandomNodeTypeNames = RandomNodeTypeNames([FolderTypeEnum.INNERNODE,FolderTypeEnum.VTS_SIMULATION], 0)
    
    #print(f"Start GenerateTreeRecursivelyAsync: maxLevel = {max_level} engine:{not engine is None}")

    if(True):
        #generate the root
        id_counter:int = 1
        #node_type = random_node_type.get_inner_node_type()
        child_level:int = 0
        current_parent_name:str = root_folder_name
        root:Optional[FolderNodeDTO] = generate_node_name( is_leaf=False,
                                                          parent=None,
                                                       parent_name=current_parent_name,
                                                       #node_type=node_type, 
                                                       sibling_counter=0,
                                                       #retention_generator=retention_generator
                                                       )
        
        if not root is None:
            # generate a folder tree under the rootfolder
            current_level_nodes = [root]
            nodes_generated = 1
            for level in range(max_level):
                next_level_nodes = []
                for current_parent in current_level_nodes:
                    number_of_children = rand.randint(4, 6)
                    current_parent_name:str = current_parent.name

                    if current_parent is None : 
                        print(f"Skipping. current_parent is None")
                        continue  # Skip to next iteration of the loop
                    elif current_parent.id is None: 
                        print(f"current_parent.id is None ")
                        continue  # Skip to next iteration of the loop
                    else:
                        current_parent_id:int = current_parent.id
          
                        #generate all siblings and add InnerNodes to next_level_nodes
                        for i_sibling in range(number_of_children):
                            id_counter = id_counter + 1
                            child_level = level + 1

                            if child_level == max_level:
                                is_leaf = True
                            elif child_level <= 3 :
                                is_leaf = False
                            else:
                                is_leaf = rand.choice([True, False])

                            child:Optional[FolderNodeDTO] = generate_node_name(  is_leaf,
                                                                            parent              = current_parent,
                                                                            parent_name         = current_parent_name,
                                                                            #node_type           = node_type, 
                                                                            sibling_counter     = i_sibling,
                                                                            #retention_generator = retention_generator
                                                                            )
                            if not child is None:
                                if child_level < max_level:
                                    next_level_nodes.append(child)
                                nodes_generated += 1

                current_level_nodes = next_level_nodes
        
        # Commit all changes at the end
        #session.commit()
        
        # Access the ID before the session closes to avoid DetachedInstanceError
    
    print(f"GenerateTreeRecursivelyAsync: Total nodes generated = {id_counter}, maxLevel = {max_level}")
    return root


def generate_in_memory_rootfolders_and_folder_hierarchy () ->list[RootFolderDTO]:
    root_folders:list[(RootFolderDTO, FolderNodeDTO)] = [] 

    root_folders.append(generate_root_folder_name("jajac", "stefw, misve", "R1",2))
    #root_folders.append(generate_root_folder_name("jajac", "stefw, misve", "R2",3))
    #root_folders.append(generate_root_folder_name("jajac", "stefw, misve", "R3",4))
    #root_folders.append(generate_root_folder_name("jajac", "stefw, misve", "R4",5))
    #root_folders.append(generate_root_folder_name("misve", "stefw, arlem", "R5",6))
    #root_folders.append(generate_root_folder_name("karlu", "arlem, caemh", "R6",7))
    #root_folders.append(generate_root_folder_name("jajac", "stefw, misve", "R7",8))
    #root_folders.append(generate_root_folder_name(engine, domain_id, "caemh", "arlem, jajac", False, "R8",9))
    #root_folders.append(generate_root_folder_name(engine, domain_id, "caemh", "arlem, jajac", False, "R9",10))

    #    with Session(engine) as session:
    #        root_folders = session.exec(select(RootFolderDTO)).all()
    #        print("Test data for root_folders inserted successfully:")
    #        for rf in root_folders:
    #            print(f" - {rf.path} (ID: {rf.id}), Owner: {rf.owner}, Approvers: {rf.approvers}, CleanUpFrequency: {rf.cleanupfrequency} Folder id: {rf.folder_id}")
    return root_folders



if __name__ == "__main__":
    """    
    with Session(engine) as session:
        vts_simulation_domain = session.exec(select(SimulationDomainDTO).where(SimulationDomainDTO.name == "vts")).first()
        domain_id=vts_simulation_domain.id if vts_simulation_domain and vts_simulation_domain.id else 0
        frequency_name_dict:dict[str,CleanupFrequencyDTO] = read_cleanupfrequency_name_dict(vts_simulation_domain.id)
        if domain_id == 0:
            raise ValueError("vts simulation domain not found")
        cycle_time:int = 0 #days

        frequency = frequency_name_dict["1 week"].days
    """
    rootfolders:list[RootFolderDTO] = generate_in_memory_rootfolders_and_folder_hierarchy ()
    for rf, folder in rootfolders:
        print(f" - {rf.path} (ID: {rf.id}), Owner: {rf.owner}, Approvers: {rf.approvers}")
        if not folder is None:
            print(f"   -> Folder tree root: {folder.name} Path: {folder.path} ")
        else:
            print("   -> No folder tree generated")