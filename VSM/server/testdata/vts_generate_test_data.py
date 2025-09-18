from sqlmodel import Session, select
from sqlalchemy import Engine
from datamodel.dtos import RetentionTypeDTO, FolderTypeDTO, RootFolderDTO, FolderNodeDTO, SimulationDomainDTO 
from datamodel.db import Database
import random
from . import vts_create_meta_data

def insert_test_data_in_db(engine: Engine):
    vts_create_meta_data.insert_vts_metadata_in_db(engine)
    insert_root_folders_metadata_in_db(engine)

#-------------------------------------
# helper to generate random retenttype except for the "Path" retention. 
class RandomRetention:
    def __init__(self, seed):
        self.rand_int_generator = random.Random(seed)

        with Session(Database.get_engine()) as session:
            self.retention_types = session.exec(select(RetentionTypeDTO).where(RetentionTypeDTO.name != "Path")).all()

    def next(self):
        return self.retention_types[self.rand_int_generator.randint(0, len(self.retention_types) - 1)]

class RandomNodeType:
    def __init__(self, seed):
        self.rand_int_generator = random.Random(seed)
        with Session(Database.get_engine()) as session:
            self.folder_types = session.exec(select(FolderTypeDTO)).all()
            if not self.folder_types:
                raise ValueError("No folder types found")
            
        # Fix: use next() with a generator expression to find the "VTS" folder type
        self.simulation_type = next((x for x in self.folder_types if x.name == "VTS"), None)
        self.inner_node_type = next((x for x in self.folder_types if x.name == "InnerNode"), None)
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

def generate_root_folder(engine: Engine, domain_id:int, owner, approvers, cleanup_frequency, path, levels):
    
    with Session(engine) as session:
        root_folder = RootFolderDTO(
            simulation_domain_id=domain_id,
            owner=owner,
            approvers=approvers,
            cleanup_frequency=cleanup_frequency,
            path=path
        )
        session.add(root_folder)

        #commit in order to initialize the root_folder in the db and get a valid id
        session.commit()
        session.refresh(root_folder)
        print(f"Root folder created. id={root_folder.id} path={root_folder.path}")
        root_folder.folder_id = generate_folder_tree(engine, root_folder.id, path, levels)
        session.commit()

def insert_root_folders_metadata_in_db(engine):
    with Session(engine) as session:
        vts_simulation_domain = session.exec(select(SimulationDomainDTO).where(SimulationDomainDTO.name == "vts")).first()
        domain_id=vts_simulation_domain.id if vts_simulation_domain and vts_simulation_domain.id else 0

        if domain_id == 0:
            raise ValueError("vts simulation domain not found")

        generate_root_folder(engine, domain_id, "jajac", "stefw, misve", "1 week",   "R1",2)
        generate_root_folder(engine, domain_id, "jajac", "stefw, misve", "inactive", "R2",3)
        generate_root_folder(engine, domain_id, "jajac", "stefw, misve", "2 weeks",  "R3",4)
        generate_root_folder(engine, domain_id, "jajac", "stefw, misve", "3 weeks",  "R4",5)
        generate_root_folder(engine, domain_id, "misve", "stefw, arlem", "4 weeks",  "R5",6)
        generate_root_folder(engine, domain_id, "karlu", "arlem, caemh", "inactive", "R6",7)
        generate_root_folder(engine, domain_id, "jajac", "stefw, misve", "6 weeks",  "R7",8)
        #generate_root_folder(engine, domain_id, "caemh", "arlem, jajac", False, "R8",9)
        #generate_root_folder(engine, domain_id, "caemh", "arlem, jajac", False, "R9",10)

        with Session(engine) as session:
            root_folders = session.exec(select(RootFolderDTO)).all()
            print("Test data for root_folders inserted successfully:")
            for rf in root_folders:
                print(f" - {rf.path} (ID: {rf.id}), Owner: {rf.owner}, Approvers: {rf.approvers}, CleanUpFrequency: {rf.cleanup_frequency} Folder id: {rf.folder_id}")


from typing import Optional
def generate_node( session: Session,
                   parent:FolderNodeDTO,
                   root_folder_id:int,
                   parent_id:int,
                   parent_name:str,
                   node_type:FolderTypeDTO, 
                   sibling_counter:int, 
                   retention_generator:RandomRetention 
                 ) -> Optional[FolderNodeDTO]:
    child: Optional[FolderNodeDTO] = None
    if node_type is None: 
        return None
    elif node_type.name=="VTS":
        child = FolderNodeDTO(
            rootfolder_id=root_folder_id,
            parent_id=parent_id,
            name=f"VTS_{parent_name}_{sibling_counter + 1}" if parent_id > 0 else f"VTS_{parent_name}",
            type_id=node_type.id,  
            retention_id=retention_generator.next().id
        )
    elif node_type.name=="InnerNode":
        child = FolderNodeDTO(
            rootfolder_id=root_folder_id,
            parent_id=parent_id,
            name=f"{parent_name}_{sibling_counter + 1}" if parent_id > 0 else f"{parent_name}",
            type_id=node_type.id,
            retention_id=0
        )
    else:
        raise Exception("unknown nodetype")
    child.path = f"{parent.path}/{child.name}" if parent and parent.path else child.name

    if not child is None:
        session.add(child)
        session.flush()  # Flush to get the ID without committing
        child.path_ids = f"{parent.path_ids}/{child.id}" if parent else 0  

    return child 

def generate_folder_tree(engine:Engine, root_folder_id:int, root_folder_name:str, max_level:int=1) -> int:
    retention_generator:RandomRetention = RandomRetention(0)
    rand:random.Random = random.Random(42)
    random_node_type: RandomNodeType = RandomNodeType(0)
    

    #print(f"Start GenerateTreeRecursivelyAsync: maxLevel = {max_level} engine:{not engine is None}")

    with Session(engine) as session:
        #generate the root
        id_counter:int = 1
        node_type = random_node_type.get_inner_node_type()
        child_level:int = 0
        current_parent_id:int = 0
        current_parent_name:str = root_folder_name
        root:Optional[FolderNodeDTO] = generate_node(  session=session,
                                                       parent=None, 
                                                       root_folder_id=root_folder_id,
                                                       parent_name=current_parent_name,
                                                       parent_id=current_parent_id, 
                                                       node_type=node_type, 
                                                       sibling_counter=0,
                                                       retention_generator=retention_generator)
        
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
                                node_type = random_node_type.get_simulation_type()
                            elif child_level <= 3 :
                                node_type = random_node_type.get_inner_node_type()
                            else:
                                node_type = random_node_type.next()

                            child:Optional[FolderNodeDTO] = generate_node(  session             = session,
                                                                            parent              = current_parent,
                                                                            root_folder_id      = root_folder_id,
                                                                            parent_id           = current_parent_id, 
                                                                            parent_name         = current_parent_name,
                                                                            node_type           = node_type, 
                                                                            sibling_counter     = i_sibling,
                                                                            retention_generator = retention_generator)
                            if not child is None:
                                if node_type == random_node_type.get_inner_node_type():
                                    next_level_nodes.append(child)
                                nodes_generated += 1

                current_level_nodes = next_level_nodes
        
        # Commit all changes at the end
        session.commit()
        
        # Access the ID before the session closes to avoid DetachedInstanceError
        if root is not None and root.id is not None:
            root_id = root.id
        else:
            raise ValueError("Root folder was not created properly")
    
    print(f"GenerateTreeRecursivelyAsync: Total nodes generated = {id_counter}, maxLevel = {max_level}")
    return root_id