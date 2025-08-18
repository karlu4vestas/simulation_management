from sqlmodel import Session, select
from sqlalchemy import Engine
from datamodel.dtos import RetentionTypeDTO, FolderTypeDTO, RootFolderDTO, FolderNodeDTO 
from datamodel.db import Database
import random

def insert_test_data_in_db(engine):
    insert_folder_metadata_in_db(engine)
    insert_root_folders_metadata_in_db(engine)

def insert_folder_metadata_in_db(engine):
    with Session(engine) as session:
        session.add(RetentionTypeDTO(name="Marked",           display_rank=2,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="Issue",            display_rank=3,  is_system_managed=False ))
        #session.add(RetentionTypeDTO(name="New",              display_rank=4,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+Next",            display_rank=5,  is_system_managed=False ))  
        session.add(RetentionTypeDTO(name="+Q1",              display_rank=6,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+Q3",              display_rank=7,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+Q6",              display_rank=8,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+1Y",              display_rank=9,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+2Y",              display_rank=10, is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+3Y",              display_rank=11, is_system_managed=False ))
        session.add(RetentionTypeDTO(name="longterm",         display_rank=12, is_system_managed=False ))
        session.add(RetentionTypeDTO(name="path",             display_rank=13, is_system_managed=False ))
        session.add(RetentionTypeDTO(name="Clean",            display_rank=14,  is_system_managed=True  ))

        session.commit()

        retentions = session.exec(select(RetentionTypeDTO)).all()
        print("Test data inserted successfully:")
        for retention in retentions:
            print(f" - {retention.name} (ID: {retention.id})")


    with Session(engine) as session:
        session.add(FolderTypeDTO(name="InnerNode" ))
        session.add(FolderTypeDTO(name="VTS" ))
        session.commit()

        folder_types = session.exec(select(FolderTypeDTO)).all()
        print("Test data for folder_types inserted successfully:")
        for folder in folder_types:
            print(f" - {folder.name} (ID: {folder.id})")

#-------------------------------------
# helper to generate random testdata
class RandomRetention:
    def __init__(self, seed):
        self.rand_int_generator = random.Random(seed)

        with Session(Database.get_engine()) as session:
            self.retention_types = session.exec(select(RetentionTypeDTO)).all()

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

def generate_root_folder(engine: Engine, owner, approvers, active_cleanup, path, levels):
    folder_id = generate_folder_tree(engine, path, levels)
    with Session(engine) as session:
        root_folder = RootFolderDTO(
            owner=owner,
            approvers=approvers,
            active_cleanup=active_cleanup,
            path=path,
            #folder_id=folder_id
        )
        session.add(root_folder)
        session.commit()

        root_folder_id:int = root_folder.id if root_folder.id else 0
        root_folder_name:str = root_folder.path
        root_folder.folder_id=generate_folder_tree(engine, root_folder_id, root_folder_name, levels)
        session.commit()



def insert_root_folders_metadata_in_db(engine):

    generate_root_folder(engine, "jajac", "stefw, misve", True,  "R1",2)
    generate_root_folder(engine, "jajac", "stefw, misve", False, "R2",3)
    generate_root_folder(engine, "jajac", "stefw, misve", True,  "R3",4)
    generate_root_folder(engine, "jajac", "stefw, misve", True,  "R4",5)
    generate_root_folder(engine, "misve", "stefw, arlem", True,  "R5",6)
    generate_root_folder(engine, "karlu", "arlem, caemh", False, "R6",7)
    generate_root_folder(engine, "jajac", "stefw, misve", True,  "R7",8)
    #generate_root_folder(engine, "caemh", "arlem, jajac", False, "R8",9)
    #generate_root_folder(engine, "caemh", "arlem, jajac", False, "R9",10)

    with Session(engine) as session:
        root_folders = session.exec(select(RootFolderDTO)).all()
        print("Test data for root_folders inserted successfully:")
        for rf in root_folders:
            print(f" - {rf.path} (ID: {rf.id}), Owner: {rf.owner}, Approvers: {rf.approvers}, Active Cleanup: {rf.active_cleanup} Folder id: {rf.folder_id}")


#class FolderNodeDTO(SQLModel, table=True):
#    id: int | None = Field(default=None, primary_key=True)
#    parent_id: int = Field(default=0)  # 0 means no parent
#    name: str = Field(default="")
#    type_id: int | None = Field(default=None, foreign_key="foldertypedto.id")
#    modified: str | None = None
#    retention_date: str | None = None
#    retention_id: int | None = Field(default=None, foreign_key="retentiontypedto.id")

from typing import Optional

def generate_node( session: Session, 
                   root_folder_id:int, 
                   parent_id:int, 
                   node_type:FolderTypeDTO, 
                   child_level:int, 
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
            name=f"VTS_{child_level}_{sibling_counter + 1}",
            type_id=node_type.id,  
            retention_id=retention_generator.next().id
        )
    elif node_type.name=="InnerNode":
        child = FolderNodeDTO(
            rootfolder_id=root_folder_id,
            parent_id=parent_id,
            name=f"Inner_{child_level}_{sibling_counter + 1}",
            type_id=node_type.id
        )
    else:
        raise Exception("unknown nodetype")

    if not child is None:
        session.add(child)
        session.flush()  # Flush to get the ID without committing

    return child 

def generate_folder_tree(engine:Engine, root_folder_id:int, root_folder_name:str, max_level:int=1) -> int:
    retention_generator:RandomRetention = RandomRetention(0)
    rand:random.Random = random.Random(42)
    random_node_type: RandomNodeType = RandomNodeType(0)
    

    print(f"Start GenerateTreeRecursivelyAsync: maxLevel = {max_level} engine:{not engine is None}")

    with Session(engine) as session:
        #generate the root
        id_counter:int = 1
        root:Optional[FolderNodeDTO] = generate_node(   session=session, 
                                                        root_folder_id=root_folder_id,
                                                        parent_id=0, 
                                                        node_type=random_node_type.get_inner_node_type(), 
                                                        child_level=1, 
                                                        sibling_counter=0,
                                                        retention_generator=retention_generator)
        if root is None: 
            root = FolderNodeDTO(id=None, rootfolder_id=root_folder_id, parent_id=0, name=root_folder_name)
            session.add(root)
            session.flush()
        else:
            # generate a folder tree under the rootfolder
            current_level_nodes = [root]
            nodes_generated = 1
            YIELD_EVERY_N_NODES = 100
            for level in range(max_level):
                next_level_nodes = []
                for current_parent in current_level_nodes:
                    number_of_children = rand.randint(4, 6)
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

                            child:Optional[FolderNodeDTO] = generate_node(  session=session, 
                                                                            root_folder_id=root_folder_id,
                                                                            parent_id=current_parent_id, 
                                                                            node_type=node_type, 
                                                                            child_level=child_level, 
                                                                            sibling_counter=i_sibling,
                                                                            retention_generator=retention_generator)
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