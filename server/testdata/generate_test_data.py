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
        session.add(RetentionTypeDTO(name="Cleaned",          display_rank=1,  is_system_managed=True ))
        session.add(RetentionTypeDTO(name="MarkedForCleanup", display_rank=2,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="CleanupIssue",     display_rank=3,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="New",              display_rank=4,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+1Next",           display_rank=5,  is_system_managed=False ))  
        session.add(RetentionTypeDTO(name="+Q1",              display_rank=6,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+Q3",              display_rank=7,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+Q6",              display_rank=8,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+1Y",              display_rank=9,  is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+2Y",              display_rank=10, is_system_managed=False ))
        session.add(RetentionTypeDTO(name="+3Y",              display_rank=11, is_system_managed=False ))
        session.add(RetentionTypeDTO(name="longterm",         display_rank=12, is_system_managed=False ))
        session.add(RetentionTypeDTO(name="path protected",   display_rank=13, is_system_managed=False ))

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
        return self.simulation_type

    def get_inner_node_type(self) -> FolderTypeDTO:
        return self.inner_node_type
    
def insert_root_folders_metadata_in_db(engine):
    with Session(engine) as session:
        session.add(RootFolderDTO(
            owner="jajac",
            approvers="stefw, misve",
            active_cleanup=True,
            path="R1",
            folder_id=generate_folder_tree("R1", 10)[0].id
            ))
        
        session.add(RootFolderDTO(
            owner="misve",
            approvers="stefw, jajac",
            active_cleanup=True,
            path="R2",
            folder_id=generate_folder_tree("R2", 5)[0].id
            ))
        
        session.add(RootFolderDTO(
            owner="karlu",
            approvers="arlem, jajac",
            active_cleanup=True,
            path="R3",
            folder_id=generate_folder_tree("R3", 5)[0].id
            ))
        
        session.add(RootFolderDTO(
            owner="caemh",
            approvers="arlem, jajac",
            active_cleanup=True,
            path="R4",
            folder_id=generate_folder_tree("R4", 5)[0].id
            ))
        
        #session.add(RootFolderDTO(path="VTS" ))
        session.commit()

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

def generate_node(engine:Engine, parent_id:int, node_type:Optional[FolderTypeDTO], child_level:int, sibling_counter:int, retention_generator:RandomRetention) -> Optional[FolderNodeDTO]:
    child: Optional[FolderNodeDTO] = None
    if node_type is None: 
        return None
    elif node_type.name=="VTS":
        child = FolderNodeDTO(
            parent_id=parent_id,
            name=f"VTS_{child_level}_{sibling_counter + 1}",
            type_id=node_type.id,  
            retention_id=retention_generator.next().id
        )
    elif node_type.name=="InnerNode":
        child = FolderNodeDTO(
            parent_id=parent_id,
            name=f"Inner_{child_level}_{sibling_counter + 1}",
            type_id=node_type.id
        )
    else:
        raise Exception("unknown nodetype")

    if not child is None:
        with Session(engine) as session:
            session.add(child)
            session.commit()

    return child 

def generate_folder_tree(name_root_folder:str, max_level:int=1) -> FolderNodeDTO:
    retention_generator:RandomRetention = RandomRetention(0)
    rand:random.Random = random.Random(42)
    random_node_type: RandomNodeType = RandomNodeType(0)
    engine:Engine = Database.get_engine()

    print(f"Start GenerateTreeRecursivelyAsync: maxLevel = {max_level}")

    #generate the root
    id_counter:int = 1 
    root:Optional[FolderNodeDTO] = generate_node(   engine=engine, 
                                                    parent_id=0, 
                                                    node_type=random_node_type.get_inner_node_type(), 
                                                    child_level=1, 
                                                    sibling_counter=0,
                                                    retention_generator=retention_generator)
    if root is None: 
        root = FolderNodeDTO(id=None, parent_id=0, name=name_root_folder)
    else:
        # generate a folder tree under the rootfolder
        current_level_nodes = [root]
        nodes_generated = 1
        YIELD_EVERY_N_NODES = 100
        for level in range(max_level):
            next_level_nodes = []
            for current_parent in current_level_nodes:
                number_of_children = rand.randint(4, 6)
                if current_parent.id is None: 
                    continue
                else:
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

                        child:Optional[FolderNodeDTO] = generate_node(  engine=engine, 
                                                                        parent_id=current_parent.id, 
                                                                        node_type=node_type, 
                                                                        child_level=child_level, 
                                                                        sibling_counter=i_sibling,
                                                                        retention_generator=retention_generator)
                        if not child is None:
                            if node_type == random_node_type.get_inner_node_type():
                                next_level_nodes.append(child)
                            nodes_generated += 1


            current_level_nodes = next_level_nodes
    print(f"GenerateTreeRecursivelyAsync: Total nodes generated = {id_counter}, maxLevel = {max_level}")
    return root