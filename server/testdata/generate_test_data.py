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

# helper to generate random testdata
class BooleanGenerator:
    def __init__(self):
        self.rnd = random.Random()
    def next_boolean(self):
        return self.rnd.randint(0, 1) == 1
    
def insert_root_folders_metadata_in_db(engine):
    with Session(engine) as session:
        session.add(RootFolderDTO(
            owner="jajac",
            approvers="stefw, misve",
            active_cleanup=True,
            path="R1",
            folder_id=generate_folder_tree("R1", 1)[0].id
            ))
        
        session.add(RootFolderDTO(
            owner="misve",
            approvers="stefw, jajac",
            active_cleanup=True,
            path="R2",
            folder_id=generate_folder_tree("R2", 1)[0].id
            ))
        
        session.add(RootFolderDTO(
            owner="karlu",
            approvers="arlem, jajac",
            active_cleanup=True,
            path="R3",
            folder_id=generate_folder_tree("R3", 1)[0].id
            ))
        
        session.add(RootFolderDTO(
            owner="caemh",
            approvers="arlem, jajac",
            active_cleanup=True,
            path="R4",
            folder_id=generate_folder_tree("R4", 1)[0].id
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
            print(f"FolderNodeDTO {child.id}")

    return child 

def generate_folder_tree(name_root_folder:str, max_level:int=1) -> tuple[FolderNodeDTO, int]:
    retention_generator:RandomRetention = RandomRetention(0)
    bool_gen:BooleanGenerator = BooleanGenerator()
    rand:random.Random = random.Random(42)
    random_node_type: RandomNodeType = RandomNodeType(0)
    engine:Engine = Database.get_engine()

    print(f"Start GenerateTreeRecursivelyAsync: maxLevel = {max_level}")
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
        pass     
    # generate a folder tree for the rootfolder
    """
        current_level_nodes = [root]
        nodes_generated = 0
        YIELD_EVERY_N_NODES = 100
        for level in range(max_level):
            next_level_nodes = []
            for current_parent in current_level_nodes:
                number_of_children = rand.randint(4, 6)
                for i in range(number_of_children):
                    child_id = id_counter + 1
                    id_counter = child_id
                    child_level = level + 1
                    if child_level == max_level:
                        child = LeafNode(
                            id=child_id,
                            parent_id=current_parent.id,
                            parent=current_parent,
                            name=f"SimData_{child_level}_{i + 1}",
                            level=child_level,
                            retention=retention_generator.next(),
                        )
                    else:
                        should_be_leaf_node = child_level > 3 and bool_gen.next_boolean()
                        if should_be_leaf_node:
                            child = LeafNode(
                                id=child_id,
                                parent_id=current_parent.id,
                                parent=current_parent,
                                name=f"SimData_{child_level}_{i + 1}",
                                level=child_level,
                                retention=retention_generator.next(),
                            )
                        else:
                            child = InnerNode(
                                id=child_id,
                                parent_id=current_parent.id,
                                parent=current_parent,
                                name=f"Folder_{child_level}_{i + 1}",
                                level=child_level,
                            )
                            next_level_nodes.append(child)
                    current_parent.children.append(child)
                    nodes_generated += 1

            current_level_nodes = next_level_nodes
            if not current_level_nodes:
                break
        print(f"GenerateTreeRecursivelyAsync: Total nodes generated = {nodes_generated}")
    """

    return (root, id_counter)

#-------------------------------------
"""
import asyncio
import random

class RootFolder:
    def __init__(self, id, root_path="", is_registeredfor_cleanup=False, users=None):
        self.id = id
        self.is_registeredfor_cleanup = is_registeredfor_cleanup
        self.root_path = root_path
        self._folder_tree = None
        self._folder_tree_task = None
        self.users = users if users is not None else []

    @property
    def folder_tree(self):
        if self._folder_tree is None and self._folder_tree_task and self._folder_tree_task.done():
            self._folder_tree = self._folder_tree_task.result()
        return self._folder_tree

    @folder_tree.setter
    def folder_tree(self, value):
        self._folder_tree = value

    async def get_folder_tree_async(self):
        if self._folder_tree is not None:
            return self._folder_tree
        if self._folder_tree_task is None:
            self._folder_tree_task = asyncio.create_task(TestDataGenerator.get_root_folder_tree_async(self))
        self._folder_tree = await self._folder_tree_task
        return self._folder_tree

    @property
    def is_loading_folder_tree(self):
        return self._folder_tree_task is not None and not self._folder_tree_task.done()

    @property
    def retention_headers(self):
        # Assuming DataModel.Instance.RetentionOptions is a list
        return [] if self.folder_tree is None else DataModel.Instance.RetentionOptions


class TestDataGenerator:
    @staticmethod
    async def get_root_folder_tree_async(root_folder):
        if root_folder is None:
            return None
        id_counter = 1
        the_root_folder = InnerNode(
            id=id_counter,
            parent_id=id_counter,
            name=root_folder.root_path,
            is_expanded=True,
            level=0,
        )
        id_counter = the_root_folder.id
        await TestDataGenerator.generate_tree_recursively_async(
            the_root_folder, id_counter, max_level=12
        )
        return the_root_folder

    @staticmethod
    async def generate_tree_recursively_async(parent, id_counter:int, max_level:int):
        retention_generator:RandomRetention = RandomRetention(0)
        bool_gen:BooleanGenerator = BooleanGenerator()
        rand:random.Random = random.Random(42)
        print(f"Start GenerateTreeRecursivelyAsync: maxLevel = {max_level}")

        # generate a folder tree for the rootfolder
        current_level_nodes = [parent]
        nodes_generated = 0
        YIELD_EVERY_N_NODES = 100
        for level in range(max_level):
            next_level_nodes = []
            for current_parent in current_level_nodes:
                number_of_children = rand.randint(4, 6)
                for i in range(number_of_children):
                    child_id = id_counter + 1
                    id_counter = child_id
                    child_level = level + 1
                    if child_level == max_level:
                        child = LeafNode(
                            id=child_id,
                            parent_id=current_parent.id,
                            parent=current_parent,
                            name=f"SimData_{child_level}_{i + 1}",
                            level=child_level,
                            retention=retention_generator.next(),
                        )
                    else:
                        should_be_leaf_node = child_level > 3 and bool_gen.next_boolean()
                        if should_be_leaf_node:
                            child = LeafNode(
                                id=child_id,
                                parent_id=current_parent.id,
                                parent=current_parent,
                                name=f"SimData_{child_level}_{i + 1}",
                                level=child_level,
                                retention=retention_generator.next(),
                            )
                        else:
                            child = InnerNode(
                                id=child_id,
                                parent_id=current_parent.id,
                                parent=current_parent,
                                name=f"Folder_{child_level}_{i + 1}",
                                level=child_level,
                            )
                            next_level_nodes.append(child)
                    current_parent.children.append(child)
                    nodes_generated += 1
                    if nodes_generated % YIELD_EVERY_N_NODES == 0:
                        print(f"GenerateTreeRecursivelyAsync: YIELD_EVERY_N_NODES : Total nodes generated = {nodes_generated}")
                        await asyncio.sleep(0)
            current_level_nodes = next_level_nodes
            if not current_level_nodes:
                break
        print(f"GenerateTreeRecursivelyAsync: Total nodes generated = {nodes_generated}")

    @staticmethod
    def gen_test_root_folders_for_user(user):
        root_folders = []
        root_id = 1
        root_folders.append(RootFolder(
            id=root_id,
            is_registeredfor_cleanup=True,
            users=[user, User("jajac"), User("misve")],
            root_path="\\\\domain.net\\root_1"
        ))
        root_id += 1
        root_folders.append(RootFolder(
            id=root_id,
            is_registeredfor_cleanup=True,
            users=[user, User("stefw"), User("misve")],
            root_path="\\\\domain.net\\root_2"
        ))
        root_folders.append(RootFolder(
            id=root_id,
            users=[user, User("facap"), User("misve")],
            root_path="\\\\domain.net\\root_3"
        ))
        root_id += 1
        root_folders.append(RootFolder(
            id=root_id,
            users=[user, User("caemh"), User("arlem")],
            root_path="\\\\domain.net\\root_4"
        ))
        return root_folders
"""