import pytest
from sqlmodel import Session, select
from datamodel.DTOs import RootFolderDTO, FolderNodeDTO, NodeAttributesDTO, FolderTypeDTO, RetentionDTO
from datamodel.db import Database


class TestDatabaseOperations:
    """Test database operations with DTOs"""

    def test_create_and_retrieve_root_folder(self, clean_database, sample_root_folder_data):
        """Test creating and retrieving a RootFolderDTO from database"""
        engine = Database.get_engine()
        
        with Session(engine) as session:
            # Create and save
            folder = RootFolderDTO(**sample_root_folder_data)
            session.add(folder)
            session.commit()
            session.refresh(folder)
            
            # Verify it was saved with an ID
            assert folder.id is not None
            saved_id = folder.id
            
            # Retrieve and verify
            statement = select(RootFolderDTO).where(RootFolderDTO.id == saved_id)
            retrieved_folder = session.exec(statement).first()
            
            assert retrieved_folder is not None
            assert retrieved_folder.path == "/test/folder"
            assert retrieved_folder.owner == "JD"
            assert retrieved_folder.active_cleanup is False

    def test_create_multiple_folder_nodes(self, clean_database):
        """Test creating multiple FolderNodeDTO records"""
        engine = Database.get_engine()
        
        with Session(engine) as session:
            # Create parent node
            parent = FolderNodeDTO(name="Parent", type_id=1, node_attributes=0)
            session.add(parent)
            session.commit()
            session.refresh(parent)
            
            # Create child nodes
            child1 = FolderNodeDTO(parent_id=parent.id, name="Child1", type_id=2, node_attributes=0)
            child2 = FolderNodeDTO(parent_id=parent.id, name="Child2", type_id=2, node_attributes=0)
            
            session.add(child1)
            session.add(child2)
            session.commit()
            
            # Verify parent-child relationships
            statement = select(FolderNodeDTO).where(FolderNodeDTO.parent_id == parent.id)
            children = session.exec(statement).all()
            
            assert len(children) == 2
            child_names = [child.name for child in children]
            assert "Child1" in child_names
            assert "Child2" in child_names

    def test_update_folder_node(self, clean_database, sample_folder_node_data):
        """Test updating a FolderNodeDTO record"""
        engine = Database.get_engine()
        
        with Session(engine) as session:
            # Create
            node = FolderNodeDTO(**sample_folder_node_data)
            session.add(node)
            session.commit()
            session.refresh(node)
            
            # Update
            node.name = "UpdatedFolder"
            node.type_id = 99
            session.add(node)
            session.commit()
            
            # Verify update
            statement = select(FolderNodeDTO).where(FolderNodeDTO.id == node.id)
            updated_node = session.exec(statement).first()
            
            assert updated_node.name == "UpdatedFolder"
            assert updated_node.type_id == 99

    def test_delete_retention_record(self, clean_database, sample_retention_data):
        """Test deleting a RetentionDTO record"""
        engine = Database.get_engine()
        
        with Session(engine) as session:
            # Create
            retention = RetentionDTO(**sample_retention_data)
            session.add(retention)
            session.commit()
            session.refresh(retention)
            saved_id = retention.id
            
            # Delete
            session.delete(retention)
            session.commit()
            
            # Verify deletion
            statement = select(RetentionDTO).where(RetentionDTO.id == saved_id)
            deleted_retention = session.exec(statement).first()
            
            assert deleted_retention is None

    def test_node_attributes_relationship(self, clean_database):
        """Test NodeAttributesDTO with FolderNodeDTO relationship"""
        engine = Database.get_engine()
        
        with Session(engine) as session:
            # Create a folder node
            node = FolderNodeDTO(name="TestNode", type_id=1, node_attributes=0)
            session.add(node)
            session.commit()
            session.refresh(node)
            
            # Create node attributes for this node
            attrs = NodeAttributesDTO(
                node_id=node.id,
                retention_id=5,
                retention_date="2025-12-31",
                modified="2025-08-11"
            )
            session.add(attrs)
            session.commit()
            
            # Verify the relationship
            statement = select(NodeAttributesDTO).where(NodeAttributesDTO.node_id == node.id)
            retrieved_attrs = session.exec(statement).first()
            
            assert retrieved_attrs is not None
            assert retrieved_attrs.node_id == node.id
            assert retrieved_attrs.retention_date == "2025-12-31"

    def test_folder_type_enum_like_behavior(self, clean_database):
        """Test FolderTypeDTO as an enum-like structure"""
        engine = Database.get_engine()
        
        with Session(engine) as session:
            # Create standard folder types
            inner_node = FolderTypeDTO(name="InnerNode")
            vts_simulation = FolderTypeDTO(name="VTSSimulation")
            leaf_node = FolderTypeDTO(name="LeafNode")
            
            session.add(inner_node)
            session.add(vts_simulation)
            session.add(leaf_node)
            session.commit()
            
            # Retrieve all folder types
            statement = select(FolderTypeDTO)
            folder_types = session.exec(statement).all()
            
            assert len(folder_types) == 3
            type_names = [ft.name for ft in folder_types]
            assert "InnerNode" in type_names
            assert "VTSSimulation" in type_names
            assert "LeafNode" in type_names

    def test_retention_display_ranking(self, clean_database):
        """Test RetentionDTO display ranking functionality"""
        engine = Database.get_engine()
        
        with Session(engine) as session:
            # Create retention periods with different ranks
            retentions = [
                RetentionDTO(name="New", display_rank=1),
                RetentionDTO(name="+1 round", display_rank=2),
                RetentionDTO(name="+3 rounds", display_rank=3),
                RetentionDTO(name="longterm", display_rank=10),
                RetentionDTO(name="Cleaned", is_system_managed="true", display_rank=0)
            ]
            
            for retention in retentions:
                session.add(retention)
            session.commit()
            
            # Retrieve ordered by display rank
            statement = select(RetentionDTO).order_by(RetentionDTO.display_rank)
            ordered_retentions = session.exec(statement).all()
            
            assert len(ordered_retentions) == 5
            assert ordered_retentions[0].name == "Cleaned"  # rank 0
            assert ordered_retentions[1].name == "New"      # rank 1
            assert ordered_retentions[-1].name == "longterm"  # rank 10

    def test_database_persistence_across_sessions(self, clean_database, sample_root_folder_data):
        """Test that data persists across different database sessions"""
        engine = Database.get_engine()
        
        # First session - create data
        with Session(engine) as session:
            folder = RootFolderDTO(**sample_root_folder_data)
            session.add(folder)
            session.commit()
            session.refresh(folder)
            saved_id = folder.id
        
        # Second session - retrieve data
        with Session(engine) as session:
            statement = select(RootFolderDTO).where(RootFolderDTO.id == saved_id)
            retrieved_folder = session.exec(statement).first()
            
            assert retrieved_folder is not None
            assert retrieved_folder.path == "/test/folder"
            assert retrieved_folder.owner == "JD"
