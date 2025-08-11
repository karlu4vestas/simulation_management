import pytest
from sqlmodel import Field, Session, select, asc
from datamodel.DTOs import RootFolderDTO, FolderNodeDTO, NodeAttributesDTO, FolderTypeDTO, RetentionDTO


class TestRootFolderDTO:
    """Test RootFolderDTO database operations"""

    def test_root_folder_create_and_retrieve(self, test_session, sample_root_folder_data):
        """Test creating, saving, and retrieving a RootFolderDTO"""
        # Create and save
        original_folder = RootFolderDTO(**sample_root_folder_data)
        test_session.add(original_folder)
        test_session.commit()
        test_session.refresh(original_folder)
        
        # Verify ID was assigned
        assert original_folder.id is not None
        folder_id = original_folder.id
        
        # Retrieve from database
        retrieved_folder = test_session.get(RootFolderDTO, folder_id)
        
        # Verify all attributes are identical
        assert retrieved_folder is not None
        assert retrieved_folder.id == original_folder.id
        assert retrieved_folder.path == original_folder.path
        assert retrieved_folder.folder_id == original_folder.folder_id
        assert retrieved_folder.owner == original_folder.owner
        assert retrieved_folder.approvers == original_folder.approvers
        assert retrieved_folder.active_cleanup == original_folder.active_cleanup

    def test_root_folder_update_and_retrieve(self, test_session, sample_root_folder_data):
        """Test updating and retrieving a RootFolderDTO"""
        # Create and save
        folder = RootFolderDTO(**sample_root_folder_data)
        test_session.add(folder)
        test_session.commit()
        test_session.refresh(folder)
        folder_id = folder.id
        
        # Update attributes
        folder.path = "/updated/path"
        folder.owner = "UpdatedOwner"
        folder.active_cleanup = True
        test_session.commit()
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_folder = test_session.get(RootFolderDTO, folder_id)
        
        # Verify updated values
        assert retrieved_folder.path == "/updated/path"
        assert retrieved_folder.owner == "UpdatedOwner"
        assert retrieved_folder.active_cleanup is True
        assert retrieved_folder.folder_id == sample_root_folder_data["folder_id"]  # Unchanged
        assert retrieved_folder.approvers == sample_root_folder_data["approvers"]  # Unchanged

    def test_root_folder_query_by_attributes(self, test_session):
        """Test querying RootFolderDTO by various attributes"""
        # Create multiple folders
        folders_data = [
            {"path": "/folder1", "folder_id": 1, "owner": "Owner1", "approvers": "A,B", "active_cleanup": False},
            {"path": "/folder2", "folder_id": 2, "owner": "Owner2", "approvers": "C,D", "active_cleanup": True},
            {"path": "/folder3", "folder_id": 3, "owner": "Owner1", "approvers": "E,F", "active_cleanup": False}
        ]
        
        for data in folders_data:
            folder = RootFolderDTO(**data)
            test_session.add(folder)
        test_session.commit()
        
        # Query by owner
        owner1_folders = test_session.exec(
            select(RootFolderDTO).where(RootFolderDTO.owner == "Owner1")
        ).all()
        assert len(owner1_folders) == 2
        assert all(f.owner == "Owner1" for f in owner1_folders)
        
        # Query by active_cleanup
        active_folders = test_session.exec(
            select(RootFolderDTO).where(RootFolderDTO.active_cleanup == True)
        ).all()
        assert len(active_folders) == 1
        assert active_folders[0].path == "/folder2"


class TestFolderNodeDTO:
    """Test FolderNodeDTO database operations"""

    def test_folder_node_create_and_retrieve(self, test_session, sample_folder_node_data):
        """Test creating, saving, and retrieving a FolderNodeDTO"""
        # Create and save
        original_node = FolderNodeDTO(**sample_folder_node_data)
        test_session.add(original_node)
        test_session.commit()
        test_session.refresh(original_node)
        
        # Verify ID was assigned
        assert original_node.id is not None
        node_id = original_node.id
        
        # Retrieve from database
        retrieved_node = test_session.get(FolderNodeDTO, node_id)
        
        # Verify all attributes are identical
        assert retrieved_node is not None
        assert retrieved_node.id == original_node.id
        assert retrieved_node.parent_id == original_node.parent_id
        assert retrieved_node.name == original_node.name
        assert retrieved_node.type_id == original_node.type_id
        assert retrieved_node.node_attributes == original_node.node_attributes

    def test_folder_node_with_defaults_create_and_retrieve(self, test_session):
        """Test FolderNodeDTO with default values through database round-trip"""
        # Create with minimal data (testing defaults)
        original_node = FolderNodeDTO(node_attributes=42)
        test_session.add(original_node)
        test_session.commit()
        test_session.refresh(original_node)
        node_id = original_node.id
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_node = test_session.get(FolderNodeDTO, node_id)
        
        # Verify defaults persisted correctly
        assert retrieved_node.parent_id == 0      # Default value
        assert retrieved_node.name == ""          # Default value
        assert retrieved_node.type_id == 0        # Default value
        assert retrieved_node.node_attributes == 42  # Provided value

    def test_folder_node_hierarchy_creation(self, test_session):
        """Test creating parent-child folder relationships and retrieving them"""
        # Create parent folder
        parent = FolderNodeDTO(name="ParentFolder", type_id=1, node_attributes=100)
        test_session.add(parent)
        test_session.commit()
        test_session.refresh(parent)
        parent_id = parent.id
        
        # Create child folder
        child = FolderNodeDTO(
            parent_id=parent.id,  # Use parent.id directly instead of parent_id variable
            name="ChildFolder", 
            type_id=2, 
            node_attributes=200
        )
        test_session.add(child)
        test_session.commit()
        test_session.refresh(child)
        child_id = child.id
        
        # Clear session and retrieve both
        test_session.expunge_all()
        retrieved_parent = test_session.get(FolderNodeDTO, parent_id)
        retrieved_child = test_session.get(FolderNodeDTO, child_id)
        
        # Verify parent attributes
        assert retrieved_parent.name == "ParentFolder"
        assert retrieved_parent.parent_id == 0  # Root level
        assert retrieved_parent.type_id == 1
        assert retrieved_parent.node_attributes == 100
        
        # Verify child attributes and relationship
        assert retrieved_child.name == "ChildFolder"
        assert retrieved_child.parent_id == retrieved_parent.id  # Points to parent
        assert retrieved_child.type_id == 2
        assert retrieved_child.node_attributes == 200
        
        # Query children by parent_id
        children = test_session.exec(
            select(FolderNodeDTO).where(FolderNodeDTO.parent_id == retrieved_parent.id)
        ).all()
        assert len(children) == 1
        assert children[0].id == child_id


class TestNodeAttributesDTO:
    """Test NodeAttributesDTO database operations"""

    def test_node_attributes_create_and_retrieve(self, test_session):
        """Test creating, saving, and retrieving a NodeAttributesDTO"""
        # Create and save
        original_attrs = NodeAttributesDTO(
            node_id=1, 
            retention_id=2,
            retention_date="2025-12-31",
            modified="2025-08-11"
        )
        test_session.add(original_attrs)
        test_session.commit()
        test_session.refresh(original_attrs)
        
        # Retrieve from database using node_id (primary key)
        retrieved_attrs = test_session.get(NodeAttributesDTO, 1)
        
        # Verify all attributes are identical
        assert retrieved_attrs is not None
        assert retrieved_attrs.node_id == original_attrs.node_id
        assert retrieved_attrs.retention_id == original_attrs.retention_id
        assert retrieved_attrs.retention_date == original_attrs.retention_date
        assert retrieved_attrs.modified == original_attrs.modified

    def test_node_attributes_with_defaults_create_and_retrieve(self, test_session):
        """Test NodeAttributesDTO with default values through database round-trip"""
        # Create with minimal data (testing defaults)
        original_attrs = NodeAttributesDTO(node_id=5)
        test_session.add(original_attrs)
        test_session.commit()
        test_session.refresh(original_attrs)
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_attrs = test_session.get(NodeAttributesDTO, 5)
        
        # Verify defaults persisted correctly
        assert retrieved_attrs.node_id == 5
        assert retrieved_attrs.retention_id == 0      # Default value
        assert retrieved_attrs.retention_date is None  # Default value
        assert retrieved_attrs.modified is None       # Default value

    def test_node_attributes_update_and_retrieve(self, test_session):
        """Test updating and retrieving NodeAttributesDTO"""
        # Create and save
        attrs = NodeAttributesDTO(node_id=10, retention_id=1)
        test_session.add(attrs)
        test_session.commit()
        
        # Update attributes
        attrs.retention_id = 5
        attrs.retention_date = "2026-01-01"
        attrs.modified = "2025-08-12"
        test_session.commit()
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_attrs = test_session.get(NodeAttributesDTO, 10)
        
        # Verify updated values
        assert retrieved_attrs.retention_id == 5
        assert retrieved_attrs.retention_date == "2026-01-01"
        assert retrieved_attrs.modified == "2025-08-12"
        assert retrieved_attrs.node_id == 10  # Unchanged (primary key)


class TestFolderTypeDTO:
    """Test FolderTypeDTO database operations"""

    def test_folder_type_create_and_retrieve(self, test_session):
        """Test creating, saving, and retrieving a FolderTypeDTO"""
        # Create and save
        original_type = FolderTypeDTO(name="VTSSimulation")
        test_session.add(original_type)
        test_session.commit()
        test_session.refresh(original_type)
        
        # Verify ID was assigned
        assert original_type.id is not None
        type_id = original_type.id
        
        # Retrieve from database
        retrieved_type = test_session.get(FolderTypeDTO, type_id)
        
        # Verify all attributes are identical
        assert retrieved_type is not None
        assert retrieved_type.id == original_type.id
        assert retrieved_type.name == original_type.name

    def test_folder_type_with_defaults_create_and_retrieve(self, test_session):
        """Test FolderTypeDTO with default values through database round-trip"""
        # Create with defaults
        original_type = FolderTypeDTO()  # Uses default name "InnerNode"
        test_session.add(original_type)
        test_session.commit()
        test_session.refresh(original_type)
        type_id = original_type.id
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_type = test_session.get(FolderTypeDTO, type_id)
        
        # Verify defaults persisted correctly
        assert retrieved_type.name == "InnerNode"  # Default value

    def test_folder_type_query_by_name(self, test_session):
        """Test querying FolderTypeDTO by name"""
        # Create multiple folder types
        types_data = ["InnerNode", "LeafNode", "VTSSimulation", "CustomType"]
        created_types = []
        
        for name in types_data:
            folder_type = FolderTypeDTO(name=name)
            test_session.add(folder_type)
            created_types.append(folder_type)
        test_session.commit()
        
        # Query by specific name
        vts_type = test_session.exec(
            select(FolderTypeDTO).where(FolderTypeDTO.name == "VTSSimulation")
        ).first()
        assert vts_type is not None
        assert vts_type.name == "VTSSimulation"
        
        # Query all types
        all_types = test_session.exec(select(FolderTypeDTO)).all()
        assert len(all_types) == 4
        type_names = [t.name for t in all_types]
        assert set(type_names) == set(types_data)


class TestRetentionDTO:
    """Test RetentionDTO database operations"""

    def test_retention_create_and_retrieve(self, test_session, sample_retention_data):
        """Test creating, saving, and retrieving a RetentionDTO"""
        # Create and save
        original_retention = RetentionDTO(**sample_retention_data)
        test_session.add(original_retention)
        test_session.commit()
        test_session.refresh(original_retention)
        
        # Verify ID was assigned
        assert original_retention.id is not None
        retention_id = original_retention.id
        
        # Retrieve from database
        retrieved_retention = test_session.get(RetentionDTO, retention_id)
        
        # Verify all attributes are identical
        assert retrieved_retention is not None
        assert retrieved_retention.id == original_retention.id
        assert retrieved_retention.name == original_retention.name
        assert retrieved_retention.is_system_managed == original_retention.is_system_managed
        assert retrieved_retention.display_rank == original_retention.display_rank

    def test_retention_with_defaults_create_and_retrieve(self, test_session):
        """Test RetentionDTO with default values through database round-trip"""
        # Create with defaults
        original_retention = RetentionDTO()
        test_session.add(original_retention)
        test_session.commit()
        test_session.refresh(original_retention)
        retention_id = original_retention.id
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_retention = test_session.get(RetentionDTO, retention_id)
        
        # Verify defaults persisted correctly
        assert retrieved_retention.name == ""          # Default value
        assert retrieved_retention.is_system_managed == ""  # Default value
        assert retrieved_retention.display_rank == 0   # Default value

    def test_retention_system_managed_create_and_retrieve(self, test_session):
        """Test RetentionDTO system managed flag through database round-trip"""
        # Create system managed retention
        original_retention = RetentionDTO(
            name="Auto-Cleanup",
            is_system_managed="true",
            display_rank=99
        )
        test_session.add(original_retention)
        test_session.commit()
        test_session.refresh(original_retention)
        retention_id = original_retention.id
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_retention = test_session.get(RetentionDTO, retention_id)
        
        # Verify system managed attributes
        assert retrieved_retention.name == "Auto-Cleanup"
        assert retrieved_retention.is_system_managed == "true"
        assert retrieved_retention.display_rank == 99

    def test_retention_query_by_display_rank(self, test_session):
        """Test querying RetentionDTO by display rank"""
        # Create multiple retention policies
        retentions_data = [
            {"name": "7 days", "is_system_managed": "false", "display_rank": 1},
            {"name": "30 days", "is_system_managed": "false", "display_rank": 2},
            {"name": "90 days", "is_system_managed": "false", "display_rank": 3},
            {"name": "Never", "is_system_managed": "true", "display_rank": 99}
        ]
        
        for data in retentions_data:
            retention = RetentionDTO(**data)
            test_session.add(retention)
        test_session.commit()
        
        # Query system managed retentions
        system_managed = test_session.exec(
            select(RetentionDTO).where(RetentionDTO.is_system_managed == "true")
        ).all()
        assert len(system_managed) == 1
        assert system_managed[0].name == "Never"
        
        # Query by display rank order
        ordered_retentions = test_session.exec(
            select(RetentionDTO).order_by(asc(RetentionDTO.display_rank))
        ).all()
        expected_order = ["7 days", "30 days", "90 days", "Never"]
        actual_order = [r.name for r in ordered_retentions]
        assert actual_order == expected_order


class TestDTODatabaseIntegration:
    """Test DTO database integration and relationships"""

    def test_all_dtos_table_creation(self, test_session):
        """Test that all DTOs can be created as database tables and accessed"""
        # Create one instance of each DTO type
        root_folder = RootFolderDTO(
            path="/test", folder_id=1, owner="test", approvers="test", active_cleanup=False
        )
        folder_node = FolderNodeDTO(node_attributes=0)
        node_attrs = NodeAttributesDTO(node_id=100)
        folder_type = FolderTypeDTO(name="TestType")
        retention = RetentionDTO(name="TestRetention")
        
        # Add all to session
        test_session.add_all([root_folder, folder_node, node_attrs, folder_type, retention])
        test_session.commit()
        test_session.refresh(root_folder)
        test_session.refresh(folder_node)
        test_session.refresh(folder_type)
        test_session.refresh(retention)
        
        # Verify all have IDs assigned (except NodeAttributesDTO which uses node_id)
        assert root_folder.id is not None
        assert folder_node.id is not None
        assert node_attrs.node_id == 100
        assert folder_type.id is not None
        assert retention.id is not None
        
        # Verify all can be retrieved
        retrieved_root = test_session.get(RootFolderDTO, root_folder.id)
        retrieved_node = test_session.get(FolderNodeDTO, folder_node.id)
        retrieved_attrs = test_session.get(NodeAttributesDTO, 100)
        retrieved_type = test_session.get(FolderTypeDTO, folder_type.id)
        retrieved_retention = test_session.get(RetentionDTO, retention.id)
        
        assert all([
            retrieved_root is not None,
            retrieved_node is not None,
            retrieved_attrs is not None,
            retrieved_type is not None,
            retrieved_retention is not None
        ])

    def test_dto_primary_key_behavior(self, test_session):
        """Test primary key behavior for all DTOs"""
        # Test auto-incrementing primary keys
        folder1 = FolderNodeDTO(name="Folder1", node_attributes=1)
        folder2 = FolderNodeDTO(name="Folder2", node_attributes=2)
        
        test_session.add_all([folder1, folder2])
        test_session.commit()
        test_session.refresh(folder1)
        test_session.refresh(folder2)
        
        # Verify sequential IDs
        assert folder1.id is not None
        assert folder2.id is not None
        assert folder2.id == folder1.id + 1
        
        # Test custom primary key (NodeAttributesDTO)
        attrs1 = NodeAttributesDTO(node_id=500, retention_id=1)
        attrs2 = NodeAttributesDTO(node_id=600, retention_id=2)
        
        test_session.add_all([attrs1, attrs2])
        test_session.commit()
        
        # Verify custom primary keys
        retrieved_attrs1 = test_session.get(NodeAttributesDTO, 500)
        retrieved_attrs2 = test_session.get(NodeAttributesDTO, 600)
        
        assert retrieved_attrs1 is not None
        assert retrieved_attrs2 is not None
        assert retrieved_attrs1.retention_id == 1
        assert retrieved_attrs2.retention_id == 2

    def test_dto_field_data_integrity(self, test_session):
        """Test that all field types preserve data correctly through database round-trip"""
        # Test all data types and edge cases
        root_folder = RootFolderDTO(
            path="/very/long/path/with/special/chars/äöü/测试",
            folder_id=999999,
            owner="Owner with spaces and åæø",
            approvers="A1,B2,C3,D4,E5",
            active_cleanup=True
        )
        
        node_attrs = NodeAttributesDTO(
            node_id=123456,
            retention_id=789,
            retention_date="2025-12-31",
            modified="2025-08-11T14:30:00Z"
        )
        
        retention = RetentionDTO(
            name="Very long retention policy name with special characters ñü",
            is_system_managed="true",
            display_rank=2147483647  # Max int value
        )
        
        test_session.add_all([root_folder, node_attrs, retention])
        test_session.commit()
        test_session.refresh(root_folder)
        test_session.refresh(retention)
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        
        retrieved_folder = test_session.get(RootFolderDTO, root_folder.id)
        retrieved_attrs = test_session.get(NodeAttributesDTO, 123456)
        retrieved_retention = test_session.get(RetentionDTO, retention.id)
        
        # Verify all complex data preserved exactly
        assert retrieved_folder.path == "/very/long/path/with/special/chars/äöü/测试"
        assert retrieved_folder.folder_id == 999999
        assert retrieved_folder.owner == "Owner with spaces and åæø"
        assert retrieved_folder.approvers == "A1,B2,C3,D4,E5"
        assert retrieved_folder.active_cleanup is True
        
        assert retrieved_attrs.node_id == 123456
        assert retrieved_attrs.retention_id == 789
        assert retrieved_attrs.retention_date == "2025-12-31"
        assert retrieved_attrs.modified == "2025-08-11T14:30:00Z"
        
        assert retrieved_retention.name == "Very long retention policy name with special characters ñü"
        assert retrieved_retention.is_system_managed == "true"
        assert retrieved_retention.display_rank == 2147483647
