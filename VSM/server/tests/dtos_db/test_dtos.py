import pytest
from sqlmodel import Field, Session, select, asc
from datamodel.dtos import RootFolderDTO, FolderNodeDTO, FolderTypeDTO, RetentionTypeDTO


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
        #assert retrieved_folder.active_cleanup == original_folder.active_cleanup

    def test_root_folder_update_and_retrieve(self, test_session, sample_root_folder_data):
        """Test updating and retrieving a RootFolderDTO"""
        # Create and save initial folder
        folder = RootFolderDTO(**sample_root_folder_data)
        test_session.add(folder)
        test_session.commit()
        test_session.refresh(folder)
        folder_id = folder.id
        
        # Create expected updated state
        expected_folder = RootFolderDTO(
            id=folder_id,
            path="/updated/path",
            folder_id=sample_root_folder_data["folder_id"],  # Unchanged
            owner="UpdatedOwner",
            approvers=sample_root_folder_data["approvers"]  # Unchanged
            #active_cleanup=True
        )
        
        # Apply updates from expected object
        folder.path = expected_folder.path
        folder.owner = expected_folder.owner
        #folder.active_cleanup = expected_folder.active_cleanup
        test_session.commit()
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_folder = test_session.get(RootFolderDTO, folder_id)
        
        # Verify against expected values
        assert retrieved_folder.path == expected_folder.path
        assert retrieved_folder.owner == expected_folder.owner
        #assert retrieved_folder.active_cleanup == expected_folder.active_cleanup
        assert retrieved_folder.folder_id == expected_folder.folder_id  # Unchanged
        assert retrieved_folder.approvers == expected_folder.approvers  # Unchanged

    def test_root_folder_query_by_attributes(self, test_session):
        """Test querying RootFolderDTO by various attributes"""
        # Define expected test folders
        expected_folders = [
            RootFolderDTO(path="/folder1", folder_id=1, owner="Owner1", approvers="A,B"),
            RootFolderDTO(path="/folder2", folder_id=2, owner="Owner2", approvers="C,D"),
            RootFolderDTO(path="/folder3", folder_id=3, owner="Owner1", approvers="E,F")
        ]
        
        # Create folders from expected data
        for expected_folder in expected_folders:
            folder = RootFolderDTO(
                path=expected_folder.path,
                folder_id=expected_folder.folder_id,
                owner=expected_folder.owner,
                approvers=expected_folder.approvers,
                #active_cleanup=expected_folder.active_cleanup
            )
            test_session.add(folder)
        test_session.commit()
        
        # Query by owner
        owner1_folders = test_session.exec(
            select(RootFolderDTO).where(RootFolderDTO.owner == "Owner1")
        ).all()
        expected_owner1_count = len([f for f in expected_folders if f.owner == "Owner1"])
        assert len(owner1_folders) == expected_owner1_count
        assert all(f.owner == "Owner1" for f in owner1_folders)
        
        # Query by active_cleanup
        #active_folders = test_session.exec(
        #    select(RootFolderDTO).where(RootFolderDTO.active_cleanup == True)
        #).all()
        #expected_active_paths = [f.path for f in expected_folders if f.active_cleanup]
        #assert len(active_folders) == len(expected_active_paths)
        #assert active_folders[0].path in expected_active_paths


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
        assert retrieved_node.type_id == original_node.nodetype_id

    def test_folder_node_with_defaults_create_and_retrieve(self, test_session):
        """Test FolderNodeDTO with default values through database round-trip"""
        # Create with minimal data (testing defaults)
        original_node = FolderNodeDTO()
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
        assert retrieved_node.type_id is None     # Default value
        assert retrieved_node.modified is None    # Default value  
        assert retrieved_node.retention_date is None  # Default value
        assert retrieved_node.retention_id is None    # Default value

    def test_folder_node_with_attributes_create_and_retrieve(self, test_session):
        """Test creating and retrieving a FolderNodeDTO with attributes"""
        # Create and save
        original_node = FolderNodeDTO(
            parent_id=1,
            name="TestNode",
            nodetype_id=2,
            modified_date="2025-08-12T10:30:00Z",
            expiration_date="2025-12-31",
            retention_id=5
        )
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
        assert retrieved_node.type_id == original_node.nodetype_id
        assert retrieved_node.modified == original_node.modified_date
        assert retrieved_node.retention_date == original_node.expiration_date
        assert retrieved_node.retention_id == original_node.retention_id

    def test_folder_node_attributes_update_and_retrieve(self, test_session):
        """Test updating and retrieving FolderNodeDTO attributes"""
        # Create and save initial node
        initial_node = FolderNodeDTO(
            parent_id=1,
            name="UpdateTest",
            nodetype_id=1
        )
        test_session.add(initial_node)
        test_session.commit()
        test_session.refresh(initial_node)
        node_id = initial_node.id
        
        # Update attributes
        initial_node.modified_date = "2025-08-12T15:45:00Z"
        initial_node.expiration_date = "2026-01-01"
        initial_node.retention_id = 3
        test_session.commit()
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_node = test_session.get(FolderNodeDTO, node_id)
        
        # Verify updates persisted
        assert retrieved_node.modified == "2025-08-12T15:45:00Z"
        assert retrieved_node.retention_date == "2026-01-01"
        assert retrieved_node.retention_id == 3
        # Verify other fields unchanged
        assert retrieved_node.parent_id == 1
        assert retrieved_node.name == "UpdateTest"
        assert retrieved_node.type_id == 1

    def test_folder_node_hierarchy_creation(self, test_session):
        """Test creating parent-child folder relationships and retrieving them"""
        # Create expected parent folder
        expected_parent = FolderNodeDTO(
            name="ParentFolder", 
            nodetype_id=1, 
            parent_id=0  # Root level
        )
        
        # Create and save parent
        parent = FolderNodeDTO(
            name=expected_parent.name,
            nodetype_id=expected_parent.nodetype_id,
            parent_id=expected_parent.parent_id
        )
        test_session.add(parent)
        test_session.commit()
        test_session.refresh(parent)
        parent_id = parent.id  # Store ID before expunge
        
        # Create expected child folder
        expected_child = FolderNodeDTO(
            parent_id=parent_id or 0,  # Use stored ID
            name="ChildFolder",
            nodetype_id=2
        )
        
        # Create child folder from expected values
        child = FolderNodeDTO(
            parent_id=expected_child.parent_id,
            name=expected_child.name,
            nodetype_id=expected_child.nodetype_id
        )
        test_session.add(child)
        test_session.commit()
        test_session.refresh(child)
        child_id = child.id  # Store ID before expunge
        
        # Clear session and retrieve both
        test_session.expunge_all()
        retrieved_parent = test_session.get(FolderNodeDTO, parent_id)
        retrieved_child = test_session.get(FolderNodeDTO, child_id)
        
        # Verify parent attributes match expected
        assert retrieved_parent.name == expected_parent.name
        assert retrieved_parent.parent_id == expected_parent.parent_id
        assert retrieved_parent.type_id == expected_parent.nodetype_id
        
        # Verify child attributes match expected
        assert retrieved_child.name == expected_child.name
        assert retrieved_child.parent_id == parent_id  # Use stored parent ID
        assert retrieved_child.type_id == expected_child.nodetype_id
        
        # Query children by parent_id
        children = test_session.exec(
            select(FolderNodeDTO).where(FolderNodeDTO.parent_id == parent_id)
        ).all()
        assert len(children) == 1
        assert children[0].id == child_id


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
        # Define expected folder types
        expected_type_names = ["InnerNode", "LeafNode", "VTSSimulation", "CustomType"]
        expected_types = [FolderTypeDTO(name=name) for name in expected_type_names]
        
        # Create folder types from expected data
        for expected_type in expected_types:
            folder_type = FolderTypeDTO(name=expected_type.name)
            test_session.add(folder_type)
        test_session.commit()
        
        # Query by specific name
        target_name = "VTSSimulation"
        vts_type = test_session.exec(
            select(FolderTypeDTO).where(FolderTypeDTO.name == target_name)
        ).first()
        assert vts_type is not None
        assert vts_type.name == target_name
        
        # Query all types
        all_types = test_session.exec(select(FolderTypeDTO)).all()
        assert len(all_types) == len(expected_type_names)
        retrieved_names = [t.name for t in all_types]
        assert set(retrieved_names) == set(expected_type_names)


class TestRetentionDTO:
    """Test RetentionDTO database operations"""

    def test_retention_create_and_retrieve(self, test_session, sample_retention_data):
        """Test creating, saving, and retrieving a RetentionDTO"""
        # Create and save
        original_retention = RetentionTypeDTO(**sample_retention_data)
        test_session.add(original_retention)
        test_session.commit()
        test_session.refresh(original_retention)
        
        # Verify ID was assigned
        assert original_retention.id is not None
        retention_id = original_retention.id
        
        # Retrieve from database
        retrieved_retention = test_session.get(RetentionTypeDTO, retention_id)
        
        # Verify all attributes are identical
        assert retrieved_retention is not None
        assert retrieved_retention.id == original_retention.id
        assert retrieved_retention.name == original_retention.name
        assert retrieved_retention.is_system_managed == original_retention.is_system_managed
        assert retrieved_retention.display_rank == original_retention.display_rank

    def test_retention_with_defaults_create_and_retrieve(self, test_session):
        """Test RetentionDTO with default values through database round-trip"""
        # Create with defaults
        original_retention = RetentionTypeDTO()
        test_session.add(original_retention)
        test_session.commit()
        test_session.refresh(original_retention)
        retention_id = original_retention.id

        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_retention = test_session.get(RetentionTypeDTO, retention_id)
        
        # Verify defaults persisted correctly
        assert retrieved_retention.name == ""          # Default value
        assert retrieved_retention.is_system_managed == False  # Default value
        assert retrieved_retention.display_rank == 0   # Default value

    def test_retention_system_managed_create_and_retrieve(self, test_session):
        """Test RetentionDTO system managed flag through database round-trip"""
        # Create expected system managed retention
        expected_retention = RetentionTypeDTO(
            name="Auto-Cleanup",
            is_system_managed=True,
            display_rank=99
        )
        
        # Create retention from expected values
        original_retention = RetentionTypeDTO(
            name=expected_retention.name,
            is_system_managed=expected_retention.is_system_managed,
            display_rank=expected_retention.display_rank
        )
        test_session.add(original_retention)
        test_session.commit()
        test_session.refresh(original_retention)
        retention_id = original_retention.id
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_retention = test_session.get(RetentionTypeDTO, retention_id)
        
        # Verify against expected values
        assert retrieved_retention.name == expected_retention.name
        assert retrieved_retention.is_system_managed == expected_retention.is_system_managed
        assert retrieved_retention.display_rank == expected_retention.display_rank

    def test_retention_query_by_display_rank(self, test_session):
        """Test querying RetentionDTO by display rank"""
        # Define expected retention policies
        expected_retentions = [
            RetentionTypeDTO(name="7 days", is_system_managed=False, display_rank=1),
            RetentionTypeDTO(name="30 days", is_system_managed=False, display_rank=2),
            RetentionTypeDTO(name="90 days", is_system_managed=False, display_rank=3),
            RetentionTypeDTO(name="Never", is_system_managed=True, display_rank=99)
        ]
        
        # Create retention policies from expected data
        for expected_retention in expected_retentions:
            retention = RetentionTypeDTO(
                name=expected_retention.name,
                is_system_managed=expected_retention.is_system_managed,
                display_rank=expected_retention.display_rank
            )
            test_session.add(retention)
        test_session.commit()
        
        # Query system managed retentions
        system_managed = test_session.exec(
            select(RetentionTypeDTO).where(RetentionTypeDTO.is_system_managed == True)
        ).all()
        expected_system_managed = [r for r in expected_retentions if r.is_system_managed == True]
        assert len(system_managed) == len(expected_system_managed)
        assert system_managed[0].name == expected_system_managed[0].name
        
        # Query by display rank order
        ordered_retentions = test_session.exec(
            select(RetentionTypeDTO).order_by(asc(RetentionTypeDTO.display_rank))
        ).all()
        expected_order = [r.name for r in sorted(expected_retentions, key=lambda x: x.display_rank)]
        actual_order = [r.name for r in ordered_retentions]
        assert actual_order == expected_order


class TestDTODatabaseIntegration:
    """Test DTO database integration and relationships"""

    def test_all_dtos_table_creation(self, test_session):
        """Test that all DTOs can be created as database tables and accessed"""
        # Create one instance of each DTO type
        root_folder = RootFolderDTO(
            path="/test", folder_id=1, owner="test", approvers="test"#, active_cleanup=False
        )
        folder_node = FolderNodeDTO(parent_id=0, name="test", nodetype_id=0)
        folder_type = FolderTypeDTO(name="TestType")
        retention = RetentionTypeDTO(name="TestRetention")
        
        # Add all to session
        test_session.add_all([root_folder, folder_node, folder_type, retention])
        test_session.commit()
        test_session.refresh(root_folder)
        test_session.refresh(folder_node)
        test_session.refresh(folder_type)
        test_session.refresh(retention)
        
        # Verify all have IDs assigned
        assert root_folder.id is not None
        assert folder_node.id is not None
        assert folder_type.id is not None
        assert retention.id is not None
        
        # Verify all can be retrieved
        retrieved_root = test_session.get(RootFolderDTO, root_folder.id)
        retrieved_node = test_session.get(FolderNodeDTO, folder_node.id)
        retrieved_type = test_session.get(FolderTypeDTO, folder_type.id)
        retrieved_retention = test_session.get(RetentionTypeDTO, retention.id)
        
        assert all([
            retrieved_root is not None,
            retrieved_node is not None,
            retrieved_type is not None,
            retrieved_retention is not None
        ])


    def test_dto_field_data_integrity(self, test_session):
        """Test that all field types preserve data correctly through database round-trip"""
        # Define expected complex test data
        expected_root_folder = RootFolderDTO(
            path="/very/long/path/with/special/chars/äöü/测试",
            folder_id=999999,
            owner="Owner with spaces and åæø",
            approvers="A1,B2,C3,D4,E5",
            #active_cleanup=True
        )
        
        expected_folder_node = FolderNodeDTO(
            parent_id=123456,
            name="Test node with special chars äöü",
            nodetype_id=1,
            expiration_date="2025-12-31",
            modified_date="2025-08-11T14:30:00Z"
        )
        
        expected_retention = RetentionTypeDTO(
            name="Very long retention policy name with special characters ñü",
            is_system_managed=True,
            display_rank=2147483647  # Max int value
        )
        
        # Create DTOs from expected data
        root_folder = RootFolderDTO(
            path=expected_root_folder.path,
            folder_id=expected_root_folder.folder_id,
            owner=expected_root_folder.owner,
            approvers=expected_root_folder.approvers,
            #active_cleanup=expected_root_folder.active_cleanup
        )
        
        folder_node = FolderNodeDTO(
            parent_id=expected_folder_node.parent_id,
            name=expected_folder_node.name,
            nodetype_id=expected_folder_node.nodetype_id,
            expiration_date=expected_folder_node.expiration_date,
            modified_date=expected_folder_node.modified_date
        )
        
        retention = RetentionTypeDTO(
            name=expected_retention.name,
            is_system_managed=expected_retention.is_system_managed,
            display_rank=expected_retention.display_rank
        )
        
        test_session.add_all([root_folder, folder_node, retention])
        test_session.commit()
        test_session.refresh(root_folder)
        test_session.refresh(folder_node)
        test_session.refresh(retention)
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        
        retrieved_folder = test_session.get(RootFolderDTO, root_folder.id)
        retrieved_node = test_session.get(FolderNodeDTO, folder_node.id)
        retrieved_retention = test_session.get(RetentionTypeDTO, retention.id)
        
        # Verify all complex data preserved exactly against expected values
        assert retrieved_folder.path == expected_root_folder.path
        assert retrieved_folder.folder_id == expected_root_folder.folder_id
        assert retrieved_folder.owner == expected_root_folder.owner
        assert retrieved_folder.approvers == expected_root_folder.approvers
        #assert retrieved_folder.active_cleanup == expected_root_folder.active_cleanup
        
        assert retrieved_node.parent_id == expected_folder_node.parent_id
        assert retrieved_node.name == expected_folder_node.name
        assert retrieved_node.type_id == expected_folder_node.nodetype_id
        assert retrieved_node.retention_date == expected_folder_node.expiration_date
        assert retrieved_node.modified == expected_folder_node.modified_date
        
        assert retrieved_retention.name == expected_retention.name
        assert retrieved_retention.is_system_managed == expected_retention.is_system_managed
        assert retrieved_retention.display_rank == expected_retention.display_rank
        
