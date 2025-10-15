import pytest
from sqlmodel import Field, Session, select, asc
from datamodel.dtos import (
    FolderTypeEnum, 
    RootFolderDTO, 
    FolderNodeDTO, 
    FolderTypeDTO, 
    RetentionTypeDTO, 
    SimulationDomainDTO
)
from datetime import date


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
            simulationdomain_id=sample_root_folder_data["simulationdomain_id"],
            path="/updated/path",
            folder_id=sample_root_folder_data["folder_id"],  # Unchanged
            owner="UpdatedOwner",
            approvers=sample_root_folder_data["approvers"]  # Unchanged
        )
        
        # Update the folder
        folder.path = expected_folder.path
        folder.owner = expected_folder.owner
        test_session.commit()
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_folder = test_session.get(RootFolderDTO, folder_id)
        
        # Verify updated values
        assert retrieved_folder.path == expected_folder.path
        assert retrieved_folder.owner == expected_folder.owner
        assert retrieved_folder.approvers == expected_folder.approvers  # Unchanged

    def test_root_folder_query_by_attributes(self, test_session):
        """Test querying RootFolderDTO by attributes"""
        # Create simulation domain first
        domain = SimulationDomainDTO(name="TestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        # Create multiple folders with different owners
        folder1 = RootFolderDTO(
            simulationdomain_id=domain.id,
            folder_id=1,
            path="/test/folder1",
            owner="Owner1",
            approvers="Approver1"
        )
        folder2 = RootFolderDTO(
            simulationdomain_id=domain.id,
            folder_id=2,
            path="/test/folder2", 
            owner="Owner2",
            approvers="Approver2"
        )
        test_session.add_all([folder1, folder2])
        test_session.commit()
        
        # Query by owner
        owner1_folders = test_session.exec(
            select(RootFolderDTO).where(RootFolderDTO.owner == "Owner1")
        ).all()
        
        assert len(owner1_folders) == 1
        assert owner1_folders[0].folder_id == 1

    def test_create_and_retrieve_root_folder(self, test_session, sample_root_folder_data):
        """Test creating and retrieving a RootFolderDTO from database"""

        # Create and save
        folder = RootFolderDTO(**sample_root_folder_data)
        test_session.add(folder)
        test_session.commit()
        test_session.refresh(folder)
            
        # Verify it was saved with an ID
        assert folder.id is not None
        saved_id = folder.id
            
        # Retrieve and verify
        statement = select(RootFolderDTO).where(RootFolderDTO.id == saved_id)
        retrieved_folder = test_session.exec(statement).first()
            
        assert retrieved_folder is not None
        assert retrieved_folder.path == "/test/folder"
        assert retrieved_folder.owner == "JD"

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
        assert retrieved_node.nodetype_id == original_node.nodetype_id

    def test_folder_node_with_defaults_create_and_retrieve(self, test_session, test_root_folder):
        """Test creating FolderNodeDTO with minimal data using defaults"""
        # Create simulation domain and folder type
        domain = SimulationDomainDTO(name="TestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        folder_type = FolderTypeDTO(simulationdomain_id=domain.id, name="innernode")
        test_session.add(folder_type)
        test_session.commit()
        test_session.refresh(folder_type)
        
        # Create with minimal required data
        original_node = FolderNodeDTO(
            rootfolder_id=test_root_folder.id,
            name="MinimalNode",
            nodetype_id=folder_type.id
        )
        test_session.add(original_node)
        test_session.commit()
        test_session.refresh(original_node)
        
        # Verify ID was assigned
        assert original_node.id is not None
        node_id = original_node.id
        
        # Retrieve from database
        retrieved_node = test_session.get(FolderNodeDTO, node_id)
        
        # Verify attributes
        assert retrieved_node is not None
        assert retrieved_node.name == "MinimalNode"
        assert retrieved_node.parent_id == 0          # Default value
        assert retrieved_node.path == ""              # Default value
        assert retrieved_node.path_ids == ""          # Default value
        assert retrieved_node.modified_date is None   # Default value
        assert retrieved_node.expiration_date is None # Default value
        assert retrieved_node.retention_id is None    # Default value

    def test_folder_node_with_attributes_create_and_retrieve(self, test_session, test_root_folder):
        """Test creating and retrieving a FolderNodeDTO with attributes"""
        # Create simulation domain and folder type
        domain = SimulationDomainDTO(name="TestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        folder_type = FolderTypeDTO(simulationdomain_id=domain.id, name="innernode")
        test_session.add(folder_type)
        test_session.commit()
        test_session.refresh(folder_type)
        
        # Create and save with proper date objects
        original_node = FolderNodeDTO(
            rootfolder_id=test_root_folder.id,
            parent_id=1,
            name="TestNode",
            nodetype_id=folder_type.id,
            modified_date=date(2025, 8, 12),
            expiration_date=date(2025, 12, 31),
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
        assert retrieved_node.nodetype_id == original_node.nodetype_id
        assert retrieved_node.modified_date == original_node.modified_date
        assert retrieved_node.expiration_date == original_node.expiration_date
        assert retrieved_node.retention_id == original_node.retention_id

    def test_folder_node_attributes_update_and_retrieve(self, test_session, test_root_folder):
        """Test updating and retrieving FolderNodeDTO attributes"""
        # Create simulation domain and folder type
        domain = SimulationDomainDTO(name="TestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        folder_type = FolderTypeDTO(simulationdomain_id=domain.id, name="innernode")
        test_session.add(folder_type)
        test_session.commit()
        test_session.refresh(folder_type)
        
        # Create and save initial node
        initial_node = FolderNodeDTO(
            rootfolder_id=test_root_folder.id,
            parent_id=1,
            name="UpdateTest",
            nodetype_id=folder_type.id
        )
        test_session.add(initial_node)
        test_session.commit()
        test_session.refresh(initial_node)
        node_id = initial_node.id
        
        # Update attributes with proper date objects
        initial_node.name = "UpdatedName"
        initial_node.modified_date = date(2025, 9, 15)
        initial_node.expiration_date = date(2026, 1, 15)
        test_session.commit()
        
        # Store the folder_type_id before expunging
        expected_folder_type_id = folder_type.id
        
        # Clear session and retrieve fresh
        test_session.expunge_all()
        retrieved_node = test_session.get(FolderNodeDTO, node_id)
        
        # Verify updated values
        assert retrieved_node.name == "UpdatedName"
        assert retrieved_node.modified_date == date(2025, 9, 15)
        assert retrieved_node.expiration_date == date(2026, 1, 15)
        assert retrieved_node.nodetype_id == expected_folder_type_id

    def test_folder_node_hierarchy_creation(self, test_session, test_root_folder):
        """Test creating folder node hierarchies"""
        # Create simulation domain and folder type
        domain = SimulationDomainDTO(name="TestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        folder_type = FolderTypeDTO(simulationdomain_id=domain.id, name="innernode")
        test_session.add(folder_type)
        test_session.commit()
        test_session.refresh(folder_type)
        
        # Create parent folder
        expected_parent = FolderNodeDTO(
            rootfolder_id=test_root_folder.id,
            parent_id=0,
            name="ParentFolder",
            nodetype_id=folder_type.id
        )
        test_session.add(expected_parent)
        test_session.commit()
        test_session.refresh(expected_parent)
        parent_id = expected_parent.id
        
        # Create child folder
        expected_child = FolderNodeDTO(
            rootfolder_id=test_root_folder.id,
            parent_id=parent_id,
            name="ChildFolder",
            nodetype_id=folder_type.id
        )
        test_session.add(expected_child)
        test_session.commit()
        test_session.refresh(expected_child)
        child_id = expected_child.id
        
        # Store expected values before expunging
        expected_parent_name = expected_parent.name
        expected_parent_parent_id = expected_parent.parent_id
        expected_parent_nodetype_id = expected_parent.nodetype_id
        expected_child_name = expected_child.name
        expected_child_nodetype_id = expected_child.nodetype_id
        
        # Clear session and retrieve both
        test_session.expunge_all()
        retrieved_parent = test_session.get(FolderNodeDTO, parent_id)
        retrieved_child = test_session.get(FolderNodeDTO, child_id)
        
        # Verify parent attributes match expected
        assert retrieved_parent.name == expected_parent_name
        assert retrieved_parent.parent_id == expected_parent_parent_id
        assert retrieved_parent.nodetype_id == expected_parent_nodetype_id
        
        # Verify child attributes match expected
        assert retrieved_child.name == expected_child_name
        assert retrieved_child.parent_id == parent_id  # Use stored parent ID
        assert retrieved_child.nodetype_id == expected_child_nodetype_id
        
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
        # Create simulation domain first
        domain = SimulationDomainDTO(name="TestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        # Create and save
        original_type = FolderTypeDTO(simulationdomain_id=domain.id, name="VTSSimulation")
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
        assert retrieved_type.simulationdomain_id == original_type.simulationdomain_id

    def test_folder_type_with_defaults_create_and_retrieve(self, test_session):
        """Test creating FolderTypeDTO with default values"""
        # Create simulation domain first
        domain = SimulationDomainDTO(name="TestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        # Create with minimal data
        original_type = FolderTypeDTO(simulationdomain_id=domain.id)
        test_session.add(original_type)
        test_session.commit()
        test_session.refresh(original_type)
        
        # Verify ID was assigned
        assert original_type.id is not None
        type_id = original_type.id
        
        # Retrieve from database
        retrieved_type = test_session.get(FolderTypeDTO, type_id)
        
        # Verify defaults applied
        assert retrieved_type is not None
        assert retrieved_type.name == ""  # Default value

    def test_folder_type_query_by_name(self, test_session):
        """Test querying FolderTypeDTO by name"""
        # Create simulation domain first
        domain = SimulationDomainDTO(name="TestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        # Create multiple types
        type1 = FolderTypeDTO(simulationdomain_id=domain.id, name="Type1")
        type2 = FolderTypeDTO(simulationdomain_id=domain.id, name="Type2")
        test_session.add_all([type1, type2])
        test_session.commit()
        
        # Query by name
        type1_results = test_session.exec(
            select(FolderTypeDTO).where(FolderTypeDTO.name == "Type1")
        ).all()
        
        assert len(type1_results) == 1
        assert type1_results[0].name == "Type1"


class TestRetentionDTO:
    """Test RetentionTypeDTO database operations"""

    def test_retention_create_and_retrieve(self, test_session, sample_retention_data):
        """Test creating, saving, and retrieving a RetentionTypeDTO"""
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
        assert retrieved_retention.is_endstage == original_retention.is_endstage
        assert retrieved_retention.display_rank == original_retention.display_rank

    def test_retention_with_defaults_create_and_retrieve(self, test_session):
        """Test creating RetentionTypeDTO with default values"""
        # Create simulation domain first
        domain = SimulationDomainDTO(name="TestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        # Create with minimal data
        original_retention = RetentionTypeDTO(simulationdomain_id=domain.id)
        test_session.add(original_retention)
        test_session.commit()
        test_session.refresh(original_retention)
        
        # Verify defaults
        assert original_retention.name == ""          # Default value
        assert original_retention.is_endstage == False  # Default value
        assert original_retention.display_rank == 0   # Default value

    def test_retention_system_managed_create_and_retrieve(self, test_session):
        """Test creating system-managed RetentionTypeDTO"""
        # Create simulation domain first
        domain = SimulationDomainDTO(name="TestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        # Create system-managed retention
        original_retention = RetentionTypeDTO(
            simulationdomain_id=domain.id,
            name="System Retention",
            is_endstage=True,
            days_to_cleanup=90,
            display_rank=10
        )
        test_session.add(original_retention)
        test_session.commit()
        test_session.refresh(original_retention)
        
        # Verify attributes
        assert original_retention.is_endstage == True
        assert original_retention.days_to_cleanup == 90

    def test_retention_query_by_display_rank(self, test_session):
        """Test querying RetentionTypeDTO by display_rank"""
        # Create simulation domain first
        domain = SimulationDomainDTO(name="TestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        # Create multiple retentions with different ranks
        retention1 = RetentionTypeDTO(simulationdomain_id=domain.id, name="Rank1", display_rank=1)
        retention2 = RetentionTypeDTO(simulationdomain_id=domain.id, name="Rank2", display_rank=2)
        test_session.add_all([retention1, retention2])
        test_session.commit()
        
        # Query ordered by display_rank
        all_retentions = test_session.exec(
            select(RetentionTypeDTO).order_by(asc(RetentionTypeDTO.display_rank))
        ).all()
        
        assert len(all_retentions) >= 2
        assert all_retentions[0].display_rank <= all_retentions[1].display_rank


class TestDTODatabaseIntegration:
    """Test integration scenarios across multiple DTOs"""

    def test_all_dtos_table_creation(self, test_session):
        """Test that all DTO table schemas work together"""
        # Create simulation domain first
        domain = SimulationDomainDTO(name="IntegrationTestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        # Create folder type
        folder_type = FolderTypeDTO(simulationdomain_id=domain.id, name="TestType")
        test_session.add(folder_type)
        test_session.commit()
        test_session.refresh(folder_type)
        
        # Create root folder
        root_folder = RootFolderDTO(
            simulationdomain_id=domain.id,
            folder_id=1,
            path="/integration/test",
            owner="IntegrationTester",
            approvers="IntegrationApprover"
        )
        test_session.add(root_folder)
        test_session.commit()
        test_session.refresh(root_folder)
        
        # Create folder node with proper references
        folder_node = FolderNodeDTO(
            rootfolder_id=root_folder.id,
            parent_id=0,
            name="test",
            nodetype_id=folder_type.id
        )
        test_session.add(folder_node)
        test_session.commit()
        test_session.refresh(folder_node)
        
        # Verify all entities were created successfully
        assert domain.id is not None
        assert folder_type.id is not None
        assert root_folder.id is not None
        assert folder_node.id is not None

    def test_dto_field_data_integrity(self, test_session, test_root_folder):
        """Test that all field types preserve data correctly through database round-trip"""
        # Create simulation domain first
        domain = SimulationDomainDTO(name="TestDomain")
        test_session.add(domain)
        test_session.commit()
        test_session.refresh(domain)
        
        # Create folder type
        folder_type = FolderTypeDTO(simulationdomain_id=domain.id, name="innernode")
        test_session.add(folder_type)
        test_session.commit()
        test_session.refresh(folder_type)
        
        # Define expected complex test data
        expected_root_folder = test_root_folder  # Use the fixture

        expected_folder_node = FolderNodeDTO(
            rootfolder_id=test_root_folder.id,
            parent_id=123456,
            name="Test node with special chars äöü",
            nodetype_id=folder_type.id,
            expiration_date=date(2025, 12, 31),
            modified_date=date(2025, 8, 11)
        )

        expected_retention = RetentionTypeDTO(
            simulationdomain_id=domain.id,
            name="Very long retention policy name with special characters ñü",
            is_endstage=True,
            display_rank=2147483647  # Max int value
        )

        # Create DTOs from expected data
        root_folder = expected_root_folder  # Use the fixture

        folder_node = FolderNodeDTO(
            rootfolder_id=test_root_folder.id,
            parent_id=expected_folder_node.parent_id,
            name=expected_folder_node.name,
            nodetype_id=expected_folder_node.nodetype_id,
            expiration_date=expected_folder_node.expiration_date,
            modified_date=expected_folder_node.modified_date
        )

        retention = RetentionTypeDTO(
            simulationdomain_id=expected_retention.simulationdomain_id,
            name=expected_retention.name,
            is_endstage=expected_retention.is_endstage,
            display_rank=expected_retention.display_rank
        )

        test_session.add_all([folder_node, retention])
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

        assert retrieved_node.parent_id == expected_folder_node.parent_id
        assert retrieved_node.name == expected_folder_node.name
        assert retrieved_node.nodetype_id == expected_folder_node.nodetype_id
        assert retrieved_node.expiration_date == expected_folder_node.expiration_date
        assert retrieved_node.modified_date == expected_folder_node.modified_date

        assert retrieved_retention.name == expected_retention.name
        assert retrieved_retention.is_endstage == expected_retention.is_endstage
        assert retrieved_retention.display_rank == expected_retention.display_rank