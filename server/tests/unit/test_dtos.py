import pytest
from sqlmodel import Field
from datamodel.DTOs import RootFolderDTO, FolderNodeDTO, NodeAttributesDTO, FolderTypeDTO, RetentionDTO


class TestRootFolderDTO:
    """Test RootFolderDTO model"""

    def test_root_folder_creation(self, sample_root_folder_data):
        """Test creating a RootFolderDTO instance"""
        folder = RootFolderDTO(**sample_root_folder_data)
        
        assert folder.path == "/test/folder"
        assert folder.folder_id == 1
        assert folder.owner == "JD"
        assert folder.approvers == "AB,CD"
        assert folder.active_cleanup is False
        assert folder.id is None  # Should be None until saved to DB

    def test_root_folder_with_id(self, sample_root_folder_data):
        """Test creating a RootFolderDTO with an ID"""
        sample_root_folder_data["id"] = 123
        folder = RootFolderDTO(**sample_root_folder_data)
        
        assert folder.id == 123

    def test_root_folder_required_fields(self):
        """Test RootFolderDTO field requirements - SQLModel doesn't enforce by default"""
        # SQLModel/Pydantic doesn't make all fields required by default
        # This test demonstrates the current behavior
        folder = RootFolderDTO()
        
        # Verify that we can create an instance, but it may have None/default values
        assert folder.id is None
        # In a real application, you'd add Field(...) to make fields required


class TestFolderNodeDTO:
    """Test FolderNodeDTO model"""

    def test_folder_node_creation(self, sample_folder_node_data):
        """Test creating a FolderNodeDTO instance"""
        node = FolderNodeDTO(**sample_folder_node_data)
        
        assert node.parent_id == 0
        assert node.name == "TestFolder"
        assert node.type_id == 1
        assert node.node_attributes == 0
        assert node.id is None

    def test_folder_node_defaults(self):
        """Test FolderNodeDTO default values"""
        node = FolderNodeDTO(node_attributes=5)
        
        assert node.parent_id == 0  # Default
        assert node.name == ""      # Default
        assert node.type_id == 0    # Default
        assert node.node_attributes == 5  # Provided value

    def test_folder_node_with_parent(self, sample_folder_node_data):
        """Test FolderNodeDTO with parent relationship"""
        sample_folder_node_data["parent_id"] = 42
        node = FolderNodeDTO(**sample_folder_node_data)
        
        assert node.parent_id == 42


class TestNodeAttributesDTO:
    """Test NodeAttributesDTO model"""

    def test_node_attributes_creation(self):
        """Test creating a NodeAttributesDTO instance"""
        attrs = NodeAttributesDTO(node_id=1, retention_id=2)
        
        assert attrs.node_id == 1
        assert attrs.retention_id == 2
        assert attrs.retention_date is None
        assert attrs.modified is None

    def test_node_attributes_with_dates(self):
        """Test NodeAttributesDTO with date values"""
        attrs = NodeAttributesDTO(
            node_id=1,
            retention_id=2,
            retention_date="2025-12-31",
            modified="2025-08-11"
        )
        
        assert attrs.retention_date == "2025-12-31"
        assert attrs.modified == "2025-08-11"

    def test_node_attributes_defaults(self):
        """Test NodeAttributesDTO default values"""
        attrs = NodeAttributesDTO(node_id=1)
        
        assert attrs.retention_id == 0  # Default
        assert attrs.retention_date is None  # Default
        assert attrs.modified is None  # Default


class TestFolderTypeDTO:
    """Test FolderTypeDTO model"""

    def test_folder_type_creation(self):
        """Test creating a FolderTypeDTO instance"""
        folder_type = FolderTypeDTO()
        
        assert folder_type.name == "InnerNode"  # Default
        assert folder_type.id is None

    def test_folder_type_custom_name(self):
        """Test FolderTypeDTO with custom name"""
        folder_type = FolderTypeDTO(name="VTSSimulation")
        
        assert folder_type.name == "VTSSimulation"

    def test_folder_type_with_id(self):
        """Test FolderTypeDTO with ID"""
        folder_type = FolderTypeDTO(id=5, name="LeafNode")
        
        assert folder_type.id == 5
        assert folder_type.name == "LeafNode"


class TestRetentionDTO:
    """Test RetentionDTO model"""

    def test_retention_creation(self, sample_retention_data):
        """Test creating a RetentionDTO instance"""
        retention = RetentionDTO(**sample_retention_data)
        
        assert retention.name == "30 days"
        assert retention.is_system_managed == "false"
        assert retention.display_rank == 1
        assert retention.id is None

    def test_retention_defaults(self):
        """Test RetentionDTO default values"""
        retention = RetentionDTO()
        
        assert retention.name == ""  # Default
        assert retention.is_system_managed == ""  # Default
        assert retention.display_rank == 0  # Default

    def test_retention_system_managed(self):
        """Test RetentionDTO with system managed flag"""
        retention = RetentionDTO(
            name="Cleaned",
            is_system_managed="true",
            display_rank=99
        )
        
        assert retention.name == "Cleaned"
        assert retention.is_system_managed == "true"
        assert retention.display_rank == 99


class TestDTOTableProperties:
    """Test that DTOs are properly configured as SQLModel tables"""

    def test_dto_table_configuration(self):
        """Test that all DTOs are configured as tables"""
        # Check that table=True is set for all DTOs
        assert hasattr(RootFolderDTO, '__table__')
        assert hasattr(FolderNodeDTO, '__table__')
        assert hasattr(NodeAttributesDTO, '__table__')
        assert hasattr(FolderTypeDTO, '__table__')
        assert hasattr(RetentionDTO, '__table__')

    def test_dto_primary_keys(self):
        """Test that DTOs have proper primary key configuration"""
        # Create instances to check field properties
        root_folder = RootFolderDTO(path="/test", folder_id=1, owner="test", approvers="test", active_cleanup=False)
        folder_node = FolderNodeDTO(node_attributes=0)
        node_attrs = NodeAttributesDTO(node_id=1)
        folder_type = FolderTypeDTO()
        retention = RetentionDTO()
        
        # Check that id fields exist (primary keys should be automatically added)
        assert hasattr(root_folder, 'id')
        assert hasattr(folder_node, 'id')
        assert hasattr(node_attrs, 'node_id')  # This is the primary key for NodeAttributesDTO
        assert hasattr(folder_type, 'id')
        assert hasattr(retention, 'id')
