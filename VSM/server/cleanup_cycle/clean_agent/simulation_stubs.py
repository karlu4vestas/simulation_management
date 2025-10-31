"""
Stub classes for BaseSimulation and Simulation.
These are placeholders to avoid pulling in dependencies during initial implementation.
"""
import os
from datetime import date
from datamodel.dtos import ExternalRetentionTypes, FolderTypeEnum, FileInfo


class BaseSimulation:
    """
    Stub - represents simulation base interface.
    This is a placeholder to avoid dependencies on the full implementation.
    """
    
    def __init__(self, path: str):
        """
        Initialize base simulation.
        
        Args:
            path: Simulation folder path
        """
        self.path = path


class Simulation(BaseSimulation):
    """
    Stub - represents a VTS simulation to be cleaned.
    This is a placeholder to avoid dependencies on the full implementation.
    
    In the real implementation, this class would:
    - Scan the simulation folder structure
    - Identify files to clean based on retention rules
    - Determine if simulation has issues
    - Check if simulation can be cleaned
    """
    
    def __init__(self, path: str, modified_date: date = None):
        """
        Initialize simulation.
        
        Args:
            path: Simulation folder path
            modified_date: Last modification date (from database or filesystem)
        """
        super().__init__(path)
        
        # Stub properties - would be computed in real implementation
        self._modified_date = modified_date if modified_date else date.today()
        self._external_retention = ExternalRetentionTypes.UNDEFINED
        self._files_to_clean = []
    
    @property
    def modified_date(self) -> date:
        """
        Get simulation modification date.
        
        Returns:
            Date of last modification
        """
        return self._modified_date
    
    @property
    def external_retention(self) -> ExternalRetentionTypes:
        """
        Get simulation retention status.
        
        Returns:
            One of: UNDEFINED, Clean, Issue, Missing
        """
        return self._external_retention
    
    @property
    def was_cleaned(self) -> bool:
        """
        Check if simulation was cleaned.
        
        Returns:
            True if simulation was cleaned
        """
        return self._external_retention == ExternalRetentionTypes.CLEAN
    
    @property
    def has_issue(self) -> bool:
        """
        Check if simulation has issues.
        
        Returns:
            True if simulation has issues
        """
        return self._external_retention == ExternalRetentionTypes.ISSUE
    
    @property
    def was_skipped(self) -> bool:
        """
        Check if simulation was skipped.
        
        Returns:
            True if simulation was skipped (UNDEFINED status)
        """
        return self._external_retention == ExternalRetentionTypes.UNDEFINED
    
    def get_files_to_clean(self) -> list[str]:
        """
        Get list of file paths to delete.
        
        In the real implementation, this would:
        - Identify cleanable output files based on retention rules
        - Filter files based on .set file names
        - Apply cleaner strategies (clean_all_pr_ext, clean_all_but_one_pr_ext)
        
        Returns:
            List of absolute file paths to delete
        """
        # For stub: Return empty list if path doesn't exist
        # This prevents errors during testing with non-existent paths
        if not os.path.exists(self.path):
            return []
        return self._files_to_clean
    
    def GetFileInfo(self) -> FileInfo:
        """
        Get lightweight FileInfo object for queue processing.
        
        This method extracts only the essential information needed for
        processing results, avoiding memory issues from keeping the entire
        Simulation object (with its potentially large file structure) in memory.
        
        Returns:
            FileInfo object with path, modified_date, nodetype, and external_retention
        """
        return FileInfo(
            filepath=self.path,
            modified_date=self.modified_date,
            nodetype=FolderTypeEnum.VTS_SIMULATION,
            external_retention=self.external_retention
        )
