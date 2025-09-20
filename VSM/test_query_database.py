#!/usr/bin/env python3
"""
Comprehensive test for the SQLAlchemy case ordering query
"""

import sys
import os
sys.path.append('/workspaces/simulation_management/VSM/server')

from typing import NamedTuple
from datetime import date
from sqlalchemy import create_engine, case, select
from sqlmodel import SQLModel, Session, Field

# Mock DTOs for testing
class MockFolderNodeDTO(SQLModel, table=True):
    __tablename__ = "mock_folder_node"
    
    id: int | None = Field(default=None, primary_key=True)
    rootfolder_id: int = Field(default=1)
    path: str = Field(default="")
    retention_id: int | None = None

class FileInfo(NamedTuple):
    filepath: str
    modified: date
    id: int = None
    retention_id: int | None = None

def test_case_ordering_with_database():
    """Test the actual SQLAlchemy case ordering with an in-memory database"""
    
    # Create in-memory SQLite database
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    
    with Session(engine) as session:
        # Insert test data in random order
        test_folders = [
            MockFolderNodeDTO(id=1, rootfolder_id=1, path="/path/to/simulation3"),
            MockFolderNodeDTO(id=2, rootfolder_id=1, path="/path/to/simulation1"), 
            MockFolderNodeDTO(id=3, rootfolder_id=1, path="/path/to/simulation2"),
            MockFolderNodeDTO(id=4, rootfolder_id=2, path="/other/path"),  # Different rootfolder
        ]
        
        for folder in test_folders:
            session.add(folder)
        session.commit()
        
        # Define simulations in desired order
        simulations = [
            FileInfo(filepath="/path/to/simulation1", modified=date(2025, 1, 1)),
            FileInfo(filepath="/path/to/simulation2", modified=date(2025, 1, 2)),
            FileInfo(filepath="/path/to/simulation3", modified=date(2025, 1, 3)),
        ]
        
        print("Simulations in desired order:")
        for i, sim in enumerate(simulations):
            print(f"  {i}: {sim.filepath}")
        
        # Test the query from the code
        rootfolder_id = 1
        
        query = (
            select(MockFolderNodeDTO)
            .where(
                (MockFolderNodeDTO.rootfolder_id == rootfolder_id) &
                (MockFolderNodeDTO.path.in_([sim.filepath.lower() for sim in simulations]))
            )
            .order_by(
                case(
                    {sim.filepath.lower(): index for index, sim in enumerate(simulations)},
                    value=MockFolderNodeDTO.path,
                    else_=len(simulations)
                )
            )
        )
        
        # Execute query
        result_folders = session.exec(query).all()
        
        print(f"\nQuery results (should be in same order as simulations):")
        for i, folder in enumerate(result_folders):
            if hasattr(folder, 'path'):
                print(f"  {i}: path={folder.path} id={folder.id}")
            else:
                # If it's a tuple/row, access by index or key
                print(f"  {i}: result_type={type(folder)} len={len(folder) if hasattr(folder, '__len__') else 'N/A'}")
                if hasattr(folder, '__getitem__'):
                    try:
                        print(f"       item[0]={folder[0] if len(folder) > 0 else 'N/A'}")
                        if len(folder) > 1:
                            print(f"       item[1]={folder[1]}")
                    except:
                        pass
        
        # Let's debug what we actually get
        if result_folders:
            first_result = result_folders[0]
            print(f"First result type: {type(first_result)}")
            print(f"First result content: {first_result}")
        
        # For now, let's assume the results are the MockFolderNodeDTO objects
        # Try different access methods
        actual_paths = []
        for folder in result_folders:
            try:
                if hasattr(folder, 'path'):
                    actual_paths.append(folder.path)
                elif hasattr(folder, '__getitem__'):
                    actual_paths.append(str(folder[0]))  # Assume path is first column
                else:
                    actual_paths.append('UNKNOWN')
            except:
                actual_paths.append('ERROR')
        
        # Verify ordering
        expected_paths = [sim.filepath for sim in simulations]
        print(f"\nExpected order: {expected_paths}")
        print(f"Actual order:   {actual_paths}")
        print(f"Ordering correct: {expected_paths == actual_paths}")
        
        # Test case sensitivity
        simulations_mixed_case = [
            FileInfo(filepath="/PATH/TO/SIMULATION1", modified=date(2025, 1, 1)),
            FileInfo(filepath="/path/to/simulation2", modified=date(2025, 1, 2)),
            FileInfo(filepath="/Path/To/Simulation3", modified=date(2025, 1, 3)),
        ]
        
        print(f"\n--- Testing Case Sensitivity ---")
        print("Mixed case simulations:")
        for i, sim in enumerate(simulations_mixed_case):
            print(f"  {i}: {sim.filepath} -> {sim.filepath.lower()}")
        
        query_mixed = (
            select(MockFolderNodeDTO)
            .where(
                (MockFolderNodeDTO.rootfolder_id == rootfolder_id) &
                (MockFolderNodeDTO.path.in_([sim.filepath.lower() for sim in simulations_mixed_case]))
            )
            .order_by(
                case(
                    {sim.filepath.lower(): index for index, sim in enumerate(simulations_mixed_case)},
                    value=MockFolderNodeDTO.path,
                    else_=len(simulations_mixed_case)
                )
            )
        )
        
        result_mixed = session.exec(query_mixed).all()
        print(f"Results with mixed case input:")
        for i, folder in enumerate(result_mixed):
            print(f"  {i}: {folder.path}")
            
        # Check if ordering is still correct (should match lowercase versions)
        expected_lower = [sim.filepath.lower() for sim in simulations_mixed_case]
        actual_lower = [folder.path.lower() for folder in result_mixed]
        print(f"Case-insensitive ordering correct: {expected_lower == actual_lower}")

def test_edge_cases():
    """Test edge cases that might cause issues"""
    
    print(f"\n--- Testing Edge Cases ---")
    
    # Empty simulations list
    simulations_empty = []
    case_mapping_empty = {sim.filepath.lower(): index for index, sim in enumerate(simulations_empty)}
    print(f"Empty simulations case mapping: {case_mapping_empty}")
    
    # Duplicate paths (should not happen in real usage but test anyway)
    simulations_dup = [
        FileInfo(filepath="/same/path", modified=date(2025, 1, 1)),
        FileInfo(filepath="/same/path", modified=date(2025, 1, 2)),  # Duplicate
    ]
    case_mapping_dup = {sim.filepath.lower(): index for index, sim in enumerate(simulations_dup)}
    print(f"Duplicate paths case mapping: {case_mapping_dup}")
    print(f"Note: Duplicates will overwrite previous mapping - last wins")
    
    # Very long path
    long_path = "/very/long/path/" + "segment/" * 50 + "file"
    simulations_long = [FileInfo(filepath=long_path, modified=date(2025, 1, 1))]
    case_mapping_long = {sim.filepath.lower(): index for index, sim in enumerate(simulations_long)}
    print(f"Long path test passed: {len(case_mapping_long) == 1}")

if __name__ == "__main__":
    print("Testing SQLAlchemy case ordering query...")
    test_case_ordering_with_database()
    test_edge_cases()
    print("\nAll tests completed!")