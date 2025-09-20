#!/usr/bin/env python3
"""
Test script to verify the SQLAlchemy query logic for ordering results
"""

from typing import NamedTuple
from datetime import date

class FileInfo(NamedTuple):
    filepath: str
    modified: date
    id: int = None
    retention_id: int | None = None

def test_case_ordering_logic():
    """Test the logic behind the case ordering"""
    
    # Sample simulations data
    simulations = [
        FileInfo(filepath="/path/to/simulation1", modified=date(2025, 1, 1)),
        FileInfo(filepath="/PATH/TO/SIMULATION2", modified=date(2025, 1, 2)),  # Different case
        FileInfo(filepath="/path/to/simulation3", modified=date(2025, 1, 3)),
    ]
    
    print("Original simulation order:")
    for i, sim in enumerate(simulations):
        print(f"  {i}: {sim.filepath} -> {sim.filepath.lower()}")
    
    # This mimics the logic in the case statement
    case_mapping = {sim.filepath.lower(): index for index, sim in enumerate(simulations)}
    print(f"\nCase mapping: {case_mapping}")
    
    # Test what happens when we have database results
    db_results = [
        "/path/to/simulation3",  # This would be ordered as index 2
        "/path/to/simulation1",  # This would be ordered as index 0  
        "/PATH/TO/SIMULATION2",  # This would be ordered as index 1
    ]
    
    print("\nSimulated database results with ordering:")
    for path in db_results:
        order_index = case_mapping.get(path.lower(), len(simulations))
        print(f"  {path} -> order index: {order_index}")
    
    # Sort the results as the SQL would
    sorted_results = sorted(db_results, key=lambda path: case_mapping.get(path.lower(), len(simulations)))
    print(f"\nSorted results: {sorted_results}")
    
    # Verify they match the original simulation order
    expected_order = [sim.filepath for sim in simulations]
    print(f"Expected order: {expected_order}")
    
    # Check if the logic would work
    print(f"\nOrdering would work correctly: {[path.lower() for path in sorted_results] == [sim.filepath.lower() for sim in simulations]}")

def test_import_requirements():
    """Check what imports are needed"""
    print("\nRequired imports for the query:")
    print("from sqlalchemy import case  # Missing in current code!")
    
    # Show the corrected query structure
    print("""
Corrected query structure:
from sqlalchemy import case

query = (
    select(FolderNodeDTO)
    .where(
        (FolderNodeDTO.rootfolder_id == rootfolder.id) &
        (FolderNodeDTO.path.in_([sim.filepath.lower() for sim in simulations]))
    )
    .order_by(
        case(
            {sim.filepath.lower(): index for index, sim in enumerate(simulations)},
            value=FolderNodeDTO.path,
            else_=len(simulations)
        )
    )
)

# Execute the query
existing_folders = session.exec(query).all()
""")

if __name__ == "__main__":
    test_case_ordering_logic()
    test_import_requirements()