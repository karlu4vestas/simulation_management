"""
Example test script showing how the hierarchical insert would work
This demonstrates the expected behavior of the insert_simulations_in_db function
"""

from datetime import date
from typing import NamedTuple

class FileInfo(NamedTuple):
    filepath: str
    modified: date
    id: int = None   # will be used during updates
    retention_id: int | None = None

def demo_hierarchical_insert():
    """Demonstrate how the hierarchical insert would work"""
    
    print("Demo: Hierarchical Insert Functionality")
    print("=" * 50)
    
    # Example simulation data with various path structures
    simulations = [
        FileInfo(filepath="/VTS/Project_A/Run_001", modified=date(2024, 1, 15)),
        FileInfo(filepath="/VTS/Project_A/Run_002", modified=date(2024, 1, 16)),
        FileInfo(filepath="/VTS/Project_B/Scenario_1/Run_001", modified=date(2024, 1, 17)),
        FileInfo(filepath="\\Mixed\\Path\\Types", modified=date(2024, 1, 18)),
        FileInfo(filepath="Single_Level", modified=date(2024, 1, 19)),
        FileInfo(filepath="/VTS/Project_A/Run_003", modified=date(2024, 1, 20)),  # Same parent as Run_001, Run_002
    ]
    
    print(f"Processing {len(simulations)} simulation paths:")
    for i, sim in enumerate(simulations, 1):
        print(f"  {i}. {sim.filepath}")
    
    print("\nExpected hierarchy to be created:")
    print("=" * 40)
    
    # Simulate what the hierarchical insert would create
    expected_hierarchy = {
        "VTS": {
            "path": "VTS",
            "path_ids": "1",  # First node created
            "parent_id": 0,
            "children": {
                "Project_A": {
                    "path": "VTS/Project_A", 
                    "path_ids": "1/2",
                    "parent_id": 1,
                    "children": {
                        "Run_001": {"path": "VTS/Project_A/Run_001", "path_ids": "1/2/3", "parent_id": 2},
                        "Run_002": {"path": "VTS/Project_A/Run_002", "path_ids": "1/2/4", "parent_id": 2},
                        "Run_003": {"path": "VTS/Project_A/Run_003", "path_ids": "1/2/5", "parent_id": 2},
                    }
                },
                "Project_B": {
                    "path": "VTS/Project_B",
                    "path_ids": "1/6", 
                    "parent_id": 1,
                    "children": {
                        "Scenario_1": {
                            "path": "VTS/Project_B/Scenario_1",
                            "path_ids": "1/6/7",
                            "parent_id": 6,
                            "children": {
                                "Run_001": {"path": "VTS/Project_B/Scenario_1/Run_001", "path_ids": "1/6/7/8", "parent_id": 7}
                            }
                        }
                    }
                }
            }
        },
        "Mixed": {
            "path": "Mixed",
            "path_ids": "9",
            "parent_id": 0,
            "children": {
                "Path": {
                    "path": "Mixed/Path",
                    "path_ids": "9/10",
                    "parent_id": 9,
                    "children": {
                        "Types": {"path": "Mixed/Path/Types", "path_ids": "9/10/11", "parent_id": 10}
                    }
                }
            }
        },
        "Single_Level": {
            "path": "Single_Level",
            "path_ids": "12",
            "parent_id": 0
        }
    }
    
    def print_hierarchy(node, level=0):
        indent = "  " * level
        if isinstance(node, dict):
            for name, details in node.items():
                if isinstance(details, dict):
                    if "path" in details:
                        print(f"{indent}📁 {name}")
                        print(f"{indent}   path: {details['path']}")
                        print(f"{indent}   path_ids: {details['path_ids']}")
                        print(f"{indent}   parent_id: {details['parent_id']}")
                        if "children" in details:
                            print_hierarchy(details["children"], level + 1)
                    else:
                        print(f"{indent}📁 {name}")
                        print_hierarchy(details, level + 1)
    
    print_hierarchy(expected_hierarchy)
    
    print("\nKey Features Demonstrated:")
    print("=" * 30)
    print("✓ Case-insensitive path handling")
    print("✓ Mixed path separators (/ and \\) normalized to /")
    print("✓ Hierarchical parent-child relationships")
    print("✓ Path_ids generation (parent_ids/child_id format)")
    print("✓ Efficient reuse of existing nodes")
    print("✓ Support for various path depths")
    
    print("\nImplementation Notes:")
    print("=" * 20)
    print("• Root nodes have parent_id = 0")
    print("• Path_ids follow format: parent_path_ids/node_id")  
    print("• Full path is materialized in the 'path' field")
    print("• Only hierarchy is created, attributes assigned later")
    print("• Database transactions ensure consistency")

if __name__ == "__main__":
    demo_hierarchical_insert()