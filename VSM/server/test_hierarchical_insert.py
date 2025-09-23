"""
Test script for hierarchical insert functionality
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date
from app.web_server_retention_api import (
    normalize_and_split_path, 
    find_existing_node, 
    generate_path_ids,
    insert_hierarchy_for_one_filepath,
    insert_simulations_in_db,
    FileInfo
)

def test_normalize_and_split_path():
    """Test path normalization and splitting"""
    print("Testing normalize_and_split_path...")
    
    test_cases = [
        ("/root/child/grandchild", ["root", "child", "grandchild"]),
        ("\\root\\child\\", ["root", "child"]),
        ("root/child/", ["root", "child"]),
        ("", []),
        ("   ", []),
        ("single", ["single"]),
        ("//double//slash//", ["double", "slash"])
    ]
    
    for input_path, expected in test_cases:
        result = normalize_and_split_path(input_path)
        assert result == expected, f"Expected {expected}, got {result} for input '{input_path}'"
        print(f"✓ '{input_path}' -> {result}")

def test_generate_path_ids():
    """Test path_ids generation"""
    print("\nTesting generate_path_ids...")
    
    test_cases = [
        ("0", 1, "1"),
        ("1", 2, "1/2"),
        ("1/2", 3, "1/2/3"),
        ("", 1, "1"),
        (None, 1, "1")
    ]
    
    for parent_path_ids, node_id, expected in test_cases:
        result = generate_path_ids(parent_path_ids, node_id)
        assert result == expected, f"Expected {expected}, got {result}"
        print(f"✓ parent_path_ids='{parent_path_ids}', node_id={node_id} -> '{result}'")

if __name__ == "__main__":
    print("Running hierarchical insert tests...\n")
    
    try:
        test_normalize_and_split_path()
        test_generate_path_ids()
        print("\n✅ All basic tests passed!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)