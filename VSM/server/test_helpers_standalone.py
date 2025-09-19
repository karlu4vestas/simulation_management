"""
Test script for hierarchical insert helper functions (standalone)
"""

def normalize_and_split_path(filepath: str) -> list[str]:
    """
    Normalize path to forward slashes and split into segments.
    Handles edge cases like empty paths, root paths, trailing slashes.
    """
    if not filepath or filepath.strip() == "":
        return []
    
    # Normalize to forward slashes
    normalized = filepath.replace("\\", "/")
    
    # Remove leading and trailing slashes, then split
    segments = [segment for segment in normalized.strip("/").split("/") if segment.strip()]
    
    return segments


def generate_path_ids(parent_path_ids: str, node_id: int) -> str:
    """
    Generate path_ids for a new node based on parent information.
    Format: parent_ids + "/" + node_id (e.g., "0/1/2")
    For root nodes (parent_id=0), just return the node_id as string.
    """
    if not parent_path_ids or parent_path_ids == "0":
        return str(node_id)
    else:
        return f"{parent_path_ids}/{node_id}"


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
    print("Running hierarchical insert helper tests...\n")
    
    try:
        test_normalize_and_split_path()
        test_generate_path_ids()
        print("\n✅ All basic tests passed!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()