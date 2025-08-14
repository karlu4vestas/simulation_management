#!/usr/bin/env python3
"""
Demo script showing how to use the Database.is_empty() function
"""

from datamodel.db import Database
from datamodel.dtos import RootFolderDTO
from sqlmodel import Session


def demonstrate_is_empty():
    """Demonstrate the is_empty function with different database states"""
    
    print("=== Database is_empty() Function Demo ===\n")
    
    # Get database instance
    db = Database.get_db()
    
    # Check if database is empty before creating tables
    print("1. Checking database before creating tables:")
    print(f"   Database is empty: {db.is_empty()}")
    
    # Create tables
    print("\n2. Creating database tables...")
    db.create_db_and_tables()
    
    # Check if database is empty with empty tables
    print("\n3. Checking database with empty tables:")
    print(f"   Database is empty: {db.is_empty()}")
    
    # Add some data
    print("\n4. Adding data to the database...")
    with Session(db.get_engine()) as session:
        root_folder = RootFolderDTO(
            path="/example/simulation/data",
            owner="demo_user",
            approvers="approver1,approver2",
            active_cleanup=False
        )
        session.add(root_folder)
        session.commit()
        print(f"   Added RootFolderDTO with path: {root_folder.path}")
    
    # Check if database is empty with data
    print("\n5. Checking database after adding data:")
    print(f"   Database is empty: {db.is_empty()}")
    
    print("\n=== Demo Complete ===")


if __name__ == "__main__":
    demonstrate_is_empty()
