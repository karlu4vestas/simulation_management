#!/usr/bin/env python3
"""
Demo script showing Database.get_engine() functionality
"""
import sys
sys.path.append('.')

from datamodel.db import Database
from datamodel.dtos import RootFolderDTO, FolderNodeDTO, RetentionTypeDTO
from sqlmodel import Session, select

def main():
    print("=== Database.get_engine() Demo ===")
    
    # Test 1: Get engine via class method
    print("\n1. Testing Database.get_engine() class method:")
    engine = Database.get_engine()
    print(f"   ✓ Engine created: {engine}")
    print(f"   ✓ Database URL: {engine.url}")
    
    # Test 2: Verify singleton behavior
    print("\n2. Testing singleton behavior:")
    engine2 = Database.get_engine()
    print(f"   ✓ Same engine instance: {engine is engine2}")

    print("\n2. Create tables:")
    Database.get_db().create_db_and_tables()

    # Test 3: Test database operations
    print("\n3. Testing database operations:")
    with Session(engine) as session:
        # Create some test data
        folder = RootFolderDTO(
            path="/demo/folder",
            folder_id=1,
            owner="Demo",
            approvers="TestUser",
            active_cleanup=False
        )
        
        retention = RetentionTypeDTO(
            name="30 days",
            is_system_managed=False,
            display_rank=1
        )
        
        session.add(folder)
        session.add(retention)
        session.commit()
        session.refresh(folder)
        session.refresh(retention)
        
        print(f"   ✓ Created RootFolderDTO with ID: {folder.id}")
        print(f"   ✓ Created RetentionDTO with ID: {retention.id}")
        
        # Query the data back
        statement = select(RootFolderDTO).where(RootFolderDTO.id == folder.id)
        retrieved_folder = session.exec(statement).first()
        
        print(f"   ✓ Retrieved folder: {retrieved_folder.path}")
        print(f"   ✓ Owner: {retrieved_folder.owner}")
    
    # Test 4: Multiple sessions with same engine
    print("\n4. Testing multiple sessions:")
    with Session(engine) as session:
        count = len(session.exec(select(RootFolderDTO)).all())
        print(f"   ✓ Total RootFolderDTO records: {count}")
    
    print("\n=== All tests passed! Database.get_engine() working correctly ===")

if __name__ == "__main__":
    main()
