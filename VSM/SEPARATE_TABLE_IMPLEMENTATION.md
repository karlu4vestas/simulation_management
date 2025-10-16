# CleanupConfigurationDTO - Separate Table Implementation

## Overview

Convert `CleanupConfiguration` from a dataclass to a separate SQLModel table with one-to-one relationship to `RootFolderDTO`.

**Why:** Prepare for future scheduler integration with state change actions.

**📋 Reference:** See [CLEANUP_CONFIGURATION_REFERENCES.md](./CLEANUP_CONFIGURATION_REFERENCES.md) for complete list of all 81 locations requiring updates across 12 files.

---

## Step 1: Add CleanupConfigurationDTO Model

**File:** `/server/datamodel/dtos.py`

**ADD (after CleanupProgressEnum, BEFORE the existing @dataclass CleanupConfiguration):**
```python
class CleanupConfigurationBase(SQLModel):
    """Base class for cleanup configuration."""
    rootfolder_id: int = Field(foreign_key="rootfolderdto.id", unique=True, index=True)
    cycletime: int = Field(default=0)
    cleanupfrequency: int = Field(default=0)
    cleanup_start_date: date | None = Field(default=None)
    cleanup_progress: str = Field(default=CleanupProgressEnum.INACTIVE.value)


class CleanupConfigurationDTO(CleanupConfigurationBase, table=True):
    """Cleanup configuration as separate table."""
    id: int | None = Field(default=None, primary_key=True)
    
    # Relationship
    rootfolder: "RootFolderDTO" = Relationship(back_populates="cleanup_config")
    
    # Copy all existing business logic methods from CleanupConfiguration dataclass:
    # - is_valid()
    # - can_start_cleanup()
    # - can_transition_to()
    # - get_valid_next_states()
    # - transition_to()
    # - transition_to_next()
    # Note: Update cleanup_progress references from enum to string comparisons
```

**KEEP the old @dataclass CleanupConfiguration for now** - We'll remove it in Step 8!

**Verify:**
```bash
cd /workspaces/simulation_management/VSM/server
python -m pytest tests/unittests/dtos/ -v
```
All existing tests should still pass since we haven't broken anything yet.

---

## Step 2: Add Relationship to RootFolderDTO

**File:** `/server/datamodel/dtos.py`

**ADD to RootFolderDTO class (KEEP existing fields for now):**
```python
class RootFolderDTO(RootFolderBase, table=True):
    id: int | None = Field(default=None, primary_key=True)
    
    # Add this new relationship:
    cleanup_config: CleanupConfigurationDTO | None = Relationship(
        back_populates="rootfolder",
        sa_relationship_kwargs={"uselist": False}
    )
    
    # KEEP existing get_cleanup_configuration() and set_cleanup_configuration() methods for now
```

**ADD new helper method (alongside existing methods):**
```python
def ensure_cleanup_config(self, session) -> CleanupConfigurationDTO:
    """Get or create cleanup configuration."""
    if self.cleanup_config is None:
        self.cleanup_config = CleanupConfigurationDTO(rootfolder_id=self.id)
        session.add(self.cleanup_config)
    return self.cleanup_config
```

**DON'T DELETE anything yet!** We're adding alongside the old code.

**Verify:**
```bash
cd /workspaces/simulation_management/VSM/server
python -m pytest tests/unittests/dtos/ -v
```
Tests should still pass - we haven't broken backward compatibility.

---

## Step 3: Create Database Schema

**File:** `/server/db/database.py` (or wherever database is initialized)

**ENSURE CleanupConfigurationDTO is imported:**
```python
from datamodel.dtos import (
    # ... existing imports ...
    CleanupConfigurationDTO,  # ADD THIS
)
```

**Run database initialization to create the new table:**
```python
# The table will be created automatically when you run:
SQLModel.metadata.create_all(engine)
```

**Verify:**
```bash
cd /workspaces/simulation_management/VSM/server
python -m pytest tests/unittests/dtos/ -v
```
Tests should pass - the new table exists but isn't used yet.

---

## Step 4: Update API Endpoints (Migration)

**File:** `/server/app/web_api.py`

**FIND and REPLACE patterns:**

**Pattern to find:**
```python
config = rootfolder.get_cleanup_configuration()
# ... modifications ...
rootfolder.set_cleanup_configuration(config)
```

**Replace with:**
```python
config = rootfolder.ensure_cleanup_config(session)
# ... modifications ...
# (no set_cleanup_configuration needed - automatic persistence!)
```

**Specific locations (~line numbers may vary):**
- Line ~190: Remove `set_cleanup_configuration()` call
- Line ~411, 477: Use `ensure_cleanup_config(session)` instead
- Line ~482: Remove `set_cleanup_configuration()` call

**Verify:**
```bash
cd /workspaces/simulation_management/VSM/server
python -m pytest tests/unittests/dtos/ -v
```
Unit tests should pass. Integration tests may need updates in next step.

---

## Step 5: Update RetentionCalculator (Optional)

**File:** `/server/datamodel/retention_validators.py`

**No code changes needed!** RetentionCalculator already accepts the config object by duck typing.

**Optional:** Update type hint for clarity:
```python
def __init__(self, 
             retention_type_dict: dict[str, RetentionTypeDTO], 
             cleanup_config: CleanupConfigurationDTO):  # Update type hint
```

**Verify:**
```bash
cd /workspaces/simulation_management/VSM/server
python -m pytest tests/unittests/dtos/ -v
```
All tests should still pass.

---

## Step 6: Update Unit Tests

**File:** `/server/tests/unittests/dtos/test_cleanup_configuration.py`

**ADD required fixtures at the top:**
```python
@pytest.fixture
def test_rootfolder(test_session):
    """Create a test rootfolder."""
    rootfolder = RootFolderDTO(simulationdomain_id=1, owner="test", path="/test")
    test_session.add(rootfolder)
    test_session.commit()
    test_session.refresh(rootfolder)
    return rootfolder
```

**UPDATE each test to use CleanupConfigurationDTO:**

**Pattern - Before:**
```python
def test_transition():
    config = CleanupConfiguration(cycletime=30, cleanupfrequency=90)
    config.transition_to_next()
    assert config.cleanup_progress == CleanupProgressEnum.STARTED
```

**Pattern - After:**
```python
def test_transition(test_session, test_rootfolder):
    config = CleanupConfigurationDTO(
        rootfolder_id=test_rootfolder.id,
        cycletime=30,
        cleanupfrequency=90
    )
    test_session.add(config)
    test_session.commit()
    
    config.transition_to_next()
    test_session.commit()
    
    assert config.cleanup_progress == CleanupProgressEnum.STARTED.value  # .value for string
```

**Apply this pattern to all 55 tests.**

**Verify:**
```bash
cd /workspaces/simulation_management/VSM/server
python -m pytest tests/unittests/dtos/ -v
```
All 73+ tests should pass with new CleanupConfigurationDTO.

---

## Step 7: Update Test Data Generation and Integration Tests

**File:** `/server/testdata/vts_generate_test_data.py`

**UPDATE where rootfolders are created:**
```python
def generate_root_folder(session, ...):
    # Create rootfolder
    rootfolder = RootFolderDTO(...)
    session.add(rootfolder)
    session.flush()  # Get ID assigned
    
    # Create cleanup config (NEW)
    cleanup_config = CleanupConfigurationDTO(
        rootfolder_id=rootfolder.id,
        cycletime=cycle_time,
        cleanupfrequency=frequency
    )
    session.add(cleanup_config)
```

**File:** `/server/tests/conftest.py`

**UPDATE test fixtures (lines ~234, 244):**
```python
# Replace get/set pattern with ensure_cleanup_config
config = rootfolder.ensure_cleanup_config(session)
# Remove set_cleanup_configuration() calls
```

**Verify:**
```bash
cd /workspaces/simulation_management/VSM/server
python -m pytest tests/unittests/dtos/ -v
python -m pytest tests/integration/ -v  # If integration tests exist
```
All tests should pass - migration to new DTO is complete!

---

## Step 8: Clean Up Old Code (Final Step)

**File:** `/server/datamodel/dtos.py`

**NOW we can remove the old code:**

### 8.1 Remove old dataclass

**DELETE:**
```python
@dataclass
class CleanupConfiguration:
    cycletime: int
    cleanupfrequency: int
    cleanup_start_date: date | None = None
    cleanup_progress: CleanupProgressEnum = CleanupProgressEnum.INACTIVE
    
    def __eq__(self, other):
        # ... entire method ...
    
    def is_valid(self) -> tuple[bool, str]:
        # ... entire method ...
    
    # ... all other methods ...
```

### 8.2 Remove old methods from RootFolderBase

**DELETE from RootFolderBase:**
```python
cycletime: int = Field(default=0)
cleanupfrequency: int = Field(default=0)
cleanup_round_start_date: date | None = Field(default=None)
cleanup_progress: str = Field(default=CleanupProgressEnum.INACTIVE.value)
```

**DELETE from RootFolderDTO:**
```python
def get_cleanup_configuration(self) -> CleanupConfiguration:
    return CleanupConfiguration(
        self.cycletime, 
        self.cleanupfrequency, 
        self.cleanup_round_start_date,
        CleanupProgressEnum(self.cleanup_progress)
    )

def set_cleanup_configuration(self, cleanup: CleanupConfiguration):
    self.cycletime = cleanup.cycletime
    self.cleanupfrequency = cleanup.cleanupfrequency
    self.cleanup_round_start_date = cleanup.cleanup_start_date
    self.cleanup_progress = cleanup.cleanup_progress.value
```

**KEEP only:**
```python
def ensure_cleanup_config(self, session) -> CleanupConfigurationDTO:
    """Get or create cleanup configuration."""
    if self.cleanup_config is None:
        self.cleanup_config = CleanupConfigurationDTO(rootfolder_id=self.id)
        session.add(self.cleanup_config)
    return self.cleanup_config
```

**Final Verify:**
```bash
cd /workspaces/simulation_management/VSM/server
python -m pytest tests/unittests/dtos/ -v
python -m pytest tests/integration/ -v
```
All tests should pass - refactoring complete! ✅

---

## Implementation Checklist

- [ ] **Step 1:** Add `CleanupConfigurationBase` and `CleanupConfigurationDTO` (keep old dataclass) → Run tests ✓
- [ ] **Step 2:** Add relationship to `RootFolderDTO` and `ensure_cleanup_config()` method (keep old methods) → Run tests ✓
- [ ] **Step 3:** Create database schema (import CleanupConfigurationDTO) → Run tests ✓
- [ ] **Step 4:** Update web_api.py to use `ensure_cleanup_config()` instead of get/set → Run tests ✓
- [ ] **Step 5:** Update RetentionCalculator type hint (optional) → Run tests ✓
- [ ] **Step 6:** Update all 55 unit tests to use CleanupConfigurationDTO → Run tests ✓
- [ ] **Step 7:** Update test data generation and integration tests → Run tests ✓
- [ ] **Step 8:** Remove old `@dataclass CleanupConfiguration` and old RootFolder fields/methods → Run tests ✓
- [ ] **Final:** Run full test suite and verify application works

---

## Migration Strategy Summary

This incremental approach:
1. ✅ **Adds new code alongside old** (Steps 1-3)
2. ✅ **Migrates usage gradually** (Steps 4-7)
3. ✅ **Tests after each step** - Ensures nothing breaks
4. ✅ **Removes old code last** (Step 8)

**Benefits:**
- Low risk - can rollback at any step
- Tests provide safety net
- Clear verification points
- Old and new code coexist during migration

---

## Key Changes Summary

| What | Before | After |
|------|--------|-------|
| **Model** | `@dataclass CleanupConfiguration` | `CleanupConfigurationDTO(SQLModel, table=True)` |
| **Storage** | Fields in `RootFolderDTO` | Separate table with foreign key |
| **Access** | `rootfolder.get_cleanup_configuration()` | `rootfolder.ensure_cleanup_config(session)` |
| **Modify** | Create copy, modify, call `set_cleanup_configuration()` | Direct modification, automatic persistence |
| **Progress field** | `CleanupProgressEnum` | `str` (enum value) |
| **Tests** | Create CleanupConfiguration directly | Need session and rootfolder fixture |
