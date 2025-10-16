# CleanupConfiguration Reference Locations

Complete list of all files referencing `CleanupConfiguration` that need to be updated during the refactoring.

**Referenced by:** See [SEPARATE_TABLE_IMPLEMENTATION.md](./SEPARATE_TABLE_IMPLEMENTATION.md) for the implementation plan.

---

## Summary by Category

| Category | Files | Total References |
|----------|-------|------------------|
| **Core Model** | 1 | 5 |
| **API/Web** | 2 | 8 |
| **Database** | 1 | 4 |
| **Validators** | 1 | 2 |
| **Unit Tests** | 1 | 55 |
| **Integration Tests** | 3 | 6 |
| **C# Client** | 2 | 6 |
| **Documentation** | 1 | 18 |
| **TOTAL** | 12 files | **104 references** |

---

## 1. Core Model Definition

### `/server/datamodel/dtos.py` (5 references)
**Step 1 & Step 8 - Model definition and cleanup**

| Line | Type | Code | Action |
|------|------|------|--------|
| 88 | Class definition | `class CleanupConfiguration:` | **DELETE in Step 8** |
| 96 | Type check | `isinstance(other, CleanupConfiguration)` | **DELETE in Step 8** (part of __eq__) |
| 208 | Method return type | `def get_cleanup_configuration(self) -> CleanupConfiguration:` | **DELETE in Step 8** |
| 209 | Instantiation | `return CleanupConfiguration(...)` | **DELETE in Step 8** |
| 216 | Parameter type | `def set_cleanup_configuration(self, cleanup: CleanupConfiguration):` | **DELETE in Step 8** |

---

## 2. API/Web Layer

### `/server/app/web_api.py` (4 references)
**Step 4 - Update API endpoints**

| Line | Type | Code | Action |
|------|------|------|--------|
| 10 | Import | `from datamodel.dtos import CleanupConfiguration, ...` | **REPLACE with CleanupConfigurationDTO** |
| 171 | Parameter type | `def update_rootfolder_cleanup_configuration(..., cleanup_configuration: CleanupConfiguration)` | **UPDATE type hint** |
| 201 | Return type | `def get_cleanup_configuration_by_rootfolder_id(...) -> CleanupConfiguration:` | **UPDATE type hint** |
| 411 | Variable type hint | `cleanup_config: CleanupConfiguration = rootfolder.get_cleanup_configuration()` | **REPLACE with ensure_cleanup_config(session)** |
| 415 | Error message | `"...CleanupConfiguration is missing..."` | No change needed |
| 477 | Variable type hint | `cleanup_config: CleanupConfiguration = rootfolder.get_cleanup_configuration()` | **REPLACE with ensure_cleanup_config(session)** |

**Pattern to replace:**
```python
# OLD (Step 4 - BEFORE):
cleanup_config: CleanupConfiguration = rootfolder.get_cleanup_configuration()
# ... modifications ...
rootfolder.set_cleanup_configuration(cleanup_config)

# NEW (Step 4 - AFTER):
cleanup_config = rootfolder.ensure_cleanup_config(session)
# ... modifications ...
# (no set_cleanup_configuration needed)
```

### `/server/app/web_server_retention_api.py` (1 reference)
**Step 4 - Update imports**

| Line | Type | Code | Action |
|------|------|------|--------|
| 7 | Import | `from datamodel.dtos import CleanupConfiguration, ...` | **REPLACE with CleanupConfigurationDTO** |

---

## 3. Database Layer

### `/server/db/db_api.py` (4 references)
**Step 3 & 4 - Database operations**

| Line | Type | Code | Action |
|------|------|------|--------|
| 4 | Import | `from datamodel.dtos import CleanupConfiguration, ...` | **REPLACE with CleanupConfigurationDTO** |

---

## 4. Business Logic/Validators

### `/server/datamodel/retention_validators.py` (2 references)
**Step 5 - Update type hints (optional)**

| Line | Type | Code | Action |
|------|------|------|--------|
| 5 | Import | `from datamodel.dtos import CleanupConfiguration, ...` | **REPLACE with CleanupConfigurationDTO** |
| 10 | Parameter type | `def __init__(self, ..., cleanup_config: CleanupConfiguration)` | **UPDATE type hint (optional)** |

**Note:** RetentionCalculator uses duck typing, so it will work with CleanupConfigurationDTO without changes. Updating the type hint is optional but recommended.

---

## 5. Unit Tests

### `/server/tests/unittests/dtos/test_cleanup_configuration.py` (55 references!)
**Step 6 - Update all unit tests**

| Line Range | Count | Type | Action |
|------------|-------|------|--------|
| 2 | 1 | Docstring | `Unit tests for CleanupConfiguration` | Update comment |
| 6 | 1 | Import | `from datamodel.dtos import CleanupConfiguration, CleanupProgressEnum` | **REPLACE with CleanupConfigurationDTO** |
| 32-89 | 10 | Test class/instantiation | `class TestCleanupConfiguration` | **UPDATE to use CleanupConfigurationDTO** |
| 96-196 | 10 | Test validation | `class TestCleanupConfigurationValidation` | **UPDATE to use CleanupConfigurationDTO** |
| 205-401 | 20 | Test transitions | `class TestCleanupConfigurationStateTransitions` | **UPDATE to use CleanupConfigurationDTO** |
| 412-477 | 5 | Test workflow | `class TestCleanupConfigurationCompleteWorkflow` | **UPDATE to use CleanupConfigurationDTO** |
| 489-620 | 8 | Test transition_to | `class TestCleanupConfigurationTransitionTo` | **UPDATE to use CleanupConfigurationDTO** |
| 633-742 | 7 | Test transition_to_next | `class TestCleanupConfigurationTransitionToNext` | **UPDATE to use CleanupConfigurationDTO** |

**Every test needs:**
1. Add `test_session` and `test_rootfolder` fixtures as parameters
2. Create `CleanupConfigurationDTO` with `rootfolder_id` parameter
3. Add to session and commit
4. Update assertions for `.value` on enum comparisons (string instead of enum)

**Example pattern - Before:**
```python
def test_something():
    config = CleanupConfiguration(cycletime=30, cleanupfrequency=90)
    config.transition_to_next()
    assert config.cleanup_progress == CleanupProgressEnum.STARTED
```

**Example pattern - After:**
```python
def test_something(test_session, test_rootfolder):
    config = CleanupConfigurationDTO(
        rootfolder_id=test_rootfolder.id,
        cycletime=30,
        cleanupfrequency=90
    )
    test_session.add(config)
    test_session.commit()
    
    config.transition_to_next()
    test_session.commit()
    
    assert config.cleanup_progress == CleanupProgressEnum.STARTED.value  # .value!
```

---

## 6. Integration Tests

### `/server/tests/conftest.py` (2 references)
**Step 6 & 7 - Update test fixtures**

| Line | Type | Code | Action |
|------|------|------|--------|
| 9 | Import | `from datamodel.dtos import CleanupConfiguration, ...` | **REPLACE with CleanupConfigurationDTO** |
| 226 | Instantiation | `cleanup_configuration = CleanupConfiguration(...)` | **UPDATE to use rootfolder.ensure_cleanup_config(session)** |

**Pattern:**
```python
# OLD:
cleanup_configuration = CleanupConfiguration(cycletime=30, cleanupfrequency=7, cleanup_start_date=date(2000, 1, 1))
rootfolder.set_cleanup_configuration(cleanup_configuration)

# NEW:
cleanup_config = rootfolder.ensure_cleanup_config(session)
cleanup_config.cycletime = 30
cleanup_config.cleanupfrequency = 7
cleanup_config.cleanup_start_date = date(2000, 1, 1)
```

### `/server/tests/integration/base_integration_test.py` (3 references)
**Step 7 - Update integration test base class**

| Line | Type | Code | Action |
|------|------|------|--------|
| 5 | Import | `from datamodel.dtos import CleanupConfiguration, ...` | **REPLACE with CleanupConfigurationDTO** |
| 51 | Method signature | `def update_cleanup_configuration(..., cleanup_configuration: CleanupConfiguration) -> CleanupConfiguration:` | **UPDATE type hints** |
| 52 | Comment | `#Step 3: Update the CleanupConfiguration...` | Update comment |

### `/server/tests/integration/test_cleanup_workflows.py` (1 reference)
**Step 7 - Update integration tests**

| Line | Type | Code | Action |
|------|------|------|--------|
| 6 | Import | `from datamodel.dtos import CleanupConfiguration, ...` | **REPLACE with CleanupConfigurationDTO** |

### `/server/tests/integration/testdata_for_import.py` (2 references)
**Step 7 - Update test data generation**

| Line | Type | Code | Action |
|------|------|------|--------|
| 8 | Import | `from datamodel.dtos import CleanupConfiguration, ...` | **REPLACE with CleanupConfigurationDTO** |
| 341 | Variable type | `cleanup_configuration: CleanupConfiguration = rootfolder.get_cleanup_configuration()` | **UPDATE to ensure_cleanup_config(session)** |

---

## 7. Test Data Generation

### `/server/testdata/vts_generate_test_data.py` (inferred - not in search)
**Step 7 - Update test data generation**

**Action:** Need to create `CleanupConfigurationDTO` when generating rootfolders:
```python
# ADD after creating rootfolder:
cleanup_config = CleanupConfigurationDTO(
    rootfolder_id=rootfolder.id,
    cycletime=cycle_time,
    cleanupfrequency=frequency
)
session.add(cleanup_config)
```

---

## 8. C# Client (Blazor Frontend)

### `/VSM.Client/API/api.cs` (2 references)
**No changes needed - C# DTO is separate**

| Line | Type | Code | Note |
|------|------|------|------|
| 99 | Method | `UpdateCleanupConfigurationForRootFolder(..., CleanupConfigurationDTO cleanup_configuration)` | Already uses DTO name |

### `/VSM.Client/Pages/LibraryPage.razor` (4 references)
**No changes needed - Already uses DTO**

| Line | Type | Code | Note |
|------|------|------|------|
| 46 | Property access | `folder.CleanupConfiguration.CycleTime` | Works with backend API |
| 52 | Property access | `folder.CleanupConfiguration.CleanupFrequency` | Works with backend API |
| 58 | Property access | `folder.CleanupConfiguration.CanStartCleanup` | Works with backend API |
| 130 | Variable | `CleanupConfigurationDTO cleanup_config = new();` | Already correct |

**Note:** C# client already uses the DTO naming convention and communicates via API, so no changes needed.

---

## 9. Documentation

### `/SEPARATE_TABLE_IMPLEMENTATION.md` (18 references)
**Planning document - no action needed**

This is the implementation plan itself, references are expected.

---

## Implementation Order (By Step)

### **Step 1:** Add CleanupConfigurationDTO Model
- ✅ `/server/datamodel/dtos.py` - Add new classes (KEEP old for now)

### **Step 2:** Add Relationship to RootFolderDTO  
- ✅ `/server/datamodel/dtos.py` - Add relationship (KEEP old methods for now)

### **Step 3:** Create Database Schema
- ✅ `/server/db/database.py` - Import CleanupConfigurationDTO

### **Step 4:** Update API Endpoints
- 📝 `/server/app/web_api.py` - 4 locations
- 📝 `/server/app/web_server_retention_api.py` - 1 location
- 📝 `/server/db/db_api.py` - 1 location

### **Step 5:** Update RetentionCalculator (Optional)
- 📝 `/server/datamodel/retention_validators.py` - 2 locations

### **Step 6:** Update Unit Tests
- 📝 `/server/tests/unittests/dtos/test_cleanup_configuration.py` - **55 tests!**
- 📝 `/server/tests/conftest.py` - 2 locations

### **Step 7:** Update Integration Tests & Test Data
- 📝 `/server/tests/integration/base_integration_test.py` - 3 locations
- 📝 `/server/tests/integration/test_cleanup_workflows.py` - 1 location
- 📝 `/server/tests/integration/testdata_for_import.py` - 2 locations
- 📝 `/server/testdata/vts_generate_test_data.py` - Need to find and update

### **Step 8:** Clean Up Old Code
- 🗑️ `/server/datamodel/dtos.py` - DELETE @dataclass CleanupConfiguration
- 🗑️ `/server/datamodel/dtos.py` - DELETE cleanup fields from RootFolderBase
- 🗑️ `/server/datamodel/dtos.py` - DELETE get/set_cleanup_configuration methods

---

## Files NOT Requiring Changes

✅ **C# Client files** - Already use DTO naming, communicate via API  
✅ **Documentation files** - References are expected  
✅ **Migration scripts** - Not needed (no existing database)

---

## Verification Commands

After each step, run:
```bash
cd /workspaces/simulation_management/VSM/server
python -m pytest tests/unittests/dtos/ -v
```

After Step 7, also run:
```bash
python -m pytest tests/integration/ -v
```

After Step 8 (final cleanup), run full suite:
```bash
python -m pytest -v
```

---

## Quick Reference: Search Commands

To find all references:
```bash
# From /workspaces/simulation_management/VSM/server
grep -r "CleanupConfiguration" --include="*.py" .
```

To find specific patterns:
```bash
# Find get_cleanup_configuration calls
grep -rn "get_cleanup_configuration" --include="*.py" .

# Find set_cleanup_configuration calls
grep -rn "set_cleanup_configuration" --include="*.py" .

# Find CleanupConfiguration imports
grep -rn "from.*import.*CleanupConfiguration" --include="*.py" .
```

---

## Total Effort Estimate

| Step | Files | References | Estimated Time |
|------|-------|------------|----------------|
| Step 1 | 1 | - | 30 min (add new code) |
| Step 2 | 1 | - | 15 min (add relationship) |
| Step 3 | 1 | 1 | 10 min (import) |
| Step 4 | 3 | 8 | 1 hour (API updates) |
| Step 5 | 1 | 2 | 10 min (type hints) |
| Step 6 | 2 | 57 | **3-4 hours** (55 test updates!) |
| Step 7 | 4 | 8 | 1 hour (integration tests) |
| Step 8 | 1 | 5 | 30 min (delete old code) |
| **TOTAL** | **12 files** | **81 references** | **7-8 hours** |

**Note:** Step 6 (unit tests) is the most time-consuming with 55 tests to update!
