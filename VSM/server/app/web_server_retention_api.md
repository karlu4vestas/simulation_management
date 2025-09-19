# 0 what we need to do
implement insert_or_update_simulatio_db with the following signature:
from typing import NamedTuple, Literal
from datetime import date

class FileInfo(NamedTuple):
    filepath: str
    modified: date
    status: Literal["clean", "issue", "normal"]
def insert_or_update_simulatio_db(rootfolder_id: int, simulations: list[FileInfo]]):

where rootfolder_id can selected the existing folders 
where simulation consist of a url and its metadata


# 1. What we already have
- Materialized `path_ids` → e.g. `/1/5/9/`
- Materialized `path_urls` → e.g. `/root/child/grandchild/`
- Each row has:
  - `id`
  - `parent_id`
  - `name`
  - `path_ids`
  - `path_urls`
  - `rootfolder_id`

# 2. Problem we are facing
When update or insert a new URL like `/root/child/grandchild` with attributes `present and future attributes`, how do we know:
- Where each segment in the path belongs?
- Which folders already exist?
- At what level to create a new node?

# 3. General Approach
Divide the set into a set to update and a set to insert. The sets can be identified by trying to retrieve the row `id` by matching the filepath against the materialsed `path_urls` using caseinsensitive string matching. 

# 3. Approach to update
make a buld update of the attributes using the retrived `id`


# 4. Approach to insert
start by inserint gthe path and then make an update of the atttribtues:

**Insert filepath**
You need to walk the path segments left-to-right:

1. Split the URL into segments: `['root', 'child', 'grandchild']`.
2. Start from the root (`parent_id IS NULL`).
3. For each segment:
   - Look for an existing row with `parent_id = current_id` **AND** `name = segment`.
   - If **found** → move to that row.
   - If **not found** → create a new row under the current parent, generate its `path_ids` and `path_urls`.

When the loop finishes, you’ll have inserted missing nodes and end up at the leaf node.

**Update attributes**
TBD: `Then use bulk update as described above to update/set the attributes because once the path exists then there is no difference between first insertions and and updates.`

# 4. Concrete Example
Say the table is empty and you want to insert `/root/child/grandchild`.

**Step 1: Insert root**
- Not found → insert:
  - `id = 1`, `parent_id = NULL`
  - `name = 'root'`
  - `path_ids = '/1/'`
  - `path_urls = '/root/'`

**Step 2: Insert child**
- Not found under `parent_id = 1` → insert:
  - `id = 2`, `parent_id = 1`
  - `name = 'child'`
  - `path_ids = '/1/2/'`
  - `path_urls = '/root/child/'`

**Step 3: Insert grandchild**
- Not found under `parent_id = 2` → insert:
  - `id = 3`, `parent_id = 2`
  - `name = 'grandchild'`
  - `path_ids = '/1/2/3/'`
  - `path_urls = '/root/child/grandchild/'`
