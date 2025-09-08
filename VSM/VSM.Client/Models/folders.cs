namespace VSM.Client.Datamodel
{
    /// <summary>
    /// we use ChangeRetentionDelegate in order to use the same iterator over a subtree when we need to change the retentype and the PathRetention_Id
    /// The delegate is responsible for determining which nodes should have their retention settings updated. and it also containts the target retentype and pathretention_id
    /// </summary>
    public abstract class ChangeRetentionDelegate
    {
        protected Retention from;
        public Retention to;
        public ChangeRetentionDelegate(Retention to)
        {
            this.to = to.Clone();
            this.from = new Retention(); //dummy
        }
        public ChangeRetentionDelegate(Retention from, Retention to)
        {
            this.from = from.Clone();
            this.to = to.Clone();
        }
        public abstract bool update_retention(FolderNode node);
    }
    /// <summary>
    // usecases: add a path protection to a tree with a mix of 
    //      1) subtrees with path retentions that must not be overwritten (node.Retention_Id == to_retention_Id) = true 
    //      2) subtrees without path protections that must be overwritten (node.Retention_Id == to_retention_Id) = false 
    /// </summary>
    public class AddPathProtectionOnMixedSubtreesDelegate : ChangeRetentionDelegate
    {
        //add path protection without consideration for the source retention. except that it must no be of the type to.TypeId (pathretentiontype)
        public AddPathProtectionOnMixedSubtreesDelegate(Retention to) : base(to) { }
        public override bool update_retention(FolderNode node)
        {
            return node.Retention.TypeId != to.TypeId;
        }
    }
    /// <summary>
    // usecases: add a path retention to a pathprotect tree with possible subtrees with other path retentions that must not be overwritten 
    //    1) an existing parent PathProtection   (node.Retention_Id == to_retention_Id)=true and (node.Path_Protection_Id == from_path_protection_id)=true 
    //    2) subtrees with other PathProtections (PathProtection_Id == from_path_protection_id)=false must not be overwritten
    /// </summary>
    public class AddPathProtectionToParentPathProtectionDelegate : ChangeRetentionDelegate
    {
        public AddPathProtectionToParentPathProtectionDelegate(Retention from, Retention to) : base(from, to) { }
        public override bool update_retention(FolderNode node)
        {
            return node.Retention.TypeId == to.TypeId && node.Retention.PathId == from.PathId;
        }
    }

    /// <summary>
    /// only change retention with full match of retentiontype and pathretention_id
    /// </summary>
    public class ChangeOnFullmatchDelegate : ChangeRetentionDelegate
    {
        public ChangeOnFullmatchDelegate(Retention from, Retention to) : base(from, to) { }
        public override bool update_retention(FolderNode node)
        {
            return node.Retention.TypeId == from.TypeId && node.Retention.PathId == from.PathId;
        }
    }
    public class Retention
    {
        public int TypeId = 0;
        public int PathId = 0; // means no path protection
        public Retention()
        {
        }
        public Retention(int TypeId, int PathId = 0)
        {
            this.TypeId = TypeId;
            this.PathId = PathId;
        }
        public Retention(Retention retention)
        {
            this.TypeId = retention.TypeId;
            this.PathId = retention.PathId;
        }
        // override copy constructor
        public Retention Clone()
        public override bool update_retention(FolderNode node)
        {
            return new Retention(this);
        }
    }
    public class FolderNode
    {
        protected FolderNodeDTO dto;
        public FolderNode(FolderNodeDTO dto)
        {
            this.dto = dto;
        }
        //mapped to server fields
        public int Id => dto.Id;
        public int Parent_Id => dto.Parent_Id;
        public int Rootfolder_Id => dto.Rootfolder_Id;
        public string Name => dto.Name;
        public int Type_Id => dto.Type_Id;
        public Retention Retention
        {
            get => new Retention { TypeId = dto.Retention_Id, PathId = dto.Path_Protection_Id };
            set
            {
                dto.Retention_Id = value.TypeId;
                dto.Path_Protection_Id = value.PathId;
            }
        }
        //client side helper fields
        public int Retention_Id { get => dto.Retention_Id; set => dto.Retention_Id = value; }
        public int Path_Protection_Id { get => dto.Path_Protection_Id; set => dto.Path_Protection_Id = value; }
        //client side help fields. 
        // It is reconstructed on the fly,, which is ok because it is only used for display of the path protection list and when the user selects a folder
        public string FullPath
        {
            get
            {
                if (Parent is null)
                {
                    return Name; // Root node or no parent
                }
                else
                {
                    return $"{Parent.FullPath}/{Name}";
                }
            }
        }
        public bool IsLeaf { get { return !Children.Any(); } }
        public Dictionary<int, int> AttributeDict { get; set; } = new();
        public FolderNode? Parent { get; set; } = null; // Default to null, indicating no parent
        public List<FolderNode> Children { get; set; } = new();
        public async Task<FolderNode> find_by_folder_id(int folder_id)
        {
            // Iterative DFS to find folder by ID
            var stack = new Stack<FolderNode>();
            stack.Push(this);
            while (stack.Count > 0)
            {
                var currentNode = stack.Pop();
                if (currentNode.Id == folder_id)
                {
                    return currentNode; // Found the folder
                }
                // Add children to stack for further exploration
                foreach (var child in currentNode.Children)
                {
                    stack.Push(child);
                }
                // Yield control periodically for large trees to prevent UI blocking
                if (stack.Count % 100 == 0)
                {
                    await Task.Yield();
                }
            }
            throw new Exception($"Folder with ID {folder_id} not found.");
        }

        public async Task ChangeRetentions(Retention from, Retention to)
        public async Task ChangeRetentions(byte from_retentiontype_Id, byte to_retentiontype_Id)
        {
            await ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(from, to));
        }

        public async Task<PathProtectionDTO> AddPathProtection(RetentionConfiguration retention_config)
        {
            // Should handle adding if there is no path protections and 
            // adding to existing path protections
            //   - siblings 
            //   - parent to one or more path protections at lower level
            //   - child
            int path_retentiontype_id = retention_config.Path_retentiontype.Id;

            PathProtectionDTO? parent_protection = FindClosestPathProtectedParent(retention_config);
            Retention? parent_path_retention = parent_protection == null ? null : new Retention(path_retentiontype_id, parent_protection.Id);

            PathProtectionDTO new_path_protection = new PathProtectionDTO
            {
                //Id = pathProtection.Id, // Id will be set by the server
                Id = DataModel.Instance.NewID,  //untill we use persistance to get an ID
                Rootfolder_Id = this.Rootfolder_Id,
                Folder_Id = this.Id,
                Path = this.FullPath
            };
            retention_config.Path_protections.Add(new_path_protection);
            Retention new_path_retention = new Retention(path_retentiontype_id, new_path_protection.Id);

            if (parent_path_retention != null)
                //add a sub pathprotection to a parent pathprotection    
                await ChangeRetentionsOfSubtree(new AddPathProtectionToParentPathProtectionDelegate(parent_path_retention, new_path_retention));
            else
                await ChangeRetentionsOfSubtree(new AddPathProtectionOnMixedSubtreesDelegate(new_path_retention));

            return new_path_protection;
        }
        private PathProtectionDTO? FindClosestPathProtectedParent(RetentionConfiguration retention_config)
        {
            PathProtectionDTO? closest_path_protection = null;
            FolderNode current = this;
            while (current.Parent != null && closest_path_protection == null)
            {
                closest_path_protection = retention_config.Path_protections.FirstOrDefault(r => r.Folder_Id == current.Id);
                current = current.Parent;
            }
            return closest_path_protection;
        }
        public async Task<int> RemovePathProtection(RetentionConfiguration retention_config, int to_retention_Id)
        {
            // step 1: find this node path protection entry 
            // step 2: remove it from the list if found
            // step 3: Verify if this node has a parent PathProtection in the retention_config
            //         If there is no parent path protection, 
            //            -then set the retention of leaves under this nodes to (to_retention_Id, to_path_protection_id=0). 
            //         else  
            //            -then set the retention of leaves under this node to (path protection type,  from_path_protectection.Id).
            //         In this way we do not touch path protection of other children.
            int path_retentiontype_id = retention_config.Path_retentiontype.Id;

            PathProtectionDTO? from_path_protection = retention_config.Path_protections.FirstOrDefault(p => p.Folder_Id == this.Id);
            int remove_count = retention_config.Path_protections.RemoveAll(p => p.Folder_Id == this.Id);
            Retention? from_path_retention = from_path_protection == null ? null : new Retention(path_retentiontype_id, from_path_protection.Id);

            // check for the presence of a parent of path_protection_folder
            PathProtectionDTO? parent_path_protection = this.FindClosestPathProtectedParent(retention_config);
            Retention? to_parent_path_retention = parent_path_protection == null ? null : new Retention(path_retentiontype_id, parent_path_protection.Id);
            if (from_path_retention == null)
                throw new ArgumentException("Invalid new path protection folder specified.");
            else
            {
                if (to_parent_path_retention != null)
                    await ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(from_path_retention, to_parent_path_retention));
                else
                    await ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(from_path_retention, new Retention(to_retention_Id)));
            }
            return remove_count;
        }
        public async Task ChangeRetentionsOfSubtree(ChangeRetentionDelegate change_delegate)
        {
            //select the subtree to folder incl folder that have retention equal to  (from_retention_ID, from_path_protection_id)
            // and change the retentions to (to_retention_ID, to_path_protection_id)
            //Console.WriteLine($"ChangeRetentionsOfSubtree: {this.FullPath} to_retention_ID: {change_delegate.to_retention.TypeId}, to_path_protection_id: {change_delegate.to_retention.PathId} ");
            int number_of_change_leafs = 0;
            int number_of_unchanged_leafs = 0;
            var stack = new Stack<FolderNode>();
            stack.Push(this);
            while (stack.Count > 0)
            {
                var currentNode = stack.Pop();

                if (currentNode.IsLeaf)
                {
                    // use the delegate to check if current node matches the criteria and update if so
                    if (change_delegate.update_retention(currentNode))
                    {
                        currentNode.Retention = change_delegate.to.Clone();
                        number_of_change_leafs++;
                    }
                    else
                    {
                        number_of_unchanged_leafs++;
                    }
                }
                else
                {
                    foreach (var child in currentNode.Children)
                    {
                        stack.Push(child);
                    }
                }
                // Yield control periodically for large trees to prevent UI blocking
                if (stack.Count % 100 == 0)
                {
                    await Task.Yield();
                }
            }
            Console.WriteLine($"ChangeRetentionsOfSubtree changed leafs, unchanged leafs : {number_of_change_leafs} {number_of_unchanged_leafs}");
            // print_leafs(folder);
        }
        void print_leafs()
        {
            // used for debugging only
            // print fullpath, retention_id, path_protection_id
            if (this.IsLeaf)
                Console.WriteLine($"leaf fullpath, retention_id, path_protection_id:" +
                                  $" {this.FullPath} {this.Retention.TypeId} {this.Retention.PathId}");
            foreach (var child in this.Children)
            {
                child.print_leafs();
            }
        }
        /// <summary>
        /// Aggregates counts of different retention values by iterating the folder tree using depth-first traversal.
        /// The iteration starts from the leaves and aggregates up to the current level using post-order processing.
        /// Uses an iterative approach with a stack to avoid recursive function calls.
        /// After execution, each folder's AttributDict contains the total count of each retention type in its subtree.
        /// </summary>
        public async Task UpdateAggregation()
        {
            // Initialize the AttributDict for this folder
            AttributeDict.Clear();

            // Use a stack for iterative depth-first traversal (avoiding recursion)
            var stack = new Stack<(FolderNode node, bool visited)>();
            var processedFolders = new HashSet<int>();

            // Start with current folder
            stack.Push((this, false));
            while (stack.Count > 0)
            {
                var (currentNode, visited) = stack.Pop();
                if (visited)
                {
                    // Post-order processing: aggregate children's values
                    if (!processedFolders.Contains(currentNode.Id))
                    {
                        // Initialize this node's AttributDict
                        currentNode.AttributeDict.Clear();
                        if (currentNode.IsLeaf)
                        {
                            // Leaf node: count its retention value
                            if (currentNode != null)
                            {
                                currentNode.AttributeDict[currentNode.Retention.TypeId] = 1;
                            }
                        }
                        else
                        {
                            // Internal node: aggregate children's counts
                            foreach (var child in currentNode.Children)
                            {
                                foreach (var kvp in child.AttributeDict)
                                {
                                    currentNode.AttributeDict[kvp.Key] =
                                        currentNode.AttributeDict.GetValueOrDefault(kvp.Key, 0) + kvp.Value;
                                }
                            }
                            // InnerNode doesn't have its own retention value to add
                        }
                        if (currentNode != null) //get rid of the warning
                            processedFolders.Add(currentNode.Id);
                    }
                }
                else
                {
                    // Pre-order processing: mark for post-order and add children
                    stack.Push((currentNode, true));
                    // Add children in reverse order so they're processed in correct order (only for InnerNode)
                    if (!currentNode.IsLeaf)
                    {
                        for (int i = currentNode.Children.Count - 1; i >= 0; i--)
                        {
                            stack.Push((currentNode.Children[i], false));
                        }
                    }
                }
                // Yield control periodically for large trees to prevent UI blocking
                if (stack.Count % 100 == 0)
                {
                    await Task.Yield();
                }
            }
        }
        public async Task SetParentFolderLink()
        {
            // Set the Parent property to the immediate parent for each node in the hierarchy (iterative, no recursion)
            var stack = new Stack<(FolderNode node, FolderNode? parent)>();
            stack.Push((this, null)); // root_folder's parent is null

            while (stack.Count > 0)
            {
                var (currentNode, parentNode) = stack.Pop();
                currentNode.Parent = parentNode;

                foreach (var child in currentNode.Children)
                {
                    stack.Push((child, currentNode));
                }

                // Yield control periodically for large trees to prevent UI blocking
                if (stack.Count % 100 == 0)
                {
                    await Task.Yield();
                }
            }
        }
    }
}