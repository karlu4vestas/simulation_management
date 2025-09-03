namespace VSM.Client.Datamodel
{

    /// <summary>
    /// we use ChangeRetentionDelegate in order to use the same iterator over a subtree when we need to change the retentype and the PathRetention_Id
    /// The delegate is responsible for determining which nodes should have their retention settings updated. and it also containts the target retentype and pathretention_id
    /// </summary>
    public abstract class ChangeRetentionDelegate
    {
        public abstract bool update_retention(FolderNode node);
        public int to_retentiontype_Id;
        public int to_path_protection_id;
    }

    /// <summary>
    /// This delegate allows for overwrite of all retentions except the those that have the same retentiontype af the to_retentiontype_Id
    /// It is meant for adding path retentions
    /// </summary>
    public class AddPathProtectionDelegate : ChangeRetentionDelegate
    {
        int from_path_protection_id;
        // constructor when we change all but same type of retentions
        public AddPathProtectionDelegate(int to_retentiontype_Id, int to_path_protection_id)
        {
            this.from_path_protection_id = 0;
            this.to_retentiontype_Id = to_retentiontype_Id;
            this.to_path_protection_id = to_path_protection_id;
        }
        //constructor when we only change retention of same type and same path protection
        public AddPathProtectionDelegate(int from_path_protection_id, int to_retentiontype_Id, int to_path_protection_id)
        {
            this.from_path_protection_id = from_path_protection_id;
            this.to_retentiontype_Id = to_retentiontype_Id;
            this.to_path_protection_id = to_path_protection_id;
        }
        public override bool update_retention(FolderNode node)
        {
            // overwrite: 
            // all retentions except for our own type (to_retentiontype_Id). 
            // for our own type then only replace those with the same from_path_protection_id
            return node.Retention_Id == to_retentiontype_Id ? node.Path_Protection_Id == from_path_protection_id : node.Retention_Id != to_retentiontype_Id;
        }
    }
    /// <summary>
    /// only change retention with full match of retentiontype and pathretention_id
    /// </summary>
    public class ChangeOnFullmatchDelegate : ChangeRetentionDelegate
    {
        public int from_retentiontype_Id;
        public int from_path_protection_id;
        //case for when to and from pathretention_id are 0
        public ChangeOnFullmatchDelegate(int from_retentiontype_Id, int to_retentiontype_Id)
        {
            this.from_retentiontype_Id = from_retentiontype_Id;
            this.from_path_protection_id = 0;
            this.to_retentiontype_Id = to_retentiontype_Id;
            this.to_path_protection_id = 0;
        }
        // case for when all values can be different and must be matched
        public ChangeOnFullmatchDelegate(int from_retentiontype_Id, int from_path_protection_id, int to_retentiontype_Id, int to_path_protection_id)
        {
            this.from_retentiontype_Id = from_retentiontype_Id;
            this.from_path_protection_id = from_path_protection_id;
            this.to_retentiontype_Id = to_retentiontype_Id;
            this.to_path_protection_id = to_path_protection_id;
        }

        public override bool update_retention(FolderNode node)
        {
            return node.Retention_Id == from_retentiontype_Id && node.Path_Protection_Id == from_path_protection_id;
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
        public int Retention_Id { get => dto.Retention_Id; set => dto.Retention_Id = value; }
        public int Path_Protection_Id { get => dto.Path_Protection_Id; set => dto.Path_Protection_Id = value; }
        //@todo client side help fields
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
        public Dictionary<int, int> AttributDict { get; set; } = new();
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

        public async Task ChangeRetentions(byte from_retentiontype_Id, byte to_retentiontype_Id)
        {
            await ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(from_retentiontype_Id, to_retentiontype_Id));
        }

        public async Task<PathProtectionDTO> AddPathProtection(RetentionConfiguration retention_config)
        {
            // Should handle adding if there is no path protections and 
            // adding to existing path protections
            //   - siblings 
            //   - parent to one or more path protections at lower level
            //   - child
            PathProtectionDTO? parent_protection = FindClosestPathProtectedParent(retention_config);

            PathProtectionDTO new_path_protection = new PathProtectionDTO
            {
                //Id = pathProtection.Id, // Id will be set by the server
                Id = DataModel.Instance.NewID,  //untill we use persistance to get an ID
                Rootfolder_Id = this.Rootfolder_Id,
                Folder_Id = this.Id,
                Path = this.FullPath
            };
            retention_config.Path_protections.Add(new_path_protection);

            int path_retentiontype_id = retention_config.Path_retentiontype.Id;
            if (parent_protection != null)
                //add a sub pathprotection to a parent pathprotection    
                await ChangeRetentionsOfSubtree(new AddPathProtectionDelegate(parent_protection.Id, path_retentiontype_id, new_path_protection.Id));
            else
            {
                //add path retention but do not overwrite other path retentions
                await ChangeRetentionsOfSubtree(new AddPathProtectionDelegate(path_retentiontype_id, new_path_protection.Id));
            }
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
            PathProtectionDTO? from_path_retention = retention_config.Path_protections.FirstOrDefault(p => p.Folder_Id == this.Id);
            int remove_count = retention_config.Path_protections.RemoveAll(p => p.Folder_Id == this.Id);
            int path_retentiontype_id = retention_config.Path_retentiontype.Id;

            // check if any of the pathretentions from retention_config are a parent of from_path_retention_folder
            PathProtectionDTO? parent_path_protection = this.FindClosestPathProtectedParent(retention_config);
            if (from_path_retention == null || remove_count == 0)
                throw new ArgumentException("Invalid path protection folder specified.");
            else
            {
                if (parent_path_protection != null)
                    await ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(path_retentiontype_id, from_path_retention.Id, path_retentiontype_id, parent_path_protection.Id));
                else
                    await ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(path_retentiontype_id, from_path_retention.Id, to_retention_Id, 0));
            }
            return remove_count;
        }
        public async Task ChangeRetentionsOfSubtree(ChangeRetentionDelegate change_delegate)
        {
            //select the subtree to folder incl folder that have retention equal to  (from_retention_ID, from_path_protection_id)
            // and change the retentions to (to_retention_ID, to_path_protection_id)
            Console.WriteLine($"ChangeRetentionsOfSubtree: {this.FullPath} to_retention_ID: {change_delegate.to_retentiontype_Id}, to_path_protection_id: {change_delegate.to_path_protection_id} ");
            int number_of_change_leafs = 0;
            int number_of_unchanged_leafs = 0;
            var stack = new Stack<FolderNode>();
            stack.Push(this);
            while (stack.Count > 0)
            {
                var currentNode = stack.Pop();

                // Check if current node matches the criteria and update if so
                if (currentNode.IsLeaf)
                {
                    if (change_delegate.update_retention(currentNode))
                    {
                        currentNode.Retention_Id = change_delegate.to_retentiontype_Id;
                        currentNode.Path_Protection_Id = change_delegate.to_path_protection_id;
                        number_of_change_leafs++;
                    }
                    else
                    {
                        number_of_unchanged_leafs++;
                        //Console.WriteLine($"ChangeRetentionsOfSubtree do not change FullPath, Retention_Id, Path_Protection_Id:" +
                        //                   $" {currentNode.FullPath} {currentNode.Retention_Id} {currentNode.Path_Protection_Id}");
                    }
                }
                else
                {
                    // Add all children to stack for processing
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
                                  $" {this.FullPath} {this.Retention_Id} {this.Path_Protection_Id}");
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
            AttributDict.Clear();

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
                        currentNode.AttributDict.Clear();

                        if (currentNode.IsLeaf)
                        {
                            // Leaf node: count its retention value
                            if (currentNode != null)
                            {
                                currentNode.AttributDict[currentNode.Retention_Id] = 1;
                            }
                        }
                        else
                        {
                            // Internal node: aggregate children's counts
                            foreach (var child in currentNode.Children)
                            {
                                foreach (var kvp in child.AttributDict)
                                {
                                    currentNode.AttributDict[kvp.Key] =
                                        currentNode.AttributDict.GetValueOrDefault(kvp.Key, 0) + kvp.Value;
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
        public async Task SetParentFolderAsync()
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