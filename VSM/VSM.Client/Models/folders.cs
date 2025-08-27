namespace VSM.Client.Datamodel
{

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

        public async Task ChangeRetentions(byte from_retention_Id, byte to_retention_Id)
        {
            //change retention for all leaf nodes in this inner node without recursion
            var stack = new Stack<FolderNode>();
            stack.Push(this);

            while (stack.Count > 0)
            {
                var currentNode = stack.Pop();

                if (currentNode.IsLeaf)
                {
                    if (currentNode.Retention_Id == from_retention_Id)
                    {
                        // Found a leaf node - update its retention
                        currentNode.Retention_Id = to_retention_Id;
                    }
                }
                else
                {
                    // Inner node - add all children to stack for processing
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