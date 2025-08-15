namespace VSM.Client.Datamodel
{

    public class FolderBaseNode{
        public int Id { get; set; }
        public int ParentId { get; set; } = 0;   //Default to 0, indicating no parent
        public string Name { get; set; } = "";
        public int Type_Id { get; set; } = 0;
        public int Retention_Id { get; set; } = 0;

    }
    public class FolderNode
    {
        protected FolderBaseNode dto;

        public FolderNode(FolderBaseNode dto)
        {
            this.dto = dto;
        }
        
        //mapped to server fields
        public int Id { get => dto.Id; set => dto.Id = value; }
        public int ParentId { get => dto.ParentId; set => dto.ParentId = value; }
        public string Name { get => dto.Name; set => dto.Name = value; }
        public int Type_Id { get => dto.Type_Id; set => dto.Type_Id = value; }
        public int Retention_Id { get => dto.Retention_Id; set => dto.Retention_Id = value; }

        //@todo client side help fields for display and navigation
        public bool IsLeaf { get { return !Children.Any(); } }
        public Dictionary<int, int> AttributDict { get; set; } = new();
        public int Level { get; set; } = 0;
        public bool IsExpanded { get; set; } = false;
        public FolderNode? Parent { get; set; } = null; // Default to null, indicating no parent

        public List<FolderNode> Children { get; set; } = new();

        public void ChangeRetentions(RetentionType from, RetentionType to)
        {
            //@todo
            //change retention for all leaf nodes in this inner node without recursion
            var stack = new Stack<FolderNode>();
            stack.Push(this);

            while (stack.Count > 0)
            {
                var currentNode = stack.Pop();

                if (currentNode.IsLeaf) {
                    if (currentNode.Retention_Id == from.Id)
                    {
                        // Found a leaf node - update its retention
                        currentNode.Retention_Id = to.Id;
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
                            if (currentNode!=null)
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
                        if( currentNode!=null ) //get rid of the warning
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
    }
}