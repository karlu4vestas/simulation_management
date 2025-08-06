using Folder = VSM.Client.Datamodel.TreeNode;

namespace VSM.Client.Datamodel
{
    public abstract class TreeNode
    {
        public int Id { get; set; }
        public int? ParentId { get; set; }
        public string Name { get; set; } = "";
        public bool IsExpanded { get; set; } = false;
        public int Level { get; set; } = 0;
        public Dictionary<string, int> AttributDict { get; set; } = new();
    }

    public class LeafNode : TreeNode
    {
        public string Retention { get; set; } = "";
    }

    public class InnerNode : TreeNode
    {
        private readonly List<TreeNode> _children = [];
        
        public List<TreeNode> Children => _children;
        
        public async Task ChangeRetention(string new_retention)
        {
            //find all Simulation folders under this folder and change their retention 
            //Retention = new_retention;

            // The update the aggregations from the root folder and down
            // 1) find the root 
            // 2) call UpdateAggregation on the root folder
        
            // Alternatively let the caller do this
            

            // Use Task.Run to offload the computation to a background thread
            // if UpdateAggregation is CPU-intensive
            await Task.Run(async () => await UpdateAggregation());
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
            var stack = new Stack<(TreeNode node, bool visited)>();
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

                        if (currentNode is LeafNode leafNode)
                        {
                            // Leaf node: count its retention value
                            if (!string.IsNullOrEmpty(leafNode.Retention))
                            {
                                currentNode.AttributDict[leafNode.Retention] = 1;
                            }
                        }
                        else if (currentNode is InnerNode innerNode)
                        {
                            // Internal node: aggregate children's counts
                            foreach (var child in innerNode.Children)
                            {
                                foreach (var kvp in child.AttributDict)
                                {
                                    currentNode.AttributDict[kvp.Key] =
                                        currentNode.AttributDict.GetValueOrDefault(kvp.Key, 0) + kvp.Value;
                                }
                            }
                            // InnerNode doesn't have its own retention value to add
                        }

                        processedFolders.Add(currentNode.Id);
                    }
                }
                else
                {
                    // Pre-order processing: mark for post-order and add children
                    stack.Push((currentNode, true));

                    // Add children in reverse order so they're processed in correct order (only for InnerNode)
                    if (currentNode is InnerNode innerNode)
                    {
                        for (int i = innerNode.Children.Count - 1; i >= 0; i--)
                        {
                            stack.Push((innerNode.Children[i], false));
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