namespace VSM.Client.Datamodel
{
    public abstract class TreeNode
    {
        public int Id { get; set; }
        public int? ParentId { get; set; }
        public string Name { get; set; } = "";
        public bool IsExpanded { get; set; } = false;       
        public int Level { get; set; } = 0;
        public List<Folder> Children { get; set; } = new List<Folder>();
        public bool HasChildren { get{ return Children.Count>0; } }
    }

    // retention row calculated from the retention in the folder hierarchy 
    public class AttributeRow
    {
        public int Id { get; set; }     // treenode id
        public static Dictionary<string, int> GenerateRetentionDict(int random_seed)
        {
            var rand = new Random(random_seed);
            Dictionary<string, int> retentions = new Dictionary<string, int>();
            List<string> Titles = new List<string>(["Review", "Path", "LongTerm", "_2025_Q4", "_2026_Q1", "_2026_Q2"]);

            foreach (var t in Titles)
            {
                retentions.Add(t, rand.Next(0, 100));
            }
            return retentions;
        }
    }

    public class Folder : TreeNode
    {
        public string Retention { get; set; } = "";

        /// <summary>
        /// Aggregates counts of different retention values by iterating the folder tree using depth-first traversal.
        /// The iteration starts from the leaves and aggregates up to the current level using post-order processing.
        /// Uses an iterative approach with a stack to avoid recursive function calls.
        /// After execution, each folder's AttributDict contains the total count of each retention type in its subtree.
        /// </summary>
        public void UpdateAggregation()
        {
            // Initialize the AttributDict for this folder
            AttributDict.Clear();
            
            // Use a stack for iterative depth-first traversal (avoiding recursion)
            var stack = new Stack<(Folder folder, bool visited)>();
            var processedFolders = new HashSet<int>();
            
            // Start with current folder
            stack.Push((this, false));
            
            while (stack.Count > 0)
            {
                var (currentFolder, visited) = stack.Pop();
                
                if (visited)
                {
                    // Post-order processing: aggregate children's values
                    if (!processedFolders.Contains(currentFolder.Id))
                    {
                        // Initialize this folder's AttributDict
                        currentFolder.AttributDict.Clear();
                        
                        if (currentFolder.Children.Count == 0)
                        {
                            // Leaf node: count its own retention value
                            if (!string.IsNullOrEmpty(currentFolder.Retention))
                            {
                                currentFolder.AttributDict[currentFolder.Retention] = 1;
                            }
                        }
                        else
                        {
                            // Internal node: aggregate children's counts
                            foreach (var child in currentFolder.Children)
                            {
                                foreach (var kvp in child.AttributDict)
                                {
                                    currentFolder.AttributDict[kvp.Key] = 
                                        currentFolder.AttributDict.GetValueOrDefault(kvp.Key, 0) + kvp.Value;
                                }
                            }
                            
                            // Add this folder's own retention if it has one
                            if (!string.IsNullOrEmpty(currentFolder.Retention))
                            {
                                currentFolder.AttributDict[currentFolder.Retention] = 
                                    currentFolder.AttributDict.GetValueOrDefault(currentFolder.Retention, 0) + 1;
                            }
                        }
                        
                        processedFolders.Add(currentFolder.Id);
                    }
                }
                else
                {
                    // Pre-order processing: mark for post-order and add children
                    stack.Push((currentFolder, true));
                    
                    // Add children in reverse order so they're processed in correct order
                    for (int i = currentFolder.Children.Count - 1; i >= 0; i--)
                    {
                        stack.Push((currentFolder.Children[i], false));
                    }
                }
            }
        }
        public Dictionary<string, int> AttributDict { get; set; } = new();
    }
}