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
        public Dictionary<string, int> AttributDict { get; set; } = new();
    }
}