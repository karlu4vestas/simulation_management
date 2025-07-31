namespace VSM.Client.Datamodel
{
    public abstract class TreeNode
    {
        public int Id { get; set; }
        public int? ParentId { get; set; }
        public string Name { get; set; } = "";
        public bool IsExpanded { get; set; } = false;
        //public bool IsVisible { get; set; } = false;
        public int Level { get; set; } = 0;
        //public bool IsChildrenLoaded { get; set; } = false;
        public List<Folder> Children { get; set; } = new List<Folder>();
        public AttributeRow Attributs { get; set; } = new();
        public Dictionary<string, int> AttributDict { get; set; } = new();
    }

    // retention row calculated from the retention in the folder hierarchy 
    public class AttributeRow
    {
        public int Id { get; set; } // treenode id
        public string Review { get; set; } = "";
        public string Path { get; set; } = "";
        public string LongTerm { get; set; } = "";
        public string _2025_Q4 { get; set; } = "";
        public string _2026_Q1 { get; set; } = "";
        public string _2026_Q2 { get; set; } = "";
        public static AttributeRow GenerateAttributeRow(int id)
        {
            var rand = new Random(id);
            return new AttributeRow
            {
                Id = id,
                Review = rand.Next(1000, 9999).ToString(),
                Path = "path",
                LongTerm = rand.NextDouble() > 0.5 ? "Yes" : "No",
                _2025_Q4 = rand.Next(0, 100).ToString(),
                _2026_Q1 = rand.Next(0, 100).ToString(),
                _2026_Q2 = rand.Next(0, 100).ToString()
            };
        }
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
    /*public class AttributeRow2
    {
        public int Id { get; set; } // treenode id

        public Dictionary<string, int> Retentions { get; set; } = new Dictionary<string, int>
        public List<string> Titles { get; set; } = new List<string>();
        public List<string> Values { get; set; } = new List<string>();
        public static AttributeRow2 GenerateAttributeRow(int id)
        {
            var rand = new Random(id);
            AttributeRow2 r2 = new AttributeRow2
            {
                Id = id,
                Titles = new List<string>(["Review", "Path", "LongTerm", "_2025_Q4", "_2026_Q1", "_2026_Q2"]),
                Values = new List<string>([
                    rand.Next(1000, 9999).ToString(),
                    "path",
                    rand.NextDouble() > 0.5 ? "Yes" : "No",
                    rand.Next(0, 100).ToString(),
                    rand.Next(0, 100).ToString(),
                    rand.Next(0, 100).ToString(),
                ]),
            };
            return r2;
        }
    }*/

    public class Folder : TreeNode
    {
        //public string Retention { get; set; } = "";
    }
}