namespace VSM.Client.Datamodel
{
    public class RootFolder
    {
        private InnerNode? _folderTree;
        public int Id { get; set; }
        public bool Is_registeredfor_cleanup { get; set; } = false;
        public string Root_path { get; set; } = ""; // the folder to scan if we do not know it already
        public InnerNode? FolderTree
        {
            get
            {
                if (_folderTree == null)
                    _folderTree = TestDataGenerator.GetRootFolderTree(this);
                return _folderTree;
            }
            set => _folderTree = value;
        }
        public List<User> Users { get; set; } = [];
        public List<string> RetentionHeaders => FolderTree == null ? [] : FolderTree.AttributDict.Keys.ToList();
    }

    //  ------------------------------ Test data generation ------------------------------
    public static class TestDataGenerator
    {
        class RandomRetention
        {
            private Random rand_int_generator;
            List<string> Titles = DataModel.Instance.RetentionOptions;
            public RandomRetention(int seed)
            {
                rand_int_generator = new Random(seed);
            }
            public string Next()
            {
                return Titles[rand_int_generator.Next(0, Titles.Count)];
            }
        }
        public class BooleanGenerator
{
    Random rnd;

    public BooleanGenerator()
    {
        rnd = new Random();
    }

    public bool NextBoolean()
    {
        return rnd.Next(0, 2) == 1;
    }
}

        //return the folder hierarchy that match the path to the rootfolder.
        public static InnerNode? GetRootFolderTree(RootFolder root_folder)
        {
            if (root_folder == null)
                return null;

            RandomRetention retention_generator = new RandomRetention(0);
            int idCounter = 1;
            InnerNode the_root_folder = new()
            {
                Id = idCounter,
                ParentId = idCounter,
                Name = root_folder.Root_path,
                IsExpanded = true,
                Level = 0,
            };

            idCounter = the_root_folder.Id;


            // Generate n_levels=10 levels in the folder tree with InnerNode (can have children) and LeafNode (no children)
            // generate up to n_children=4 at each level
            // at any level chose randomly between generating an InnerNodes or a LeafNode 
            // ------ to be replaced start ---------
            Random random = new Random(42); // Use fixed seed for reproducible results
            GenerateTreeRecursively(the_root_folder, ref idCounter, retention_generator, new BooleanGenerator(), random, maxLevel: 10);
            // ------ to be replaced end ---------
            return the_root_folder;
        }

        private static void GenerateTreeRecursively(InnerNode parent, ref int idCounter, RandomRetention retentionGenerator, BooleanGenerator boolGen, Random random, int maxLevel)
        {
            // Stop if we've reached the maximum level
            if (parent.Level >= maxLevel)
                return;

            // Randomly decide how many children this node should have (0-9)
            int numberOfChildren = random.Next(0, 10); // 0 to 9 children

            for (int i = 0; i < numberOfChildren; i++)
            {
                var childId = ++idCounter;
                var childLevel = parent.Level + 1;
                
                // Randomly decide if this should be an InnerNode (can have children) or a LeafNode (terminal)
                // Higher probability of InnerNode at shallow levels, higher probability of LeafNode at deep levels
                bool shouldBeLeafNode = childLevel >= maxLevel || boolGen.NextBoolean();

                TreeNode child;
                
                if (shouldBeLeafNode || childLevel >= maxLevel)
                {
                    // Create a LeafNode with retention value
                    child = new LeafNode
                    {
                        Id = childId,
                        ParentId = parent.Id,
                        Name = $"SimData_{childLevel}_{i + 1}",
                        Level = childLevel,
                        Retention = retentionGenerator.Next()
                    };
                }
                else
                {
                    // Create an InnerNode that can have children
                    child = new InnerNode
                    {
                        Id = childId,
                        ParentId = parent.Id,
                        Name = $"Folder_{childLevel}_{i + 1}",
                        Level = childLevel
                    };
                }

                parent.Children.Add(child);

                // If this is an InnerNode and we haven't reached max depth, recursively generate its children
                if (child is InnerNode innerNode && childLevel < maxLevel)
                {
                    GenerateTreeRecursively(innerNode, ref idCounter, retentionGenerator, boolGen, random, maxLevel);
                }
            }
        }

        public static List<RootFolder> GenTestRootFoldersForUser( User user)
        {
            List<RootFolder> rootFolders = new List<RootFolder>();
            int rootId = 1;
            rootFolders.Add(
                new RootFolder
                {
                    Id = rootId,
                    Is_registeredfor_cleanup = true,
                    Users = [user, new User("jajac"), new User("misve")],
                    Root_path = "\\\\domain.net\\root_1"
                });

            rootFolders.Add(
                new RootFolder
                {
                    Id = ++rootId,
                    Is_registeredfor_cleanup = true,
                    Users = [user, new User("stefw"), new User("misve")],
                    Root_path = "\\\\domain.net\\root_2"
                });

            rootFolders.Add(
                new RootFolder
                {
                    Id = rootId,
                    Users = [user, new User("facap"), new User("misve")],
                    Root_path = "\\\\domain.net\\root_3"

                });

            rootFolders.Add(
                new RootFolder
                {
                    Id = ++rootId,
                    Users = [user, new User("caemh"), new User("arlem")],
                    Root_path = "\\\\domain.net\\root_4"
                });

            return rootFolders;
        }
    }

}