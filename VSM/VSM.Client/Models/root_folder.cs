namespace VSM.Client.Datamodel
{
    public class RootFolder
    {
        private Folder? _folderTree;
        public int Id { get; set; }
        public bool Is_registeredfor_cleanup { get; set; } = false;
        public string Root_path { get; set; } = ""; // the folder to scan if we do not know it already
        public Folder? FolderTree
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

        //return the folder hierarchy that match the path to the rootfolder.
        public static Folder GetRootFolderTree(RootFolder root_folder)
        {
            if (root_folder == null)
                return null;

            RandomRetention retention_generator = new RandomRetention(0);
            int idCounter = 1;
            Folder the_root_folder = new()
            {
                Id = idCounter,
                ParentId = idCounter,
                Name = root_folder.Root_path,
                IsExpanded = true,
                Level = 0,
                Retention = retention_generator.Next()
            };

            idCounter = the_root_folder.Id;

            //generate 10 children under the root folder
            for (int r = 1; r < 10; r++)
            {
                Folder parent = the_root_folder;

                //generate one child pr level
                for (int level = 1; level <= 3; level++)
                {
                    var childId = ++idCounter;
                    var child = new Folder
                    {
                        Id = childId,
                        ParentId = parent.Id,
                        Name = $"Node {r}.{level}",
                        //IsExpanded = true,
                        Level = level,
                        Retention = retention_generator.Next()
                    };

                    parent.Children.Add(child);

                    parent = child; // for the newt level 
                }
            }
            return the_root_folder;
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