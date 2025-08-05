namespace VSM.Client.Datamodel
{

    class DataModel
    {
        // Private static variable that holds the single instance
        private static readonly Lazy<DataModel> _instance = new Lazy<DataModel>(() => new DataModel());

        // Prevent instantiation from outside
        private DataModel() { }

        // Public static property to access the instance
        public static DataModel Instance => _instance.Value;

        private RootFolder? selected_root_folder;

        public User? User { get; set; }

        public void SetRootFolder(RootFolder rootFolder)
        {
            selected_root_folder = rootFolder;
        }
        public RootFolder? GetSelectedRootFolder()
        {
            if( selected_root_folder!=null && selected_root_folder.FolderTree==null )
                selected_root_folder.FolderTree = DataModel.Instance.GetRootFolderTree(selected_root_folder);
            return selected_root_folder;
        }

        private static readonly List<RootFolder> rootFolders = [];
        public List<RootFolder> GetRootFoldersForUser()
        {
            // this is a placeholder for the real logic to get the root folders for a user.
            // in a real application, this would query a database or an API.
            if (User is null)
            {
                rootFolders.Clear();
                return rootFolders;
            }

            if (rootFolders.Count == 0)
            {

                int rootId = 1;
                rootFolders.Add(
                    new RootFolder
                    {
                        Id = rootId,
                        Is_registeredfor_cleanup = true,
                        Users = [User, new("jajac"), new("misve")],
                        Root_path = "\\\\domain.net\\root_1"
                    });

                rootFolders.Add(
                    new RootFolder
                    {
                        Id = ++rootId,
                        Is_registeredfor_cleanup = true,
                        Users = [User, new("stefw"), new("misve")],
                        Root_path = "\\\\domain.net\\root_2"
                    });

                rootFolders.Add(
                    new RootFolder
                    {
                        Id = rootId,
                        Users = [User, new("facap"), new("misve")],
                        Root_path = "\\\\domain.net\\root_3"

                    });

                rootFolders.Add(
                    new RootFolder
                    {
                        Id = ++rootId,
                        Users = [User, new("caemh"), new("arlem")],
                        Root_path = "\\\\domain.net\\root_4"
                    });

            }
            return rootFolders;
        }


        public bool RegisterRootFolder(RootFolder rootFolder)
        {
            if (User is null)
            {
                return false;
            }
            rootFolder.Is_registeredfor_cleanup = true;

            return true;
        }

        class RandomRetention
        {
            private Random rand_int_generator;
            List<string> Titles = new List<string>(["Review", "Path", "LongTerm", "_2025_Q4", "_2026_Q1", "_2026_Q2"]);
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
        private Folder? GetRootFolderTree(RootFolder root_folder)
        {

            RandomRetention retention_generator = new RandomRetention(0);

            if (root_folder == null) return null;
            else
            {
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
        }

    }

}