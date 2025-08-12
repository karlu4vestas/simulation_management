namespace VSM.Client.Datamodel
{

    public class DataModel
    {
        // Private static variable that holds the single instance
        private static readonly Lazy<DataModel> _instance = new Lazy<DataModel>(() => new DataModel());

        // Prevent instantiation from outside
        private DataModel() { }

        // Public static property to access the instance
        public static DataModel Instance => _instance.Value;
        public List<string> RetentionOptions { get; } =
        [
            "Cleaned",
            "MarkedForCleanup", 
            "CleanupIssue",    
            "New",
            "+1Next",
            "+Q1",
            "+Q3",
            "+Q6",
            "+1Y",
            "+2Y",
            "+3Y",
            "longterm",
            "path protected"            
        ];
        private RootFolder? selected_root_folder;

        public User? User { get; set; }

        public void SetSelectedRootFolder(RootFolder rootFolder)
        {
            selected_root_folder = rootFolder;
        }
        public RootFolder? GetSelectedRootFolder()
        {
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
                rootFolders.AddRange(TestDataGenerator.GenTestRootFoldersForUser(User));
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

    }
}