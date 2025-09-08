using VSM.Client.SharedAPI;
namespace VSM.Client.Datamodel
{
    public class Library
    {
        // Private static variable that holds the single instance
        private static readonly Lazy<Library> _instance = new Lazy<Library>(() => new Library());
        // Prevent instantiation from outside
        private Library() { }
        // Public static property to access the instance
        public static Library Instance => _instance.Value;
        public RootFolder? SelectedRootFolder { get; set; }
        public string User { get; set; } = "";
        public List<RootFolder> UsersRootFolders { get; set; } = new List<RootFolder>();
        public List<string> CleanupFrequencies = new List<string> { "inactive", "1 week", "2 weeks", "3 weeks", "4 weeks", "6 weeks" };
        private static byte _current_id = 0;
        public byte NewID
        {
            get => _current_id++;
        }
        public async Task Load()
        {
            if (User == null || User.Length == 0)
            {
                UsersRootFolders = [];
            }
            else
            {
                List<RootFolderDTO> rootFolderDTOs = await API.Instance.LoadUserRootFolders(User);
                UsersRootFolders = rootFolderDTOs.Select(dto => new RootFolder(dto)).ToList();
            }
        }
    }
}