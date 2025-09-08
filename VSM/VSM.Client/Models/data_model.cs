using VSM.Client.SharedAPI;
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
        public RootFolder? SelectedRootFolder { get; set; }
        public string User { get; set; } = "";
        public List<RootFolder> UsersRootFolders { get; set; } = new List<RootFolder>();
        private static byte _current_id = 0;
        public byte NewID
        {
            get => _current_id++;
        }
        public async Task LoadUsersRootFolders()
        {
            if (User == null || User.Length == 0)
            {
                UsersRootFolders = [];
            }
            else
            {
                List<RootFolderDTO> rootFolderDTOs = await API.Instance.LoadTheUsersRootFolders(User);
                UsersRootFolders = rootFolderDTOs.Select(dto => new RootFolder(dto)).ToList();
            }
        }
    }
}