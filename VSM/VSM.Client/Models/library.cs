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
        // just hard code for now until we have more than one domain and know more about how to mix and match domains
        private SimulationDomainDTO? Domain { get; set; }
        public List<RootFolder> UsersRootFolders { get; set; } = new List<RootFolder>();
        public List<CleanupFrequencyDTO> CleanupFrequencies { get; set; } = new List<CleanupFrequencyDTO>();
        public List<LeadTimeDTO> CycleTimes { get; set; } = new List<LeadTimeDTO>();
        private static byte _current_id = 0;
        public byte NewID
        {
            get => _current_id++;
        }
        public async Task Load()
        {
            Domain = await API.Instance.GetSimulationDomainByName("vts");
            if (User == null || User.Length == 0 || Domain == null)
            {
                UsersRootFolders = [];
            }
            else
            {
                CleanupFrequencies = await API.Instance.GetCleanupFrequencies(Domain.Id);
                CycleTimes = await API.Instance.GetCycleTimes(Domain.Id);
                List<RootFolderDTO> rootFolderDTOs = await API.Instance.RootFoldersByDomainUser(Domain.Id, User);
                UsersRootFolders = [.. rootFolderDTOs.Select(dto => new RootFolder(dto))];
            }
        }
    }
}