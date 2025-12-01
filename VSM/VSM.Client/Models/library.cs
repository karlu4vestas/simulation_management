using VSM.Client.SharedAPI;
namespace VSM.Client.Datamodel
{
    public class Library
    {
        protected API Api { get; }
        public Library(API api)
        {   
            Api = api;
        }
        // Destructor to print when the instance is finalized
        ~Library() {
        }
        public string User { get; set; } = "";
        // just hard code for now until we have more than one domain and know more about how to mix and match domains
        private SimulationDomainDTO? Domain { get; set; }
        public List<RootFolder> RootFolders { get; set; } = new List<RootFolder>();
        public List<CleanupFrequencyDTO> CleanupFrequencies { get; set; } = new List<CleanupFrequencyDTO>();
        public List<LeadTimeDTO> CycleTimes { get; set; } = new List<LeadTimeDTO>();
        public async Task Load()
        {
            Domain = await Api.GetSimulationDomainByNameAsync("vts");
            if (User == null || User.Length == 0 || Domain == null)
            {
                RootFolders = [];
            }
            else
            {
                CleanupFrequencies = await Api.GetCleanupFrequenciesAsync(Domain.Id);
                CycleTimes = await Api.GetLeadTimesAsync(Domain.Id);
                List<RootFolderDTO> rootFolderDTOs = await Api.GetRootFoldersByDomainUserAsync(Domain.Id, User);

                RootFolders = new List<RootFolder>();
                foreach (var dto in rootFolderDTOs)
                {
                    CleanupConfigurationDTO? cleanupConfig = await Api.GetCleanupConfigurationByRootFolderIdAsync(dto.Id);
                    var rootFolder = new RootFolder(Api, dto, cleanupConfig ?? new CleanupConfigurationDTO());
                    RootFolders.Add(rootFolder);
                }
            }
        }
    }
}