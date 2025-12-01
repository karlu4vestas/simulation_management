using System.Diagnostics;
using VSM.Client.SharedAPI;
using VSM.Client.Datamodel;

namespace VSM.Client.Pages
{

    public partial class DebugTest
    {

        private string message = "Click the button to test breakpoint";
        private static SharedParameters sharedtestParameters = new SharedParameters();

        List<IEndpointTest> endpoint_tests = new List<IEndpointTest>();

        protected override void OnInitialized()
        {
            // Initialize the registry with shared parameters
            EndpointTestRegistry.Initialize(sharedtestParameters);

            // Create tests without constructor parameters
            endpoint_tests = new List<IEndpointTest> {
            new SimulationdomainsTest(Api),
            new VtsSimulationdomainsTest(Api),
            new CleanupFrequenciesTest(Api),
            new LeadTimesTest(Api),
            new RootFoldersByDomainUserTest(Api),
            new GetFoldersByRootFolderIdTest(Api),
            new GetPathProtectionsByRootFolderIdTest(Api),
            new RetentionTypesTest(Api),
            new GetCleanupConfigurationTest(Api),
            new PostCleanupConfigurationTest(Api)

        };
        }
        /* not update scenarios included yet    
            public async Task<bool> UpdateCleanupConfigurationForRootFolder(int rootFolderId, CleanupConfigurationDTO cleanup_configuration)
            public async Task<int?> AddPathProtectionByRootFolder(PathProtectionDTO pathProtection)
            public async Task<bool> DeletePathProtectionByRootFolderAndPathProtection(int rootFolderId, int pathProtectionId)
            public async Task<bool> UpdateRootFolderRetentions(int rootFolderId, List<RetentionUpdateDTO> retentionUpdates)
        */
        private void TestBreakpoint()
        {
            // Set a breakpoint on this line
            message = $"Debugger.IsAttached:{Debugger.IsAttached}";
        }

        public class SharedParameters
        {
            public string initials = "";
            public string domain_name = "vts";
            public SimulationDomainDTO? vts_simulationDomain = null;
            public RootFolderDTO? rootfolder = null;
            public CleanupConfigurationDTO? cleanup_config = null;
            public List<FolderNodeDTO>? folders = null;
            public List<PathProtectionDTO>? pathProtections = null; 
        }

        public static class EndpointTestRegistry
        {
            private static readonly Dictionary<string, IEndpointTest> _tests = new();
            private static SharedParameters? _sharedParameters;
            public static void Initialize(SharedParameters sharedParameters)
            {
                _sharedParameters = sharedParameters;
            }
            public static SharedParameters SharedParams => _sharedParameters ?? throw new InvalidOperationException("Registry not initialized");
            public static void Register(IEndpointTest test)
            {
                _tests[test.TestId] = test;
            }
            public static IEndpointTest? GetTest(string testId)
            {
                return _tests.TryGetValue(testId, out var test) ? test : null;
            }
            public static IEnumerable<IEndpointTest> GetAllTests()
            {
                return _tests.Values;
            }
        }

        public interface IEndpointTest
        {
            string TestId { get; }
            string TestName { get; }
            string TestMessage { get; }
            Task ActivateAsync();
            bool Enabled { get; }
        }

        public abstract class EndpointTest<T> : IEndpointTest
        {
            public string TestId { get; private set; }
            public string TestName { get; set; } = "test";
            public string TestMessage { get; set; } = "";

            protected SharedParameters shared_params => EndpointTestRegistry.SharedParams;
            protected API Api { get; }
            protected EndpointTest(API api)
            {
                Api = api;
                // Generate test ID and name from class name
                var className = GetType().Name;
                TestId = className;

                if (className.EndsWith("Test"))
                    className = className.Substring(0, className.Length - 4);

                TestName = System.Text.RegularExpressions.Regex.Replace(className, "([a-z])([A-Z])", "$1 $2") + " Test";

                // Register this test
                EndpointTestRegistry.Register(this);
            }
            public abstract bool Enabled { get; }

            public async Task ActivateAsync()
            {
                try
                {
                    var result = await GetDataAsync();
                    TestMessage = FormatResult(result);
                }
                catch (Exception ex)
                {
                    TestMessage = "Failed";
                    Console.WriteLine($"Error in {GetType().Name}: {ex.Message}");
                    Console.WriteLine($"Stack trace: {ex.StackTrace}");
                }
            }

            protected abstract Task<T> GetDataAsync();
            
            protected virtual string FormatResult(T result) => result?.ToString() ?? "No data";
        }

        public class SimulationdomainsTest : EndpointTest<List<SimulationDomainDTO>>
        {
            public SimulationdomainsTest(API api) : base(api) { }
            protected override async Task<List<SimulationDomainDTO>> GetDataAsync()
            {
                try
                {
                    var domains = await Api.GetSimulationDomainsAsync();
                    return domains;
                }
                catch (ApiException ex)
                {
                    Console.WriteLine($"Failed to load domains: {ex.Message}");
                    return new List<SimulationDomainDTO>();
                }
            }
            public override bool Enabled { get { return true; } }
            protected override string FormatResult(List<SimulationDomainDTO> result) => result.Count > 0 ? $"domains retrieved: {string.Join(", ", result.Select(d => d.Name))}" : "No domains retrieved";
        }
        public class VtsSimulationdomainsTest : EndpointTest<SimulationDomainDTO?>
        {
            public VtsSimulationdomainsTest(API api) : base(api) { }
            protected override async Task<SimulationDomainDTO?> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Missing domain name for simulation domain.");
                else
                {
                    shared_params.vts_simulationDomain = await Api.GetSimulationDomainByNameAsync(shared_params.domain_name);
                    return shared_params.vts_simulationDomain;
                }
            }
            public override bool Enabled { get { return shared_params.domain_name.Count() > 0; } }
            protected override string FormatResult(SimulationDomainDTO? result) => result != null ? $"domain retrieved: {result.Name}" : "No domain retrieved";
        }
        public class RootFoldersByDomainUserTest : EndpointTest<List<RootFolderDTO>>
        {
            public RootFoldersByDomainUserTest(API api) : base(api) { }
            protected override async Task<List<RootFolderDTO>> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Simulation domain not set. Run SimulationdomainsTest first.");
                else
                {
                    List<RootFolderDTO> rootFolders = shared_params.vts_simulationDomain == null ? new List<RootFolderDTO>() : await Api.GetRootFoldersByDomainUserAsync(shared_params.vts_simulationDomain.Id, shared_params.initials);
                    shared_params.rootfolder = rootFolders.Count > 0 ? rootFolders.First() : null;
                    return rootFolders;
                }
            }
            public override bool Enabled { get { return shared_params.vts_simulationDomain != null && shared_params.initials.Count() > 0; } }
            protected override string FormatResult(List<RootFolderDTO> result) => $"Number of root folders retrieved: {result.Count}";
        }
        public class CleanupFrequenciesTest : EndpointTest<List<CleanupFrequencyDTO>>
        {
            public CleanupFrequenciesTest(API api) : base(api) { }
            protected override async Task<List<CleanupFrequencyDTO>> GetDataAsync()
            {
                if (shared_params.vts_simulationDomain == null)
                    throw new Exception("Simulation domain not set. Run SimulationdomainsTest first.");
                return await Api.GetCleanupFrequenciesAsync(shared_params.vts_simulationDomain.Id);
            }
            public override bool Enabled { get { return shared_params.vts_simulationDomain != null; } }
            protected override string FormatResult(List<CleanupFrequencyDTO> result) => $"Number of cleanup frequencies retrieved: {result.Count}";
        }
        public class LeadTimesTest : EndpointTest<List<LeadTimeDTO>>
        {
            public LeadTimesTest(API api) : base(api) { }
            protected override async Task<List<LeadTimeDTO>> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Simulation domain not set. Run SimulationdomainsTest first.");
                return shared_params.vts_simulationDomain != null ? await Api.GetLeadTimesAsync(shared_params.vts_simulationDomain.Id) : new List<LeadTimeDTO>();
            }
            public override bool Enabled { get { return shared_params.vts_simulationDomain != null && shared_params.vts_simulationDomain.Id > 0; } }
            protected override string FormatResult(List<LeadTimeDTO> result) => $"Number of cycle times retrieved: {result.Count}";
        }

        public class RetentionTypesTest : EndpointTest<List<RetentionTypeDTO>>
        {
            public RetentionTypesTest(API api) : base(api) { }
            protected override async Task<List<RetentionTypeDTO>> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Root folder not set. Run RootFolderTest first.");
                return shared_params.rootfolder != null ? await Api.GetRootfolderRetentionTypesAsync(shared_params.rootfolder.Id) :  [];
            }
            public override bool Enabled { get { return shared_params.rootfolder != null; } }
            protected override string FormatResult(List<RetentionTypeDTO> result) => $"Number of retention types retrieved: {result.Count}";
        }


        public class GetCleanupConfigurationTest : EndpointTest<CleanupConfigurationDTO?>
        {
            public GetCleanupConfigurationTest(API api) : base(api) { }
            protected override async Task<CleanupConfigurationDTO?> GetDataAsync()
            {
                CleanupConfigurationDTO dto = shared_params.rootfolder == null ? new CleanupConfigurationDTO() : await Api.GetCleanupConfigurationByRootFolderIdAsync(shared_params.rootfolder.Id);
                shared_params.cleanup_config = dto;
                return dto;
            }
            public override bool Enabled { get { return shared_params.rootfolder != null; } }

            protected override string FormatResult(CleanupConfigurationDTO? result) => result != null ? $"Cleanup configurations. LeadTime {result.LeadTime} days, Frequency {result.Frequency} days" : "No cleanup configuration retrieved";
        }

        public class PostCleanupConfigurationTest : EndpointTest<CleanupConfigurationDTO?>
        {
            public PostCleanupConfigurationTest(API api) : base(api) { }
            protected override async Task<CleanupConfigurationDTO?> GetDataAsync()
            {
                if( shared_params.cleanup_config != null)
                    return  await Api.UpdateCleanupConfigurationForRootFolderAsync(shared_params.cleanup_config.RootfolderId, shared_params.cleanup_config);
                return null;
            }
            public override bool Enabled { get { return shared_params.cleanup_config != null; } }

            protected override string FormatResult(CleanupConfigurationDTO? result) => result != null ? $"Post Cleanup configurations. LeadTime {result.LeadTime} days, Frequency {result.Frequency} days" : "No cleanup configuration retrieved";
        }
        public class GetFoldersByRootFolderIdTest : EndpointTest<List<FolderNodeDTO>>
        {
            public GetFoldersByRootFolderIdTest(API api) : base(api) { }
            protected override async Task<List<FolderNodeDTO>> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Root folder not set. Run RootFolderTest first.");
                if( shared_params.rootfolder != null)    {
                    shared_params.folders =  await Api.GetFoldersByRootFolderIdAsync(shared_params.rootfolder.Id);
                    return shared_params.folders;
                }
                return new List<FolderNodeDTO>();
            }
            public override bool Enabled { get { return shared_params.rootfolder != null; } }
            protected override string FormatResult(List<FolderNodeDTO> result){
                int count_of_null_parent_ids = result.Where(f => f.ParentId == 0).Count();
                string str_result = $"count_of_null_parent_ids {count_of_null_parent_ids} Number of folders retrieved: {result.Count}";
                return str_result;
            }
        }
        public class GetPathProtectionsByRootFolderIdTest : EndpointTest<List<PathProtectionDTO>>
        {
            public GetPathProtectionsByRootFolderIdTest(API api) : base(api) { }
            protected override async Task<List<PathProtectionDTO>> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Root folder not set. Run RootFolderTest first.");

                if (shared_params.rootfolder == null)
                    return new List<PathProtectionDTO>();
                else {
                    List<PathProtectionDTO> dtos = await Api.GetPathProtectionsByRootFolderIdAsync(shared_params.rootfolder.Id) ?? new List<PathProtectionDTO>();
                    shared_params.pathProtections = dtos;
                    return dtos;
                }
            }
            public override bool Enabled { get { return shared_params.rootfolder != null; } }
            protected override string FormatResult(List<PathProtectionDTO> result) => $"Number of path protections retrieved: {result.Count}";
        }
        /*public class AddPathProtectionsByRootFolderIdTest : EndpointTest<PathProtectionDTO?>
        {
            public AddPathProtectionsByRootFolderIdTest(API api) : base(api) { }
            protected override async Task<PathProtectionDTO?> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Folder not set");

                if (shared_params.rootfolder!= null && shared_params.folders != null && shared_params.pathProtections != null){
                    //loop through folders untill we find on without path protection

                    foreach( FolderNodeDTO folder_dto in shared_params.folders){
                        bool has_protection = shared_params.pathProtections.Any( pp => pp.FolderId == folder_dto.Id );
                        if( !has_protection ){
                    FolderNode folder = new FolderNode( shared_params.folders.First() );

                    PathProtectionDTO new_path_protection = new PathProtectionDTO
                    {
                        RootfolderId = folder.RootfolderId,
                        FolderId = folder.Id,
                        Path = folder.FullPath,
                    };
                    PathProtectionDTO dto = await Api.AddPathProtectionByRootFolderAsync(new_path_protection);
                    return dto;
                }
                return null;
            }
            public override bool Enabled { get { return shared_params.folders != null && shared_params.pathProtections != null; } }
            protected override string FormatResult(PathProtectionDTO result) => $"Number of path protections retrieved: {result.Path}";
        }*/
    }
}
