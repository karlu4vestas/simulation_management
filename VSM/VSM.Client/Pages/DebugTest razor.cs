using System.Diagnostics;
using VSM.Client.SharedAPI;
using VSM.Client.Datamodel;
namespace VSM.Client.Pages
{
    public partial class DebugTest
    {
        public class SharedParameters
        {
            public string initials = "";
            public string domain_name = "vts";
            public SimulationDomainDTO? vts_simulationDomain = null;
            public RootFolderDTO? rootfolder = null;
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

            protected EndpointTest()
            {
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
            protected override async Task<List<SimulationDomainDTO>> GetDataAsync()
            {
                return await API.Instance.GetSimulationDomains();
            }
            public override bool Enabled { get { return true; } }
            protected override string FormatResult(List<SimulationDomainDTO> result) => result.Count > 0 ? $"domains retrieved: {string.Join(", ", result.Select(d => d.Name))}" : "No domains retrieved";
        }
        public class VtsSimulationdomainsTest : EndpointTest<SimulationDomainDTO?>
        {
            protected override async Task<SimulationDomainDTO?> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Missing domain name for simulation domain.");
                else
                {
                    shared_params.vts_simulationDomain = await API.Instance.GetSimulationDomainByName(shared_params.domain_name);
                    return shared_params.vts_simulationDomain;
                }
            }
            public override bool Enabled { get { return shared_params.domain_name.Count() > 0; } }
            protected override string FormatResult(SimulationDomainDTO? result) => result != null ? $"domain retrieved: {result.Name}" : "No domain retrieved";
        }
        public class RootFoldersByDomainUserTest : EndpointTest<List<RootFolderDTO>>
        {
            protected override async Task<List<RootFolderDTO>> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Simulation domain not set. Run SimulationdomainsTest first.");
                else
                {
                    List<RootFolderDTO> rootFolders = shared_params.vts_simulationDomain == null ? new List<RootFolderDTO>() : await API.Instance.RootFoldersByDomainUser(shared_params.vts_simulationDomain.Id, shared_params.initials);
                    shared_params.rootfolder = rootFolders.Count > 0 ? rootFolders.First() : null;

                    return rootFolders;
                }
            }
            public override bool Enabled { get { return shared_params.vts_simulationDomain != null && shared_params.initials.Count() > 0; } }
            protected override string FormatResult(List<RootFolderDTO> result) => $"Number of root folders retrieved: {result.Count}";
        }
        public class CleanupFrequenciesTest : EndpointTest<List<CleanupFrequencyDTO>>
        {
            protected override async Task<List<CleanupFrequencyDTO>> GetDataAsync()
            {
                if (shared_params.vts_simulationDomain == null)
                    throw new Exception("Simulation domain not set. Run SimulationdomainsTest first.");
                return await API.Instance.GetCleanupFrequencies(shared_params.vts_simulationDomain.Id);
            }
            public override bool Enabled { get { return shared_params.vts_simulationDomain != null; } }
            protected override string FormatResult(List<CleanupFrequencyDTO> result) => $"Number of cleanup frequencies retrieved: {result.Count}";
        }
        public class CycleTimesTest : EndpointTest<List<LeadTimeDTO>>
        {
            protected override async Task<List<LeadTimeDTO>> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Simulation domain not set. Run SimulationdomainsTest first.");
                return shared_params.vts_simulationDomain != null ? await API.Instance.GetCycleTimes(shared_params.vts_simulationDomain.Id) : new List<LeadTimeDTO>();
            }
            public override bool Enabled { get { return shared_params.vts_simulationDomain != null && shared_params.vts_simulationDomain.Id > 0; } }
            protected override string FormatResult(List<LeadTimeDTO> result) => $"Number of cycle times retrieved: {result.Count}";
        }

        public class RetentionTypesTest : EndpointTest<RetentionTypesDTO>
        {
            protected override async Task<RetentionTypesDTO> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Root folder not set. Run RootFolderTest first.");
                return shared_params.rootfolder != null ? await API.Instance.GetRootfolderRetentionTypes(shared_params.rootfolder.Id) : new RetentionTypesDTO();
            }
            public override bool Enabled { get { return shared_params.rootfolder != null; } }
            protected override string FormatResult(RetentionTypesDTO result) => $"Number of retention types retrieved: {result.All_retentions.Count}";
        }


        public class CleanupConfigurationTest : EndpointTest<CleanupConfigurationDTO?>
        {
            protected override async Task<CleanupConfigurationDTO?> GetDataAsync()
            {
                return shared_params.rootfolder == null ? new CleanupConfigurationDTO() : await API.Instance.GetCleanupConfigurationByRootFolderId(shared_params.rootfolder.Id);
            }
            public override bool Enabled { get { return shared_params.rootfolder != null; } }

            protected override string FormatResult(CleanupConfigurationDTO? result) => result != null ? $"Cleanup configurations. LeadTime {result.Lead_time} days, Frequency {result.Frequency} days" : "No cleanup configuration retrieved";
        }
        public class GetFoldersByRootFolderIdTest : EndpointTest<List<FolderNodeDTO>>
        {
            protected override async Task<List<FolderNodeDTO>> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Root folder not set. Run RootFolderTest first.");
                return shared_params.rootfolder == null ? new List<FolderNodeDTO>() : await API.Instance.GetFoldersByRootFolderId(shared_params.rootfolder.Id);
            }
            public override bool Enabled { get { return shared_params.rootfolder != null; } }
            protected override string FormatResult(List<FolderNodeDTO> result) => $"Number of folders retrieved: {result.Count}";
        }
        public class GetPathProtectionsByRootFolderIdTest : EndpointTest<List<PathProtectionDTO>>
        {
            protected override async Task<List<PathProtectionDTO>> GetDataAsync()
            {
                if (!Enabled)
                    throw new Exception("Root folder not set. Run RootFolderTest first.");
                return shared_params.rootfolder == null ? new List<PathProtectionDTO>() : await API.Instance.GetPathProtectionsByRootFolderId(shared_params.rootfolder.Id);
            }
            public override bool Enabled { get { return shared_params.rootfolder != null; } }
            protected override string FormatResult(List<PathProtectionDTO> result) => $"Number of path protections retrieved: {result.Count}";
        }

        private string message = "Click the button to test breakpoint";
        private static SharedParameters sharedtestParameters = new SharedParameters();

        List<IEndpointTest> endpoint_tests = new List<IEndpointTest>();

        protected override void OnInitialized()
        {
            // Initialize the registry with shared parameters
            EndpointTestRegistry.Initialize(sharedtestParameters);

            // Create tests without constructor parameters
            endpoint_tests = new List<IEndpointTest> {
            new SimulationdomainsTest(),
            new VtsSimulationdomainsTest(),
            new CleanupFrequenciesTest(),
            new CycleTimesTest(),
            new RootFoldersByDomainUserTest(),
            new RetentionTypesTest(),
            new CleanupConfigurationTest()
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
    }
}
