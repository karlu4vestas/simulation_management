using System.Net.Http;
using System.Text.Json;

namespace VSM.Client.Datamodel
{
    public class RetentionType {
        public int id { get; set; } = 0;
        public string name { get; set; } = "";
        public bool is_system_managed { get; set; } = false;
        public int display_rank { get; set; } = 0;
    }
    
    public class DataModel
{
    // Private static variable that holds the single instance
    private static readonly Lazy<DataModel> _instance = new Lazy<DataModel>(() => new DataModel());

    // Prevent instantiation from outside
    private DataModel() { }

    // Public static property to access the instance
    public static DataModel Instance => _instance.Value;

    private static readonly HttpClient httpClient = new HttpClient();
    private List<RetentionType>? _retentionOptions;

    public async Task<List<RetentionType>> GetRetentionOptionsAsync()
    {
        if (_retentionOptions == null)
        {
            try
            {
                var response = await httpClient.GetAsync("http://127.0.0.1:5173/retentiontypes/");
                response.EnsureSuccessStatusCode();
                var jsonString = await response.Content.ReadAsStringAsync();
                _retentionOptions = JsonSerializer.Deserialize<List<RetentionType>>(jsonString, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                }) ?? new List<RetentionType>();
            }
            catch (Exception ex)
            {
                // Log error and return empty list as fallback
                Console.WriteLine($"Error fetching retention types: {ex.Message}");
                _retentionOptions = new List<RetentionType>();
            }
        }
        return _retentionOptions;
    }


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