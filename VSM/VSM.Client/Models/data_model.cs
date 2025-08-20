using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;

// Add reference to the DTOs file to make RootFolderDTO accessible
using VSM.Client.Datamodel; // This ensures all types in this namespace are accessible

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

        private static readonly HttpClient httpClient = new HttpClient();
        private List<RetentionType>? _retentionOptions;
        private List<FolderType>? _foldertypes;
        private List<RootFolder>? _the_users_rootFolders;
        public RootFolder? SelectedRootFolder { get; set; }
        public string? User { get; set; }
        public async Task<List<RetentionType>> GetRetentionOptionsAsync()
        {
            return _retentionOptions ??= await GetRetentionTypesFromApiAsync();
        }
        private async Task<List<RetentionType>> GetRetentionTypesFromApiAsync()
        {
            try
            {
                return await httpClient.GetFromJsonAsync<List<RetentionType>>("http://127.0.0.1:5173/retentiontypes/", new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                }) ?? new List<RetentionType>();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching retention types: {ex.Message}");
                return new List<RetentionType>();
            }
        }
        public async Task<List<FolderType>> GetFolderTypesAsync()
        {
            return _foldertypes ??= await GetFolderTypesFromApiAsync();
        }
        private async Task<List<FolderType>> GetFolderTypesFromApiAsync()
        {
            try
            {
                return await httpClient.GetFromJsonAsync<List<FolderType>>("http://127.0.0.1:5173/foldertypes/", new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                }) ?? new List<FolderType>();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching retention types: {ex.Message}");
                return new List<FolderType>();
            }
        }
        public async Task<List<RootFolder>> GetTheUsersRootFolders()
        {
            if (User is null)
            {
                _the_users_rootFolders = new List<RootFolder>();
            }
            else
            {
                _the_users_rootFolders = await GetRootFoldersFromApiAsync(User);
            }
            return _the_users_rootFolders;
        }
        private async Task<List<RootFolder>> GetRootFoldersFromApiAsync(string? user = null)
        {
            try
            {
                string endpoint = user != null ? $"http://127.0.0.1:5173/rootfolders/?initials={user}" : "http://127.0.0.1:5173/rootfolders/";
                List<RootFolderDTO> rootFolderDTOs = await httpClient.GetFromJsonAsync<List<RootFolderDTO>>(endpoint, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                }) ?? new List<RootFolderDTO>();

                List<RootFolder> rootFolders = rootFolderDTOs.Select(dto => new RootFolder(dto)).ToList();
                return rootFolders;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching root folders: {ex.Message}");
                return new List<RootFolder>();
            }
        }

        /*public async Task<List<FolderNode>> GetFoldersByRootFolderIdAsync(int rootFolderId)
        {
            try
            {
                List<FolderNodeDTO> base_folders = await httpClient.GetFromJsonAsync<List<FolderNodeDTO>>($"http://127.0.0.1:5173/folders/?rootfolder_id={rootFolderId}", new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                }) ?? new List<FolderNodeDTO>();

                List<FolderNode> folder_nodes = base_folders.Select(dto => new FolderNode(dto)).ToList();
                return folder_nodes;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching root folders: {ex.Message}");
                return new List<FolderNode>();
            }
        }*/

        public async Task<List<FolderNode>> GetFoldersByRootFolderIdAsync(int rootFolderId)
        {
            try
            {
                // Get the raw JSON response first
                string requestUrl = $"http://127.0.0.1:5173/folders/?rootfolder_id={rootFolderId}";
                HttpResponseMessage response = await httpClient.GetAsync(requestUrl);
                response.EnsureSuccessStatusCode();

                string jsonContent = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"Raw JSON response: {jsonContent}");

                // Then attempt to deserialize
                List<FolderNodeDTO> base_folders = JsonSerializer.Deserialize<List<FolderNodeDTO>>(jsonContent, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                }) ?? new List<FolderNodeDTO>();

                List<FolderNode> folder_nodes = base_folders.Select(dto => new FolderNode(dto)).ToList();
                return folder_nodes;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching root folders: {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                }
                return new List<FolderNode>();
            }
        }
        //@todo create a fastAPI endpoint to register that the user wants to run cleanup for this folder 
        public bool RegisterRootFolderForCleanUp(RootFolder rootFolder)
        {
            if (User is null)
            {
                return false;
            }
            rootFolder.Is_registeredfor_cleanup = true;

            return true;
        }
        //@todo create a fastAPI endpoint to register change in retentions for the simulations
    }
}