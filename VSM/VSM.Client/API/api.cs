using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using VSM.Client.Datamodel; // This ensures all types in this namespace are accessible

namespace VSM.Client.SharedAPI
{
    public class API
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private API() { }
        private static readonly Lazy<API> _instance = new Lazy<API>(() => new API());
        public static API Instance => _instance.Value;

        public async Task<RetentionConfigurationDTO> GetRetentionTypesFromApiAsync()
        {
            List<RetentionTypeDTO> all_retentions = await httpClient.GetFromJsonAsync<List<RetentionTypeDTO>>("http://127.0.0.1:5173/retentiontypes/", new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            }) ?? new List<RetentionTypeDTO>();

            return new RetentionConfigurationDTO(all_retentions);
        }
        //                List<PathProtectionDTO> pathProtectionDTOs = await httpClient.GetFromJsonAsync<List<PathProtectionDTO>>("http://127.0.0.1:5173/pathprotections/?rootfolder_id={rootFolder.Id}", new JsonSerializerOptions

        public async Task<List<FolderTypeDTO>> GetFolderTypesFromApiAsync()
        {
            try
            {
                return await httpClient.GetFromJsonAsync<List<FolderTypeDTO>>("http://127.0.0.1:5173/foldertypes/", new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                }) ?? new List<FolderTypeDTO>();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching retention types: {ex.Message}");
                return new List<FolderTypeDTO>();
            }
        }

        public async Task<List<RootFolderDTO>> LoadTheUsersRootFolders(string user)
        {
            try
            {
                string endpoint = $"http://127.0.0.1:5173/rootfolders/?initials={user}";
                List<RootFolderDTO> rootFolderDTOs = await httpClient.GetFromJsonAsync<List<RootFolderDTO>>(endpoint, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                }) ?? new List<RootFolderDTO>();
                return rootFolderDTOs;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching root folders: {ex.Message}");
                return new List<RootFolderDTO>();
            }
        }
        public async Task<List<FolderNodeDTO>> GetFoldersByRootFolderIdAsync(RootFolder rootFolder)
        {
            try
            {
                string requestUrl = $"http://127.0.0.1:5173/folders/?rootfolder_id={rootFolder.Id}";
                List<FolderNodeDTO> base_folders = await httpClient.GetFromJsonAsync<List<FolderNodeDTO>>(requestUrl, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                }) ?? new List<FolderNodeDTO>();

                return base_folders;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching root folders: {ex.Message}");
                return new List<FolderNodeDTO>();
            }
        }
    }
}