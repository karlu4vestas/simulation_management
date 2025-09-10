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

        public async Task<RetentionTypesTO> GetRetentionTypes()
        {
            List<RetentionTypeDTO> all_retentions = await httpClient.GetFromJsonAsync<List<RetentionTypeDTO>>("http://127.0.0.1:5173/retentiontypes/", new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            }) ?? new List<RetentionTypeDTO>();

            return new RetentionTypesTO(all_retentions);
        }
        public async Task<List<string>> GetCleanupFrequencies()
        {
            List<string> all_frequencies = await httpClient.GetFromJsonAsync<List<string>>("http://127.0.0.1:5173/cleanup_frequencies/", new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            }) ?? new List<string>();

            return all_frequencies;
        }
        public async Task<List<RootFolderDTO>> LoadUserRootFolders(string user)
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
        public async Task<List<FolderNodeDTO>> GetFoldersByRootFolderId(RootFolder rootFolder)
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

        public async Task<bool> UpdateRootFolderCleanupFrequency(int rootFolderId, string cleanupFrequency)
        {
            try
            {
                var updateData = new { cleanup_frequency = cleanupFrequency };
                var response = await httpClient.PutAsJsonAsync($"http://127.0.0.1:5173/rootfolders/{rootFolderId}/cleanup-frequency", updateData, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                });

                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error updating root folder cleanup frequency: {ex.Message}");
                return false;
            }
        }

        public async Task<List<PathProtectionDTO>> GetPathProtectionsByRootFolderId(int rootFolderId)
        {
            List<PathProtectionDTO> pathProtections = await httpClient.GetFromJsonAsync<List<PathProtectionDTO>>($"http://127.0.0.1:5173/pathprotections/{rootFolderId}", new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            }) ?? new List<PathProtectionDTO>();
            return pathProtections;
        }
        public async Task<int?> AddPathProtectionByRootFolder(PathProtectionDTO pathProtection)
        {
            try
            {
                var jsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true, PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };
                var response = await httpClient.PostAsJsonAsync("http://127.0.0.1:5173/pathprotections", pathProtection, jsonOptions);

                if (response.IsSuccessStatusCode)
                {
                    var responseJson = await response.Content.ReadAsStringAsync();
                    //Console.WriteLine($"Response: {responseJson}");

                    using (JsonDocument document = JsonDocument.Parse(responseJson))
                    {
                        if (document.RootElement.TryGetProperty("id", out JsonElement idElement))
                        {
                            return idElement.GetInt32();
                        }
                    }
                }

                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error adding path protection: {ex.Message}");
                return null;
            }
        }

        public async Task<bool> DeletePathProtectionByRootFolder(int pathProtectionId)
        {
            try
            {
                var response = await httpClient.DeleteAsync($"http://127.0.0.1:5173/pathprotections/{pathProtectionId}");

                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error deleting path protection: {ex.Message}");
                return false;
            }
        }

        //@todo create a push method to update a folders retention values (retentiontype, pathprotection)

    }
}