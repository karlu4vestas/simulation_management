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

        public async Task<List<SimulationDomainDTO>> GetSimulationDomains()
        {
            List<SimulationDomainDTO> all_domains = await httpClient.GetFromJsonAsync<List<SimulationDomainDTO>>("http://127.0.0.1:5173/v1/simulationdomains/", new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            }) ?? new List<SimulationDomainDTO>();

            return all_domains;
        }
        /// <summary>
        /// call fastapi @app.get("/v1/simulationdomains/{domain_name}", response_model=SimulationDomainDTO)
        ///def read_simulation_domain_by_name(domain_name: str):
        /// </summary>
        public async Task<SimulationDomainDTO?> GetSimulationDomainByName(string domain_name)
        {
            SimulationDomainDTO? domain = await httpClient.GetFromJsonAsync<SimulationDomainDTO>($"http://127.0.0.1:5173/v1/simulationdomains/{Uri.EscapeDataString(domain_name)}", new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
            });
            return domain;
        }

        public async Task<RetentionTypesDTO> GetRootfolderRetentionTypes(RootFolder rootfolder)
        {
            List<RetentionTypeDTO> all_retentions = await httpClient.GetFromJsonAsync<List<RetentionTypeDTO>>($"http://127.0.0.1:5173/v1/rootfolders/{rootfolder.Id}/retentiontypes", new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
            }) ?? new List<RetentionTypeDTO>();

            return new RetentionTypesDTO(all_retentions);
        }
        public async Task<List<CleanupFrequencyDTO>> GetCleanupFrequencies(int simulationDomainId)
        {
            List<CleanupFrequencyDTO> all_frequencies = await httpClient.GetFromJsonAsync<List<CleanupFrequencyDTO>>($"http://127.0.0.1:5173/v1/simulationdomains/{simulationDomainId}/cleanupfrequencies/", new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
            }) ?? new List<CleanupFrequencyDTO>();

            return all_frequencies;
        }
        public async Task<List<LeadTimeDTO>> GetCycleTimes(int simulationDomainId)
        {
            List<LeadTimeDTO> all_cycle_times = await httpClient.GetFromJsonAsync<List<LeadTimeDTO>>($"http://127.0.0.1:5173/v1/simulationdomains/{simulationDomainId}/leadtimes/", new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
            }) ?? new List<LeadTimeDTO>();

            return all_cycle_times;
        }
        public async Task<List<RootFolderDTO>> RootFoldersByDomainUser(int simulationdomain_id, string user)
        {
            string requestUrl = $"http://127.0.0.1:5173/v1/rootfolders/?simulationdomain_id={simulationdomain_id}&initials={Uri.EscapeDataString(user)}";

            List<RootFolderDTO> rootFolderDTOs = await httpClient.GetFromJsonAsync<List<RootFolderDTO>>(requestUrl, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                //PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
            }) ?? new List<RootFolderDTO>();

            return rootFolderDTOs;
        }
        public async Task<List<FolderNodeDTO>> GetFoldersByRootFolderId(RootFolder rootFolder)
        {
            try
            {
                string requestUrl = $"http://127.0.0.1:5173/v1/rootfolders/{rootFolder.Id}/folders";
                List<FolderNodeDTO> base_folders = await httpClient.GetFromJsonAsync<List<FolderNodeDTO>>(requestUrl, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true,
                    PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
                }) ?? new List<FolderNodeDTO>();

                return base_folders;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching root folders: {ex.Message}");
                return new List<FolderNodeDTO>();
            }
        }
        public async Task<bool> UpdateCleanupConfigurationForRootFolder(int rootFolderId, CleanupConfigurationDTO cleanup_configuration)
        {
            try
            {
                var updateData = new { frequency = cleanup_configuration.Frequency, leadtime = cleanup_configuration.LeadTime };
                var response = await httpClient.PostAsJsonAsync($"http://127.0.0.1:5173/v1/rootfolders/{rootFolderId}/cleanup_configuration", updateData, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true,
                    PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
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
            List<PathProtectionDTO> pathProtections = await httpClient.GetFromJsonAsync<List<PathProtectionDTO>>($"http://127.0.0.1:5173/v1/rootfolders/{rootFolderId}/pathprotections", new JsonSerializerOptions
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
                var response = await httpClient.PostAsJsonAsync($"http://127.0.0.1:5173/v1/rootfolders/{pathProtection.Rootfolder_Id}/pathprotection", pathProtection, jsonOptions);

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
                else if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Path protection already exists or conflicts with existing data: {errorContent}");
                    return null;
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Failed to add path protection. Status: {response.StatusCode}, Error: {errorContent}");
                }

                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error adding path protection: {ex.Message}");
                return null;
            }
        }
        public async Task<bool> DeletePathProtectionByRootFolderAndPathProtection(int rootFolderId, int pathProtectionId)
        {
            try
            {
                var response = await httpClient.DeleteAsync($"http://127.0.0.1:5173/v1/rootfolders/{rootFolderId}/pathprotection?protection_id={pathProtectionId}");

                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error deleting path protection: {ex.Message}");
                return false;
            }
        }

        public async Task<bool> UpdateRootFolderRetentions(int rootFolderId, List<RetentionUpdateDTO> retentionUpdates)
        {
            try
            {
                var response = await httpClient.PostAsJsonAsync($"http://127.0.0.1:5173/v1/rootfolders/{rootFolderId}/retentions", retentionUpdates, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true,
                    PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
                });

                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error updating folder retentions: {ex.Message}");
                return false;
            }
        }
    }
}