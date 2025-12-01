using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System;
using System.Net;
using VSM.Client.Datamodel;
using System.Reflection.Metadata.Ecma335; // This ensures all types in this namespace are accessible
using System.Text.Json.Serialization;

namespace VSM.Client.SharedAPI
{
    public class ApiException : Exception
    {
        public HttpStatusCode? StatusCode { get; }
        public ApiException(string message, HttpStatusCode? statusCode = null) : base(message)
        {
            StatusCode = statusCode;
        }
    }

    public class ApiService
    {
        protected readonly HttpClient HttpClient;
        private readonly JsonSerializerOptions _jsonOptions;

        public ApiService(HttpClient httpClient)
        {
            HttpClient = httpClient;
            _jsonOptions = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                UnmappedMemberHandling = JsonUnmappedMemberHandling.Skip 
            };
        }

        protected async Task<T> GetAsync<T>(string url, CancellationToken cancellationToken = default)
        {
            try
            {
                var response = await HttpClient.GetAsync(url, cancellationToken);
                if (!response.IsSuccessStatusCode)
                {
                    var error = await response.Content.ReadAsStringAsync();
                    throw new ApiException($"GET {url} failed. Status: {response.StatusCode}, Error: {error}", response.StatusCode);
                }

                var jsonResponse = await response.Content.ReadAsStringAsync();

                var result = JsonSerializer.Deserialize<T>(jsonResponse, _jsonOptions);
                if (result == null)
                {
                    Console.WriteLine($"Raw JSON response for GET {url}: {jsonResponse}");
                    throw new ApiException($"GET {url} returned null after deserialization");
                }

                return result;
            }
            catch (HttpRequestException ex)
            {
                throw new ApiException($"GET {url} failed: {ex.Message}");
            }
            catch (JsonException ex)
            {
                throw new ApiException($"GET {url} failed to deserialize JSON: {ex.Message}");
            }
        }

        protected async Task<T> PostAsync<T>(string url, object data, CancellationToken cancellationToken = default)
        {
            try
            {
                var json = JsonSerializer.Serialize(data, _jsonOptions);
                var response = await HttpClient.PostAsJsonAsync(url, data, _jsonOptions, cancellationToken);
                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"POST JSON for {url}: {json}");

                    var error = await response.Content.ReadAsStringAsync();
                    throw new ApiException($"POST {url} failed. Status: {response.StatusCode}, Error: {error}", response.StatusCode);
                }

                // read the response content as JSON and deserialize
                var result = await response.Content.ReadFromJsonAsync<T>(_jsonOptions, cancellationToken);
                if (result == null)
                    throw new ApiException($"POST {url} returned null");

                return result;
            }
            catch (HttpRequestException ex)
            {
                throw new ApiException($"POST {url} failed: {ex.Message}");
            }
        }

        protected async Task<bool> DeleteAsync(string url, CancellationToken cancellationToken = default)
        {
            try
            {
                var response = await HttpClient.DeleteAsync(url, cancellationToken);
                if (!response.IsSuccessStatusCode)
                {
                    var error = await response.Content.ReadAsStringAsync();
                    throw new ApiException($"DELETE {url} failed. Status: {response.StatusCode}, Error: {error}", response.StatusCode);
                }
                return true;
            }
            catch (HttpRequestException ex)
            {
                throw new ApiException($"DELETE {url} failed: {ex.Message}");
            }
        }
    }


    public class API : ApiService
    {
        public API( HttpClient httpClient ) : base(httpClient) { }

        public async Task<List<SimulationDomainDTO>> GetSimulationDomainsAsync(CancellationToken ct = default)
        {
            List<SimulationDomainDTO> domains = await GetAsync<List<SimulationDomainDTO>>("/v1/simulationdomains/", ct);
            return domains;
        }

        public async Task<SimulationDomainDTO?> GetSimulationDomainByNameAsync(string domainName, CancellationToken ct = default) =>
            await GetAsync<SimulationDomainDTO>($"/v1/simulationdomains/{Uri.EscapeDataString(domainName)}", ct);

        public async Task<List<LeadTimeDTO>> GetLeadTimesAsync(int simulationDomainId, CancellationToken ct = default) =>
            await GetAsync<List<LeadTimeDTO>>($"/v1/simulationdomains/{simulationDomainId}/leadtimes/", ct);
        public async Task<List<CleanupFrequencyDTO>> GetCleanupFrequenciesAsync(int simulationDomainId, CancellationToken ct = default) =>
            await GetAsync<List<CleanupFrequencyDTO>>($"/v1/simulationdomains/{simulationDomainId}/cleanupfrequencies/", ct);

        public async Task<CleanupConfigurationDTO> GetCleanupConfigurationByRootFolderIdAsync(int rootFolderId, CancellationToken ct = default) {
            CleanupConfigurationDTO dto = await GetAsync<CleanupConfigurationDTO>($"/v1/rootfolders/{rootFolderId}/cleanup_configuration", ct);
            return dto;
        }
        public async Task<CleanupConfigurationDTO?> UpdateCleanupConfigurationForRootFolderAsync(int rootFolderId, CleanupConfigurationDTO config, CancellationToken ct = default) {
            CleanupConfigurationDTO dto = await PostAsync<CleanupConfigurationDTO>($"/v1/rootfolders/{rootFolderId}/cleanup_configuration", config, ct);
            return dto;
        }

        public async Task<List<PathProtectionDTO>?> GetPathProtectionsByRootFolderIdAsync(int rootFolderId, CancellationToken ct = default) =>
            await GetAsync<List<PathProtectionDTO>?>($"/v1/rootfolders/{rootFolderId}/pathprotections", ct);
        public async Task<PathProtectionDTO> AddPathProtectionByRootFolderAsync(PathProtectionDTO pathProtection, CancellationToken ct = default) {
            PathProtectionDTO dto = await PostAsync< PathProtectionDTO>($"/v1/rootfolders/{pathProtection.RootfolderId}/pathprotection", pathProtection, ct);
            return dto;
        }   
        public async Task<bool> DeletePathProtectionByRootFolderAndPathProtectionAsync(int rootFolderId, int pathProtectionId, CancellationToken ct = default) =>
            await DeleteAsync($"/v1/rootfolders/{rootFolderId}/pathprotection?protection_id={pathProtectionId}", ct);

        public async Task<List<RetentionTypeDTO>> GetRootfolderRetentionTypesAsync(int rootfolder_Id, CancellationToken ct = default) {
            List<RetentionTypeDTO> all_retentions = await GetAsync<List<RetentionTypeDTO>>($"/v1/rootfolders/{rootfolder_Id}/retentiontypes", ct) ?? new List<RetentionTypeDTO>();
            return all_retentions;
        }


        public async Task<List<RootFolderDTO>> GetRootFoldersByDomainUserAsync(int simulationdomain_id, string user, CancellationToken ct = default) =>
            await GetAsync<List<RootFolderDTO>>($"/v1/rootfolders/?simulationdomain_id={simulationdomain_id}&initials={Uri.EscapeDataString(user)}", ct);
        public async Task<List<FolderRetention>> UpdateRootFolderRetentionsAsync(int rootFolderId, List<FolderRetention> retentionUpdates, CancellationToken ct = default){
            List<FolderRetention> dtos = await PostAsync<List<FolderRetention>>($"/v1/rootfolders/{rootFolderId}/retentions", retentionUpdates, ct);
            return dtos;
        }

        public async Task<List<FolderNodeDTO>> GetFoldersByRootFolderIdAsync(int rootfolder_Id, CancellationToken ct = default) {
            List<FolderNodeDTO> folders = await GetAsync<List<FolderNodeDTO>>($"/v1/rootfolders/{rootfolder_Id}/folders", ct);
            return folders;
        }

    }
}
