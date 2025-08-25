using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.FluentUI.AspNetCore.Components.DesignTokens;


// Add reference to the DTOs file to make RootFolderDTO accessible
using VSM.Client.Datamodel; // This ensures all types in this namespace are accessible

namespace VSM.Client.Datamodel
{
    public class RetentionConfigurationDTO
    {
        public RetentionConfigurationDTO(List<RetentionType>? all_retentions = null)
        {
            if (all_retentions != null)
            {
                this.All_retentions = all_retentions;
                this.Path_retention = all_retentions
                        .FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains("path", StringComparison.OrdinalIgnoreCase));

                if (Path_retention == null)
                    throw new InvalidOperationException("No suitable path retention option found");

                this.Cleaned_retention = all_retentions
                    .FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains("cleaned", StringComparison.OrdinalIgnoreCase));
                if (this.Cleaned_retention == null)
                    throw new InvalidOperationException("No suitable cleaned retention option found");

                this.Issue_retention = all_retentions
                    .FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains("issue", StringComparison.OrdinalIgnoreCase));
                if (Issue_retention == null)
                    throw new InvalidOperationException("No suitable issue retention option found");

                //the list of dropdown retentions is equal to retentionOptions except for the cleaned retention value
                this.Target_retentions = all_retentions
                                        .Where(r => r.Id != this.Cleaned_retention.Id && !r.Name.Contains("issue", StringComparison.OrdinalIgnoreCase)).ToList();
            }
        }
        public List<RetentionType> All_retentions = new();
        public List<RetentionType> Target_retentions = new();
        public RetentionType Path_retention = new();
        public RetentionType Cleaned_retention = new();
        public RetentionType Issue_retention = new();
    }

    public class RetentionConfiguration
    {
        RetentionConfigurationDTO dto;
        public RetentionConfiguration(RetentionConfigurationDTO dto)
        {
            this.dto = dto;
        }
        public List<RetentionType> All_retentions => dto.All_retentions;
        public List<RetentionType> Target_retentions => dto.Target_retentions;
        public RetentionType Path_retention => dto.Path_retention;
        public RetentionType Cleaned_retention => dto.Cleaned_retention;
        public RetentionType Issue_retention => dto.Issue_retention;
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
        private List<FolderType>? _foldertypes;
        public RootFolder? SelectedRootFolder { get; set; }
        public string User { get; set; } = "";
        private RetentionConfiguration retentionConfiguration = new RetentionConfiguration(new RetentionConfigurationDTO());
        public async Task<RetentionConfiguration> GetRetentionOptionsAsync()
        {
            List<RetentionType> all_retentions = await DataModel.Instance.GetRetentionTypesFromApiAsync();
            if (all_retentions != null)
            {
                retentionConfiguration = new RetentionConfiguration(new RetentionConfigurationDTO(all_retentions));
            }
            else
            {
                throw new InvalidOperationException("failed to retrieve retention");

            }
            return retentionConfiguration;
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
        public List<RootFolder> TheUsersRootFolders { get; set; } = new List<RootFolder>();
        public async Task LoadTheUsersRootFolders()
        {
            if (User == null || User.Length == 0)
            {
                TheUsersRootFolders = [];
            }
            else
            {
                try
                {
                    string endpoint = $"http://127.0.0.1:5173/rootfolders/?initials={User}";
                    List<RootFolderDTO> rootFolderDTOs = await httpClient.GetFromJsonAsync<List<RootFolderDTO>>(endpoint, new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    }) ?? new List<RootFolderDTO>();

                    List<RootFolder> rootFolders = rootFolderDTOs.Select(dto => new RootFolder(dto)).ToList();
                    TheUsersRootFolders = rootFolders;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error fetching root folders: {ex.Message}");
                    TheUsersRootFolders = [];
                }
            }
        }
        public async Task<FolderNode> GetFoldersByRootFolderIdAsync(RootFolder rootFolder)
        {
            try
            {
                string requestUrl = $"http://127.0.0.1:5173/folders/?rootfolder_id={rootFolder.Id}";
                List<FolderNodeDTO> base_folders = await httpClient.GetFromJsonAsync<List<FolderNodeDTO>>(requestUrl, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true
                }) ?? new List<FolderNodeDTO>();

                List<FolderNode> folder_nodes = base_folders.Select(dto => new FolderNode(dto)).ToList();
                FolderNode root = await ConstructFolderTreeFromNodes(rootFolder, base_folders);
                return root;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching root folders: {ex.Message}");
                return new FolderNode(new FolderNodeDTO());
            }
        }
        async Task<FolderNode> ConstructFolderTreeFromNodes(RootFolder rootFolder, List<FolderNodeDTO> folderNodeDTOs)
        {
            // create a map for fast lookup of all FolderNodes
            Dictionary<int, FolderNode> nodeLookup = new Dictionary<int, FolderNode>();
            foreach (var dto in folderNodeDTOs)
            {
                FolderNode node = new FolderNode(dto);
                nodeLookup[node.Id] = node;
            }

            // Build the tree structure
            foreach (var dto in folderNodeDTOs)
            {
                if (dto.Parent_Id == 0)
                {
                    // This is the root node
                    continue;
                }
                else if (nodeLookup.TryGetValue(dto.Parent_Id, out var parentNode))
                {
                    parentNode.Children.Add(nodeLookup[dto.Id]);
                }
            }

            FolderNode root = nodeLookup[rootFolder.Folder_Id];
            await root.SetParentFolderAsync();
            //print_folder_leaf_levels(root, 0);
            return root;
        }
        void print_folder_leaf_levels(FolderNode folderNode, int level)
        {
            // used for debugging only
            if (folderNode.IsLeaf)
                Console.WriteLine($"leaf_level:{level}- {folderNode.Name} (ID: {folderNode.Id})");
            foreach (var child in folderNode.Children)
            {
                print_folder_leaf_levels(child, level + 1);
            }
        }
        //@todo create a fastAPI endpoint to register that the user wants to run cleanup for this folder 
        public bool RegisterRootFolderForCleanUp(RootFolder rootFolder)
        {
            if (User is null || User.Length == 0)
            {
                return false;
            }
            rootFolder.Is_registeredfor_cleanup = true;

            return true;
        }
        //@todo create a fastAPI endpoint to register change in retentions for the simulations
    }
}