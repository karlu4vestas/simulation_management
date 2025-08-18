using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;

namespace VSM.Client.Datamodel
{
    public class User
    {
        public User(string initials)
        {
            Initials = initials;
        }
        public string Initials { get; set; }
    }

    public class RetentionType
    {
        public int Id { get; set; } = 0;
        public string Name { get; set; } = "";
        public bool Is_System_Managed { get; set; } = false;
        public int Display_Rank { get; set; } = 0;
    }

    public class FolderType
    {
        public int Id { get; set; } = 0;
        public string Name { get; set; } = "";
    }
    public class RootFolderDTO
    {
        //mapped
        public int Id { get; set; } //ID of this DTO
        public string Path { get; set; } = ""; // like /parent/folder. parent would most often be a domain url
        public uint Folder_Id { get; set; } //Id to folder' FolderNodeDTO. unit24 would be sufficient
        public string Owner { get; set; } = ""; // the initials of the owner
        public string Approvers { get; set; } = ""; // the initials of the approvers (co-owners)
        public bool Active_Cleanup { get; set; } // indicates if the folder is actively being cleaned up


    }

    public class RootFolder
    {
        public RootFolder(RootFolderDTO dto)
        {
            Id = dto.Id;
            Root_path = dto.Path;
            Owner = dto.Owner;
            Approvers = dto.Approvers;
            Is_registeredfor_cleanup = dto.Active_Cleanup;
        }

        private FolderNode? _folderTree;
        public int Id { get; set; }
        public bool Is_registeredfor_cleanup { get; set; } = false;
        public string Root_path { get; set; } = "";

        public string Owner { get; set; } = "";

        public string Approvers { get; set; } = "";

        Task<List<FolderNode>>? _folderTreeTask;
        public async Task<FolderNode?> GetFolderTreeAsync()
        {
            if (_folderTree == null)
            {
                _folderTreeTask = DataModel.Instance.GetFolderTreeFromApiAsync();

                var result = await _folderTreeTask;

                if (result != null)
                {
                    FolderNode rootFolderNode = new FolderNode(new FolderBaseNode());
                    rootFolderNode.Children = result;
                    _folderTree = rootFolderNode;
                }
            }
            return _folderTree;
        }

        /*// Async method to get folder tree
        public async Task<List<FolderNode>?> GetFolderTreeAsync()
        {
            if (_folderTree == null)
                _folderTreeTask = DataModel.Instance.GetFolderTreeFromApiAsync();

            return await _folderTreeTask;
        }*/

        // Check if tree generation is in progress
        public bool IsLoadingFolderTree => _folderTreeTask != null && !_folderTreeTask.IsCompleted;

public List<User> Users { get; set; } = [];

public async Task UpdateAggregation()
{
    if (_folderTree != null)
    {
        await _folderTree.UpdateAggregation();
    }
}
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
    private List<FolderType>? _foldertypes;
    private List<RootFolder>? _rootFolders;
    private List<RootFolder>? _the_users_rootFolders;
    public RootFolder? SelectedRootFolder { get; set; }
    public User? User { get; set; }
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
    public async Task<List<RootFolder>> GetRootFoldersAsync(User user=null)
    {

        return _rootFolders ??= await GetRootFoldersFromApiAsync(user);
    }
    private async Task<List<RootFolder>> GetRootFoldersFromApiAsync(User user = null)
    {
        try
        {
            string endpoint = user != null? $"http://127.0.0.1:5173/rootfolders/?initials={user.Initials}" : "http://127.0.0.1:5173/rootfolders/";
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

    public async Task<List<FolderNode>> GetFolderTreeFromApiAsync()
    {
        try
        {
            List<FolderBaseNode> base_folders = await httpClient.GetFromJsonAsync<List<FolderBaseNode>>("http://127.0.0.1:5173/folders/", new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            }) ?? new List<FolderBaseNode>();

            List<FolderNode> folder_nodes = base_folders.Select(dto => new FolderNode(dto)).ToList();
            return folder_nodes;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error fetching root folders: {ex.Message}");
            return new List<FolderNode>();
        }
    }
    public async Task<List<RootFolder>> GetTheUsersRootFolders()
    {
        if (User is null)
        {
            return new List<RootFolder>();
        }
        else
        {
            var allRootFolders = await GetRootFoldersAsync(User);
            _the_users_rootFolders = allRootFolders.Where(rf =>
                rf.Owner == User.Initials ||
                rf.Approvers.Contains(User.Initials)).ToList();
        }
        return _the_users_rootFolders;
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