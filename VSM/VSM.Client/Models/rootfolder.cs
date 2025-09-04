using VSM.Client.SharedAPI;
namespace VSM.Client.Datamodel
{
    //-------------------- here come client side objects that contains a DTO like object ----------------
    public class RootFolder
    {
        RootFolderDTO dto;
        public RootFolder(RootFolderDTO dto)
        {
            this.dto = dto;
        }
        public int Id => dto.Id;
        public int Folder_Id => dto.Folder_Id;
        public string Cleanup_frequency
        {
            get => dto.Cleanup_frequency;
            set => dto.Cleanup_frequency = value;
        }
        public string Root_path => dto.Path;
        public string Owner => dto.Owner;
        public string Approvers => dto.Approvers;
        public List<string> AllInitials => new List<string> { Owner }.Concat(Approvers.Split(',')).ToList();
        private FolderNode? _folderTree;
        private RetentionConfiguration retentionConfiguration = new RetentionConfiguration(new RetentionConfigurationDTO());
        private List<FolderType>? _foldertypes;
        public async Task<RetentionConfiguration> GetRetentionOptionsAsync()
        {
            try
            {
                RetentionConfigurationDTO dto = await API.Instance.GetRetentionTypesFromApiAsync();
                retentionConfiguration = dto == null ? retentionConfiguration : new RetentionConfiguration(dto);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching retention types: {ex.Message}");
            }
            return retentionConfiguration;
        }
        public async Task<List<FolderType>> GetFolderTypesAsync()
        {
            if (_foldertypes == null)
            {
                List<FolderTypeDTO> foldertype_dto = await API.Instance.GetFolderTypesFromApiAsync();
                _foldertypes = foldertype_dto.Select(dto => (FolderType)dto).ToList();
            }
            return _foldertypes;
        }
        public async Task<FolderNode?> GetFolderTreeAsync()
        {
            if (_folderTree == null)
            {
                List<FolderNodeDTO> dto_folders = await API.Instance.GetFoldersByRootFolderIdAsync(this);
                _folderTree = await ConstructFolderTreeFromNodes(this, dto_folders);
                if (_folderTree == null)
                {
                    Console.WriteLine($"Error: No folder tree found for root folder with ID {Id}");
                }
            }
            return _folderTree;
        }

        public async Task UpdateAggregation()
        {
            if (_folderTree != null)
            {
                await _folderTree.UpdateAggregation();
            }
        }
        private static async Task<FolderNode> ConstructFolderTreeFromNodes(RootFolder rootFolder, List<FolderNodeDTO> dto_nodes)
        {
            // create a map for fast lookup of all FolderNodes
            Dictionary<int, FolderNode> nodeLookup = new Dictionary<int, FolderNode>();
            foreach (var dto in dto_nodes)
            {
                FolderNode node = new FolderNode(dto);
                nodeLookup[node.Id] = node;
            }
            // Build the tree structure
            foreach (var dto in dto_nodes)
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

    }
}