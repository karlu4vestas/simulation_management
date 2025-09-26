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
        public CleanupConfigurationDTO CleanupConfiguration
        {
            get
            {
                return new CleanupConfigurationDTO
                {
                    CleanupFrequency = dto.CleanupFrequency,
                    CycleTime = dto.CycleTime
                };
            }
            set
            {
                dto.CleanupFrequency = value.CleanupFrequency;
                dto.CycleTime = value.CycleTime;
            }
        }

        public string Root_path => dto.Path;
        public string Owner => dto.Owner;
        public string Approvers => dto.Approvers;
        public List<string> AllInitials => new List<string> { Owner }.Concat(Approvers.Split(',')).ToList();
        // the content of the following property must be loaded on demand by calling LoadFolderRetentions()
        public FolderNode FolderTree { get; set; } = new FolderNode(new FolderNodeDTO());
        public RetentionTypes RetentionConfiguration { get; set; } = new RetentionTypes(new RetentionTypesDTO());
        public List<PathProtectionDTO> Path_protections { get; set; } = new();

        public PathProtectionDTO? Find_PathProtection_by_FolderId(int folder_id)
        {
            return this.Path_protections.FirstOrDefault(r => r.Folder_Id == folder_id);
        }
        public async Task LoadFolderRetentions()
        {
            this.RetentionConfiguration = await GetRetentionOptionsAsync();
            List<FolderNodeDTO> dto_folders = await API.Instance.GetFoldersByRootFolderId(this);
            this.Path_protections = await API.Instance.GetPathProtectionsByRootFolderId(this.Id);
            if (dto_folders.Count == 0 || RetentionConfiguration.All_retentions.Count == 0)
            {
                throw new Exception($"unable to load foldertree or retention configuration. Folder count,retention type count {dto_folders.Count}, {RetentionConfiguration.All_retentions.Count}");
            }

            FolderTree = await ConstructFolderTreeFromNodes(this, dto_folders);
        }
        private async Task<RetentionTypes> GetRetentionOptionsAsync()
        {
            RetentionTypesDTO dto = await API.Instance.GetRootfolderRetentionTypes(this);
            return new RetentionTypes(dto);
        }
        public async Task UpdateAggregation()
        {
            // Update the aggregation from the root folder. 
            // This could be optimsed by only updating the modifed branch and its ancestors
            await FolderTree.UpdateAggregation();
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
            await root.SetParentFolderLink();
            //print_folder_leaf_levels(root, 0);
            return root;
        }
        public PathProtectionDTO? FindClosestPathProtectedParent(FolderNode folderNode)
        {
            PathProtectionDTO? closest_path_protection = null;
            FolderNode? current = folderNode.Parent;
            while (current != null && closest_path_protection == null)
            {
                closest_path_protection = Path_protections.FirstOrDefault(r => r.Folder_Id == current.Id);
                current = current.Parent;
            }
            return closest_path_protection;
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