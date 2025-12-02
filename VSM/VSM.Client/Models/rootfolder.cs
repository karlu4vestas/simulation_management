using VSM.Client.SharedAPI;
namespace VSM.Client.Datamodel
{
    //-------------------- here come client side objects that contains a DTO like object ----------------
    public class RootFolder
    {
        RootFolderDTO dto = new RootFolderDTO();
        protected API Api { get; }
        public RootFolder(API api)
        {
            Api = api;
        }
        public RootFolder(API api, RootFolderDTO dto, CleanupConfigurationDTO cleanupConfiguration)
        {
            Api = api;
            this.dto = dto;
            this.CleanupConfiguration = cleanupConfiguration;
        }
        public int Id => dto.Id;
        public int FolderId => dto.FolderId;
        public CleanupConfigurationDTO CleanupConfiguration { get; set; } = new CleanupConfigurationDTO();

        public string Root_path => dto.Path;
        public string Owner => dto.Owner;
        public string Approvers => dto.Approvers;
        public List<string> AllInitials => new List<string> { Owner }.Concat(Approvers.Split(',')).ToList();
        // the content of the following property must be loaded on demand by calling LoadFolderRetentions()
        public FolderNode FolderTree { get; set; } = new FolderNode(new FolderNodeDTO());
        public RetentionTypes RetentionTypes { get; set; } = new RetentionTypes([]);
        public List<PathProtectionDTO> PathProtections { get; set; } = new();

        public PathProtectionDTO? FindPathProtectionByFolderId(int folder_id)
        {
            return this.PathProtections.FirstOrDefault(r => r.FolderId == folder_id);
        }
        public async Task LoadFolderRetentions()
        {
            List<RetentionTypeDTO> retentiontypes_dto = await Api.GetRootfolderRetentionTypesAsync(this.Id);
            List<FolderNodeDTO> dto_folders = await Api.GetFoldersByRootFolderIdAsync(this.Id);
            if (dto_folders.Count == 0 || retentiontypes_dto.Count == 0)
            {
                throw new Exception($"unable to load foldertree or retention configuration. Folder count,retention type count {dto_folders.Count}, {RetentionTypes.AllRetentions.Count}");
            }
            this.RetentionTypes = new RetentionTypes(retentiontypes_dto);
            this.PathProtections = await Api.GetPathProtectionsByRootFolderIdAsync(this.Id)??[];

            FolderTree = await ConstructFolderTreeFromNodes(this, dto_folders);
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
                if (dto.ParentId == 0)
                {
                    // This is the root node
                    continue;
                }
                else if (nodeLookup.TryGetValue(dto.ParentId, out var parentNode))
                {
                    parentNode.Children.Add(nodeLookup[dto.Id]);
                }
            }
            FolderNode root = nodeLookup[rootFolder.FolderId];
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
                closest_path_protection = PathProtections.FirstOrDefault(r => r.FolderId == current.Id);
                current = current.Parent;
            }
            return closest_path_protection;
        }
        public PathProtectionDTO? FindPathProtection(FolderNode folderNode)
        {
            PathProtectionDTO? path_protection  = PathProtections.FirstOrDefault(r => r.FolderId == folderNode.Id);
            return path_protection;
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