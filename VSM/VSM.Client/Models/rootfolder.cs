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
        // the content of the following property must be loaded on demand by calling LoadFolderRetentions()
        public FolderNode FolderTree { get; set; } = new FolderNode(new FolderNodeDTO());
        public RetentionConfiguration RetentionConfiguration { get; set; } = new RetentionConfiguration(new RetentionConfigurationDTO());
        public async Task LoadFolderRetentions()
        {
            this.RetentionConfiguration = await GetRetentionOptionsAsync();
            List<FolderNodeDTO> dto_folders = await API.Instance.GetFoldersByRootFolderIdAsync(this);

            if (dto_folders.Count == 0 || RetentionConfiguration.All_retentions.Count == 0)
            {
                throw new Exception($"unable to load foldertree or retention configuration. Folder count,retention type count {dto_folders.Count}, {RetentionConfiguration.All_retentions.Count}");
            }

            FolderTree = await ConstructFolderTreeFromNodes(this, dto_folders);
        }
        private async Task<RetentionConfiguration> GetRetentionOptionsAsync()
        {
            RetentionConfigurationDTO dto = await API.Instance.GetRetentionTypesFromApiAsync();
            return new RetentionConfiguration(dto);
        }
        public async Task UpdateAggregation()
        {
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

        public async Task<PathProtectionDTO> AddPathProtection(FolderNode folderNode, RetentionConfiguration retention_config)
        {
            // Should handle adding if there is no path protections and 
            // adding to existing path protections
            //   - siblings 
            //   - parent to one or more path protections at lower level
            //   - child
            int path_retentiontype_id = retention_config.Path_retentiontype.Id;

            PathProtectionDTO? parent_protection = FindClosestPathProtectedParent(folderNode, retention_config);
            Retention? parent_path_retention = parent_protection == null ? null : new Retention(path_retentiontype_id, parent_protection.Id);

            PathProtectionDTO new_path_protection = new PathProtectionDTO
            {
                //Id = pathProtection.Id, // Id will be set by the server
                Id = DataModel.Instance.NewID,  //untill we use persistance to get an ID
                Rootfolder_Id = folderNode.Rootfolder_Id,
                Folder_Id = folderNode.Id,
                Path = folderNode.FullPath
            };
            retention_config.Path_protections.Add(new_path_protection);
            Retention new_path_retention = new Retention(path_retentiontype_id, new_path_protection.Id);

            if (parent_path_retention != null)
                //add a sub pathprotection to a parent pathprotection    
                await folderNode.ChangeRetentionsOfSubtree(new AddPathProtectionToParentPathProtectionDelegate(parent_path_retention, new_path_retention));
            else
                await folderNode.ChangeRetentionsOfSubtree(new AddPathProtectionOnMixedSubtreesDelegate(new_path_retention));

            return new_path_protection;
        }

        private PathProtectionDTO? FindClosestPathProtectedParent(FolderNode folderNode, RetentionConfiguration retention_config)
        {
            PathProtectionDTO? closest_path_protection = null;
            FolderNode current = folderNode;
            while (current.Parent != null && closest_path_protection == null)
            {
                closest_path_protection = retention_config.Path_protections.FirstOrDefault(r => r.Folder_Id == current.Id);
                current = current.Parent;
            }
            return closest_path_protection;
        }

        public async Task<int> RemovePathProtection(FolderNode folderNode, RetentionConfiguration retention_config, int to_retention_Id)
        {
            // step 1: find this node path protection entry 
            // step 2: remove it from the list if found
            // step 3: Verify if this node has a parent PathProtection in the retention_config
            //         If there is no parent path protection, 
            //            -then set the retention of leaves under this nodes to (to_retention_Id, to_path_protection_id=0). 
            //         else  
            //            -then set the retention of leaves under this node to (path protection type,  from_path_protectection.Id).
            //         In this way we do not touch path protection of other children.
            int path_retentiontype_id = retention_config.Path_retentiontype.Id;

            PathProtectionDTO? from_path_protection = retention_config.Path_protections.FirstOrDefault(p => p.Folder_Id == folderNode.Id);
            int remove_count = retention_config.Path_protections.RemoveAll(p => p.Folder_Id == folderNode.Id);
            Retention? from_path_retention = from_path_protection == null ? null : new Retention(path_retentiontype_id, from_path_protection.Id);

            // check for the presence of a parent of path_protection_folder
            PathProtectionDTO? parent_path_protection = FindClosestPathProtectedParent(folderNode, retention_config);
            Retention? to_parent_path_retention = parent_path_protection == null ? null : new Retention(path_retentiontype_id, parent_path_protection.Id);
            if (from_path_retention == null)
                throw new ArgumentException("Invalid new path protection folder specified.");
            else
            {
                if (to_parent_path_retention != null)
                    await folderNode.ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(from_path_retention, to_parent_path_retention));
                else
                    await folderNode.ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(from_path_retention, new Retention(to_retention_Id)));
            }
            return remove_count;
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

        public async Task<PathProtectionDTO> AddPathProtection(FolderNode folderNode, RetentionConfiguration retention_config)
        {
            // Should handle adding if there is no path protections and 
            // adding to existing path protections
            //   - siblings 
            //   - parent to one or more path protections at lower level
            //   - child
            PathProtectionDTO? parent_protection = FindClosestPathProtectedParent(folderNode, retention_config);
            PathProtectionDTO new_path_protection = new PathProtectionDTO
            {
                //Id = pathProtection.Id, // Id will be set by the server
                Id = DataModel.Instance.NewID,  //untill we use persistance to get an ID
                Rootfolder_Id = folderNode.Rootfolder_Id,
                Folder_Id = folderNode.Id,
                Path = folderNode.FullPath
            };
            retention_config.Path_protections.Add(new_path_protection);

            int path_retentiontype_id = retention_config.Path_retentiontype.Id;
            if (parent_protection != null)
                //add a sub pathprotection to a parent pathprotection    
                await folderNode.ChangeRetentionsOfSubtree(new AddPathProtectionToParentPathProtectionDelegate(parent_protection.Id, path_retentiontype_id, new_path_protection.Id));
            else
                await folderNode.ChangeRetentionsOfSubtree(new AddPathProtectionOnMixedSubtreesDelegate(path_retentiontype_id, new_path_protection.Id));

            return new_path_protection;
        }

        private PathProtectionDTO? FindClosestPathProtectedParent(FolderNode folderNode, RetentionConfiguration retention_config)
        {
            PathProtectionDTO? closest_path_protection = null;
            FolderNode current = folderNode;
            while (current.Parent != null && closest_path_protection == null)
            {
                closest_path_protection = retention_config.Path_protections.FirstOrDefault(r => r.Folder_Id == current.Id);
                current = current.Parent;
            }
            return closest_path_protection;
        }

        public async Task<int> RemovePathProtection(FolderNode folderNode, RetentionConfiguration retention_config, int to_retention_Id)
        {
            // step 1: find this node path protection entry 
            // step 2: remove it from the list if found
            // step 3: Verify if this node has a parent PathProtection in the retention_config
            //         If there is no parent path protection, 
            //            -then set the retention of leaves under this nodes to (to_retention_Id, to_path_protection_id=0). 
            //         else  
            //            -then set the retention of leaves under this node to (path protection type,  from_path_protectection.Id).
            //         In this way we do not touch path protection of other children.
            PathProtectionDTO? from_path_retention = retention_config.Path_protections.FirstOrDefault(p => p.Folder_Id == folderNode.Id);
            int remove_count = retention_config.Path_protections.RemoveAll(p => p.Folder_Id == folderNode.Id);
            int path_retentiontype_id = retention_config.Path_retentiontype.Id;

            // check if any of the pathretentions from retention_config are a parent of from_path_retention_folder
            PathProtectionDTO? parent_path_protection = FindClosestPathProtectedParent(folderNode, retention_config);
            if (from_path_retention == null || remove_count == 0)
                throw new ArgumentException("Invalid path protection folder specified.");
            else
            {
                if (parent_path_protection != null)
                    await folderNode.ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(path_retentiontype_id, from_path_retention.Id, path_retentiontype_id, parent_path_protection.Id));
                else
                    await folderNode.ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(path_retentiontype_id, from_path_retention.Id, to_retention_Id, 0));
            }
            return remove_count;
        }

    }
}