using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace VSM.Client.Datamodel
{

    public class FolderNodeDTO
    {
        public int Id { get; set; }
        public int Rootfolder_Id { get; set; }
        public int Parent_Id { get; set; } = 0; // Default to 0, indicating no parent
        public string Name { get; set; } = "";
        public int Type_Id { get; set; } = 0;               //should be byte
        public int Retention_Id { get; set; } = 0;          //should be byte
        public int Path_Protection_Id { get; set; } = 0;    //should be byte
        public string? Retention_Date { get; set; } = null;
        public string? Modified { get; set; } = null;
    }

    // NodeAttributesDTO is only used for nodes with metadata. Most nodes is just an organization of subfolders and will not contain other metadata
    public class NodeAttributesDTO
    {
        // nodeID
        public int NodeID { get; set; }
        public byte RetentionID { get; set; } = 0;

        // is set to last modification date + the retention period, when the user selects a retention date
        public DateOnly? RetentionDate { get; set; } = null;
        public DateOnly? Modified { get; set; } = null; // null for InnerNodes but for the simulation this will be the last time any file in the simulation was modified
    }




    public class RootFolderDTO
    {
        //mapped
        public int Id { get; set; } //ID of this DTO
        public string Path { get; set; } = ""; // like /parent/folder. parent would most often be a domain url
        public int Folder_Id { get; set; } //Id to folder' FolderNodeDTO. unit24 would be sufficient
        public string Owner { get; set; } = ""; // the initials of the owner
        public string Approvers { get; set; } = ""; // the initials of the approvers (co-owners)
        public bool Active_Cleanup { get; set; } // indicates if the folder is actively being cleaned up
    }

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

        public bool Is_registeredfor_cleanup
        {
            get => dto.Active_Cleanup;
            set => dto.Active_Cleanup = value;
        }
        public string Root_path => dto.Path;
        public string Owner => dto.Owner;
        public string Approvers => dto.Approvers;

        public List<string> AllInitials => new List<string> { Owner }.Concat(Approvers.Split(',')).ToList();

        private FolderNode? _folderTree;

        public async Task<FolderNode?> GetFolderTreeAsync()
        {
            if (_folderTree == null)
            {
                _folderTree = await DataModel.Instance.GetFoldersByRootFolderIdAsync(this);
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

    }


    // so far we know: 
    // InnerNode, 
    // VTSSimulation, which is currently a LeafNode but that might change
    public class FolderTypeDTO
    {
        public byte Id { get; set; }
        public string Name { get; set; } = "InnerNode";
    }

    public class RetentionTypeDTO
    {
        public byte Id { get; set; }
        public string Name { get; set; } = "";
        public string IsSystemManaged { get; set; } = "";
        public byte DisplayRank { get; set; } = 0;
    }
    public class RetentionType : RetentionTypeDTO { }
    public class FolderType : FolderTypeDTO { }
    public class PathProtectionDTO
    {
        public byte Id { get; set; }
        public int Rootfolder_Id { get; set; }
        public int Folder_Id { get; set; }
        public string Path
        {
            get;
            set;
        } = "";
        public int Levels { get; set; } = 0;
    }
    public class PathProtection
    {
        public PathProtectionDTO dto;
        public PathProtection(PathProtectionDTO dto)
        {
            this.dto = dto;
        }
    }

    public class RetentionConfigurationDTO
    {
        public RetentionConfigurationDTO(List<RetentionTypeDTO>? all_retentions = null)
        {
            if (all_retentions != null)
                this.All_retentions = all_retentions;

            this.Path_retention = this.All_retentions.FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains("path", StringComparison.OrdinalIgnoreCase)) ?? this.Path_retention;

            this.Cleaned_retention = this.All_retentions.FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains("cleaned", StringComparison.OrdinalIgnoreCase)) ?? this.Cleaned_retention;

            this.Issue_retention = this.All_retentions.FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains("issue", StringComparison.OrdinalIgnoreCase)) ?? this.Issue_retention;

            //the list of dropdown retentions is equal to retentionOptions except for the cleaned retention value
            this.Target_retentions = this.All_retentions.Where(r => r.Id != this.Cleaned_retention.Id && !r.Name.Contains("issue", StringComparison.OrdinalIgnoreCase)).ToList() ?? this.Target_retentions;
        }
        public List<RetentionTypeDTO> All_retentions = new();
        public List<RetentionTypeDTO> Target_retentions = new();
        public RetentionTypeDTO Path_retention = new();
        public RetentionTypeDTO Cleaned_retention = new();
        public RetentionTypeDTO Issue_retention = new();
        public List<PathProtectionDTO> Path_protections = new();
    }

    //@todo: convert from the DTOs
    public class RetentionConfiguration
    {
        RetentionConfigurationDTO dto;
        public RetentionConfiguration(RetentionConfigurationDTO dto)
        {
            this.dto = dto;
        }
        public List<RetentionTypeDTO> All_retentions => dto.All_retentions;
        public List<RetentionTypeDTO> Target_retentions => dto.Target_retentions;
        public RetentionTypeDTO Path_retention => dto.Path_retention;
        public RetentionTypeDTO Cleaned_retention => dto.Cleaned_retention;
        public RetentionTypeDTO Issue_retention => dto.Issue_retention;
        public List<PathProtectionDTO> Path_protections => dto.Path_protections;
        public RetentionTypeDTO? Find_by_Name(string name)
        {
            return this.All_retentions.FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains(name, StringComparison.OrdinalIgnoreCase));
        }
        public RetentionTypeDTO? Find_by_Id(int id)
        {
            return this.All_retentions.FirstOrDefault(r => r.Id == id);
        }
        public PathProtectionDTO? Find_PathProtection_by_FolderId(int folder_id)
        {
            return this.Path_protections.FirstOrDefault(r => r.Folder_Id == folder_id);
        }
    }
}
