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
    //do not delete FolderType. It has been commented out at present because it is not usee
    /*public class FolderType : FolderTypeDTO { }
        private List<FolderType>? _foldertypes;
        public async Task<List<FolderType>> GetFolderTypesAsync()
        {
            if (_foldertypes == null)
            {
                List<FolderTypeDTO> foldertype_dto = await API.Instance.GetFolderTypesFromApiAsync();
                _foldertypes = foldertype_dto.Select(dto => (FolderType)dto).ToList();
            }
            return _foldertypes;
        }
    */
    // NodeAttributesDTO is only used for nodes with metadata. Most nodes is just an organization of subfolders and will not contain other metadata
    public class RootFolderDTO
    {
        public int Id { get; set; } //ID of this DTO
        public string Path { get; set; } = ""; // like /parent/folder. parent would most often be a domain url
        public int Folder_Id { get; set; } //Id to folder' FolderNodeDTO. unit24 would be sufficient
        public string Owner { get; set; } = ""; // the initials of the owner
        public string Approvers { get; set; } = ""; // the initials of the approvers (co-owners)
        public string Cleanup_Frequency { get; set; } = ""; // indicates the folder's cleanup frequency
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
    public class PathProtectionDTO
    {
        public int Id { get; set; }
        public int Rootfolder_Id { get; set; }
        public int Folder_Id { get; set; }
        public string Path { get; set; } = "";
    }
    public class PathProtection
    {
        public PathProtectionDTO dto;
        public PathProtection(PathProtectionDTO dto)
        {
            this.dto = dto;
        }
    }
    public class RetentionTypesTO
    {
        public RetentionTypesTO(List<RetentionTypeDTO>? all_retentions = null)
        {
            if (all_retentions != null)
                this.All_retentions = all_retentions;
            this.Path_retention = this.All_retentions.FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains("path", StringComparison.OrdinalIgnoreCase)) ?? this.Path_retention;
            this.Cleaned_retention = this.All_retentions.FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains("cleaned", StringComparison.OrdinalIgnoreCase)) ?? this.Cleaned_retention;
            this.Issue_retention = this.All_retentions.FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains("issue", StringComparison.OrdinalIgnoreCase)) ?? this.Issue_retention;
        }
        public List<RetentionTypeDTO> All_retentions = new();
        public RetentionTypeDTO Path_retention = new();
        public RetentionTypeDTO Cleaned_retention = new();
        public RetentionTypeDTO Issue_retention = new();
    }

    //@todo: convert from the DTOs
    public class RetentionTypes
    {
        RetentionTypesTO dto;
        public RetentionTypes(RetentionTypesTO dto)
        {
            this.dto = dto;
            //the list of dropdown retentions is equal to retentionOptions except for the issue and cleaned retention value
            this.Target_retentions = this.All_retentions.Where(r => !r.Name.Contains("clean", StringComparison.OrdinalIgnoreCase) &&
                                                                    !r.Name.Contains("issue", StringComparison.OrdinalIgnoreCase)).ToList() ?? this.Target_retentions;
        }
        public List<RetentionTypeDTO> All_retentions => dto.All_retentions;
        public List<RetentionTypeDTO> Target_retentions = new();
        public RetentionTypeDTO Path_retentiontype => dto.Path_retention;
        public RetentionTypeDTO Cleaned_retentiontype => dto.Cleaned_retention;
        public RetentionTypeDTO Issue_retentiontype => dto.Issue_retention;
        public RetentionTypeDTO? Find_by_Name(string name)
        {
            return this.All_retentions.FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains(name, StringComparison.OrdinalIgnoreCase));
        }
        public RetentionTypeDTO? Find_by_Id(int id)
        {
            return this.All_retentions.FirstOrDefault(r => r.Id == id);
        }
    }
    public class RetentionUpdateDTO
    {
        public required int Folder_id { get; set; }
        public required int Retention_id { get; set; }
        public required int Pathprotection_id { get; set; }
    }
}