using System;
using System.Text.Json.Serialization;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace VSM.Client.Datamodel
{
    public class SimulationDomainDTO
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }
    public class CleanupFrequencyDTO
    {
        public int Id { get; set; }
        public int SimulationdomainId { get; set; }
        public string Name { get; set; } = "";
        public int Days { get; set; }
    }
    public class LeadTimeDTO
    {
        public int Id { get; set; }
        public int SimulationdomainId { get; set; }
        public string Name { get; set; } = "";
        public int Days { get; set; }
    }
    public class CleanupConfigurationDTO
    {
        public int Id { get; set; } = 0;
        public int RootfolderId { get; set; } = 0;
        public int LeadTime { get; set; } = 0;
        public int Frequency { get; set; } = 0;
        public DateTime? StartDate { get; set; } = new DateTime(); // nullable DateTime to match server nullable datetime
        public string Progress { get; set; } = "";
        /// Return true if cleanup can be started with this configuration
        public bool IsValid() { return Frequency > 0 && LeadTime > 0;}// && StartDate != null; }
    }

    public class FolderNodeDTO
    {
        public int Id { get; set; }
        public int RootfolderId { get; set; }
        public int ParentId { get; set; } = 0; // Default to 0, indicating no parent
        public string Name { get; set; } = "";
        //public int NodeTypeId { get; set; } = 0;           //could be byte
        public int RetentionId { get; set; } = 0;          //could be byte
        public int PathProtectionId { get; set; } = 0;     //could be byte
    }

    // NodeAttributesDTO is only used for nodes with metadata. Most nodes is just an organization of subfolders and will not contain other metadata
    public class RootFolderDTO
    {
        public int Id { get; set; } = 0; //ID of this DTO
        public int SimulationdomainId { get; set; } = 0; //Id of the simulation domain this rootfolder belongs to
        public int FolderId { get; set; } = 0; //Id to folder' FolderNodeDTO. unit24 would be sufficient
        public string StorageId { get; set; } = "local"; // storage identifier that will be used by the scan and cleanup agents to pick tasks for their local system.
        public string Owner { get; set; } = ""; // the initials of the owner
        public string Approvers { get; set; } = ""; // the initials of the approvers (co-owners)
        public string Path { get; set; } = ""; // like /parent/folder. parent would most often be a domain url
        public int? CleanupConfigId { get; set; } = null; // foreign key to CleanupConfigurationDTO
    }

    /*/// <summary>
    /// Enumeration of legal folder type names for simulation domains.
    /// 'innernode' must exist for all domains and will be applied to all folders that are not simulations.
    /// </summary>
    public static class FolderTypeValues
    {
        public enum Types
        {
            INNERNODE,
            VTS_SIMULATION
        }
        public static readonly Dictionary<Types, string> StringValues = new()
        {
            { Types.INNERNODE, "innernode" },
            { Types.VTS_SIMULATION, "vts_simulation" }
        };
        // Direct access properties
        public static string INNERNODE => StringValues[Types.INNERNODE];
        public static string VTS_SIMULATION => StringValues[Types.VTS_SIMULATION];
        public static string GetStringValue(Types folderType)
        {
            return StringValues.TryGetValue(folderType, out var value) ? value : folderType.ToString().ToLower();
        }
    }

    public class FolderTypeDTO
    {
        public byte Id { get; set; }
        public string Name { get; set; } = "";
    }*/
    public class RetentionTypeDTO
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool IsEndStage { get; set; } = false;
        public int DisplayRank { get; set; } = 0;
    }

    public class PathProtectionDTO
    {
        public int Id { get; set; }
        public int RootfolderId { get; set; }
        public int FolderId { get; set; }
        public string Path { get; set; } = "";
    }

    public class RetentionTypes
    {
        public RetentionTypes(List<RetentionTypeDTO>? all_retentions)
        {
            this.AllRetentions = all_retentions ?? new List<RetentionTypeDTO>();

            this.PathRetentionType = this.AllRetentions.FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains("path", StringComparison.OrdinalIgnoreCase)) ?? new RetentionTypeDTO();
            this.CleanedRetentionType = this.AllRetentions.FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains("clean", StringComparison.OrdinalIgnoreCase)) ?? new RetentionTypeDTO();
            this.IssueRetentionType = this.AllRetentions.FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains("issue", StringComparison.OrdinalIgnoreCase))?? new RetentionTypeDTO();

            // the list of dropdown retentions is equal to retentionOptions except for the following retention values
            this.TargetRetentions = this.AllRetentions.Where(r => !string.IsNullOrEmpty(r.Name) &&
                                                                  !r.Name.Contains("clean", StringComparison.OrdinalIgnoreCase) &&
                                                                  !r.Name.Contains("issue", StringComparison.OrdinalIgnoreCase) &&
                                                                  !r.Name.Contains("missing", StringComparison.OrdinalIgnoreCase) ).ToList();
        }

        public List<RetentionTypeDTO> AllRetentions { get; protected set; }
        public List<RetentionTypeDTO> TargetRetentions { get; protected set; }
        public RetentionTypeDTO PathRetentionType { get; protected set; }
        public RetentionTypeDTO CleanedRetentionType { get; protected set; }
        public RetentionTypeDTO IssueRetentionType { get; protected set; }
        public RetentionTypeDTO? FindByName(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                Console.WriteLine("FindByName called with null or empty name");
                return null;
            }
            return this.AllRetentions.FirstOrDefault(r => !string.IsNullOrEmpty(r.Name) && r.Name.Contains(name, StringComparison.OrdinalIgnoreCase));
        }
        public RetentionTypeDTO? FindById(int id)
        {
            RetentionTypeDTO? ret = this.AllRetentions.FirstOrDefault(r => r.Id == id);
            if (ret == null)
            {
                Console.WriteLine($"FindById did not find retention with id: {id}");
            }
            else
            {
                //Console.WriteLine($"FindById found retention with id: {id}, name: {ret.Name}");
            }
            return ret;
        }
    }
    public class FolderRetention
    {
        public required int FolderId { get; set; }
        public required int RetentionId { get; set; }
        public required int PathProtectionId { get; set; }
        //public DateTime? ExpirationDate{ get; set; }= null;
    }
}