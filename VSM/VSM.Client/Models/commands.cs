
using VSM.Client.Datamodel;
using VSM.Client.SharedAPI;
/// <summary>
/// A singleton manager for handling commands. Not sure that we need this at present
/// Later we can use this as a first step towards implementing real-time multiuser support by just sending the commands
/// to a server and have the server broadcast the changes to all clients - in this way we avoid sending the state changes of the folder
/// Has to be validated that this will converge to the same state on all clients.
/// </summary>
public class CommandManager
{
    private static readonly Lazy<CommandManager> _instance = new Lazy<CommandManager>(() => new CommandManager());
    // Prevent instantiation from outside
    private CommandManager() { }
    // Public static property to access the instance
    public static CommandManager Instance => _instance.Value;

    public void Add(Command command)
    {
        // Implementation for adding a command
    }
    public void Remove(Command command)
    {
        // Implementation for removing a command
    }
}
//@todo we need for all commands to have a rollback option. That is we must implement as rollback function for all commands in order to reload data from server 
public abstract class Command
{
    public string Name { get; }
    public Command(string name)
    {
        Name = name;
    }
    public abstract Task<bool> Apply();
}

public class UpdateCleanupConfigurationCmd : Command
{
    CleanupConfigurationDTO cleanup_configuration;
    RootFolder rootFolder;
    public UpdateCleanupConfigurationCmd(RootFolder rootFolder, CleanupConfigurationDTO newCleanupConfiguration) : base("Change cleanup Frequency")
    {
        this.cleanup_configuration = newCleanupConfiguration;
        this.rootFolder = rootFolder;
        CommandManager.Instance.Add(this);
    }
    public override async Task<bool> Apply()
    {
        bool result = false;
        try
        {
            result = await API.Instance.UpdateCleanupConfigurationForRootFolder(this.rootFolder.Id, this.cleanup_configuration);
            if (result)
                rootFolder.CleanupConfiguration = cleanup_configuration;
        }
        finally
        {
            CommandManager.Instance.Remove(this);
        }
        return result;
    }
}
/// <summary>
/// Command to add a path protection and update the retention value of the simulations
/// </summary>
public class AddPathProtectionCmd : Command
{
    public PathProtectionDTO? pathProtection = null;
    RootFolder rootFolder;
    FolderNode folderNode;
    public AddPathProtectionCmd(RootFolder rootFolder, FolderNode node) : base("Add path protection")
    {
        this.folderNode = node;
        this.rootFolder = rootFolder;
        CommandManager.Instance.Add(this);
    }
    public override async Task<bool> Apply()
    {
        bool result = false;
        try
        {
            // Should handle adding if there is no path protections and 
            // adding to existing path protections
            //   - siblings 
            //   - parent to one or more path protections at lower level
            //   - child
            int path_retentiontype_id = rootFolder.RetentionConfiguration.Path_retentiontype.Id;

            PathProtectionDTO? parent_protection = rootFolder.FindClosestPathProtectedParent(folderNode);
            Retention? parent_path_retention = parent_protection == null ? null : new Retention(path_retentiontype_id, parent_protection.Id);

            PathProtectionDTO new_path_protection = new PathProtectionDTO
            {
                //Id = pathProtection.Id, // Id will be set by the server
                //Id = Library.Instance.NewID,  //untill we use persistance to get an ID
                Rootfolder_Id = folderNode.Rootfolder_Id,
                Folder_Id = folderNode.Id,
                Path = folderNode.FullPath
            };
            // Create and persist the new path protection in order to get an ID assigned
            int? pathprotection_id = await API.Instance.AddPathProtectionByRootFolder(new_path_protection);
            new_path_protection.Id = pathprotection_id == null ? 0 : pathprotection_id.Value;
            result = pathprotection_id != null;
            if (pathprotection_id == null)
            {
                throw new Exception("Failed to add path protection via API.");
            }
            this.pathProtection = new_path_protection;
            rootFolder.Path_protections.Add(this.pathProtection);

            Retention new_path_retention = new Retention(path_retentiontype_id, this.pathProtection.Id);
            List<RetentionUpdateDTO> retentionUpdates = new List<RetentionUpdateDTO>();
            if (parent_path_retention != null)
                //add a sub pathprotection to a parent pathprotection    
                retentionUpdates = await folderNode.ChangeRetentionsOfSubtree(new AddPathProtectionToParentPathProtectionDelegate(parent_path_retention, new_path_retention));
            else
                retentionUpdates = await folderNode.ChangeRetentionsOfSubtree(new AddPathProtectionOnMixedSubtreesDelegate(new_path_retention));

            result = await API.Instance.UpdateRootFolderRetentions(rootFolder.Id, retentionUpdates);
            if (!result)
            {
                throw new Exception("AddPathProtectionCmd:Failed to update retentions via API.");
            }
        }
        finally
        {
            await rootFolder.UpdateAggregation();
            CommandManager.Instance.Remove(this);
        }
        return result;
    }
}
/// <summary>
/// Command to remove a path protection and update the retention value of the simulations
/// </summary>
public class RemovePathProtectionCmd : Command
{
    public int remove_count = 0;
    RootFolder rootFolder;
    FolderNode folderNode;
    int to_retention_Id;
    public RemovePathProtectionCmd(RootFolder rootFolder, FolderNode folderNode, int to_retention_Id) : base("Remove path protection")
    {
        this.folderNode = folderNode;
        this.rootFolder = rootFolder;
        this.to_retention_Id = to_retention_Id;
        CommandManager.Instance.Add(this);
    }
    public override async Task<bool> Apply()
    {
        // step 1: find this node path protection entry 
        // step 2: remove it from the list if found
        // step 3: Verify if this node has a parent PathProtection in the retention_config
        //         If there is no parent path protection, 
        //            -then set the retention of leaves under this nodes to (to_retention_Id, to_path_protection_id=0). 
        //         else  
        //            -then set the retention of leaves under this node to (path protection type,  from_path_protectection.Id).
        //         In this way we do not touch path protection of other children.
        try
        {
            int path_retentiontype_id = rootFolder.RetentionConfiguration.Path_retentiontype.Id;

            PathProtectionDTO? from_path_protection = rootFolder.Path_protections.FirstOrDefault(p => p.Folder_Id == folderNode.Id);
            if (from_path_protection == null)
                throw new ArgumentException("Invalid new path protection folder specified.");

            PathProtectionDTO valid_from_path_protection = from_path_protection; //get rid of these null warnings

            // check for the presence of a parent of path_protection_folder
            PathProtectionDTO? parent_path_protection = rootFolder.FindClosestPathProtectedParent(folderNode);
            Retention to_retention = parent_path_protection == null ? new Retention(to_retention_Id) : new Retention(path_retentiontype_id, parent_path_protection.Id);

            Retention from_path_retention = new Retention(path_retentiontype_id, valid_from_path_protection.Id);

            //change of client
            remove_count = rootFolder.Path_protections.RemoveAll(p => p.Folder_Id == folderNode.Id);
            List<RetentionUpdateDTO> retentionUpdates = await folderNode.ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(from_path_retention, to_retention));

            //change on server
            bool result = await API.Instance.DeletePathProtectionByRootFolderAndPathProtection(rootFolder.Id, valid_from_path_protection.Id);
            if (!result)
            {
                throw new Exception("Failed to delete path protection via API.");
            }
            result = await API.Instance.UpdateRootFolderRetentions(rootFolder.Id, retentionUpdates);
            if (!result)
            {
                throw new Exception("RemovePathProtectionCmd: Failed to update retentions via API.");
            }
        }
        finally
        {
            await rootFolder.UpdateAggregation();
            CommandManager.Instance.Remove(this);
        }
        return remove_count != 0;
    }
}
/// <summary>
/// Command to remove a path protection and update the retention value of the simulations
/// </summary>
public class ChangeRetentionId2IdCmd : Command
{
    RootFolder rootFolder;
    FolderNode folderNode;
    int from_retention_Id;
    int to_retention_Id;
    public ChangeRetentionId2IdCmd(RootFolder rootFolder, FolderNode folderNode, int from_retention_Id, int to_retention_Id) : base("change retention from_id to to_id path protection")
    {
        this.rootFolder = rootFolder;
        this.folderNode = folderNode;
        this.from_retention_Id = from_retention_Id;
        this.to_retention_Id = to_retention_Id;
        CommandManager.Instance.Add(this);
    }

    public override async Task<bool> Apply()
    {
        bool result = false;
        try
        {
            List<RetentionUpdateDTO> retentionUpdates = await folderNode.ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(new Retention(from_retention_Id), new Retention(to_retention_Id)));

            result = await API.Instance.UpdateRootFolderRetentions(rootFolder.Id, retentionUpdates);
            if (!result)
            {
                throw new Exception("ChangeRetentionId2IdCmd: Failed to update retentions via API.");
            }
        }
        finally
        {
            await rootFolder.UpdateAggregation();
        }
        return result;
    }
}