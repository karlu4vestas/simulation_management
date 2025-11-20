using System.Runtime.InteropServices;
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
    public string Name => GetType().Name;
    protected bool _isExecuting = false;
    protected bool _wasSuccessful = false;

    public Command()
    {
    }

    /// <summary>
    /// Executes the command with automatic rollback on failure
    /// </summary>
    public async Task<bool> ExecuteAsync()
    {
        _isExecuting = true;
        bool wasSuccessful = false;
        try
        {
            bool result = await Apply();
            _wasSuccessful = result;
            wasSuccessful = result;
            Log("Apply", wasSuccessful);
            return result;
        }
        catch (Exception applyException)
        {
            _wasSuccessful = false;
            wasSuccessful = false;
            Log("Apply", false);

            try
            {
                await Rollback();
                Log("Rollback", true);
            }
            catch (Exception rollbackException)
            {
                Log("Rollback", false);
                // Log or handle rollback failure - could throw aggregate exception
                throw new AggregateException("Command apply failed and rollback also failed",
                    applyException, rollbackException);
            }

            // Re-throw the original apply exception after successful rollback
            throw;
        }
        finally
        {
            _isExecuting = false;
            // Call the abstract finally method with success status
            await OnFinally(wasSuccessful);
            // Always ensure command is removed from manager after execution attempt
            CommandManager.Instance.Remove(this);
        }
    }

    public abstract Task<bool> Apply();
    /// <summary>
    /// Reverts the changes made by Apply. Should only be called if Apply failed.
    /// @TODO Making a full implementation in the below commands is important but has not been done at present
    /// </summary>
    public abstract Task<bool> Rollback();
    /// <summary>
    /// Called after Apply completes (success or failure) and after Rollback (if needed)
    /// </summary>
    /// <param name="wasSuccessful">True if Apply succeeded, false if it failed</param>
    protected abstract Task OnFinally(bool wasSuccessful);

    /// <summary>
    /// Logs the result of a command action. Override for custom logging behavior.
    /// </summary>
    /// <param name="action">The action being logged (e.g., "Apply", "Rollback")</param>
    /// <param name="wasSuccessful">Whether the action was successful</param>
    protected virtual void Log(string action, bool wasSuccessful)
    {
        string status = wasSuccessful ? "SUCCESS" : "FAILED";
        Console.WriteLine($"Command '{Name}' - {action}: {status}");
    }
}

public class UpdateCleanupConfigurationCmd : Command
{
    CleanupConfigurationDTO cleanup_configuration;
    RootFolder rootFolder;
    CleanupConfigurationDTO? original_cleanup_configuration;
    public UpdateCleanupConfigurationCmd(RootFolder rootFolder, CleanupConfigurationDTO newCleanupConfiguration)
    {
        this.cleanup_configuration = newCleanupConfiguration;
        this.rootFolder = rootFolder;
        CommandManager.Instance.Add(this);
    }
    public override async Task<bool> Apply()
    {
        // Store original state for rollback
        original_cleanup_configuration = rootFolder.CleanupConfiguration;

        var updatedConfig = await API.Instance.UpdateCleanupConfigurationForRootFolder(this.rootFolder.Id, this.cleanup_configuration);
        if (updatedConfig != null)
        {
            rootFolder.CleanupConfiguration = updatedConfig;
            return true;
        }
        //else
        //    throw new Exception("Failed to update cleanup configuration via API.");

        return false;
    }
    public override async Task<bool> Rollback()
    {
        if (original_cleanup_configuration != null)
        {
            rootFolder.CleanupConfiguration = original_cleanup_configuration;
            // Optionally restore on server too
            // await API.Instance.UpdateCleanupConfigurationForRootFolder(rootFolder.Id, original_cleanup_configuration);
        }
        await Task.Delay(0);
        return true;
    }
    protected override async Task OnFinally(bool wasSuccessful)
    {
        // No specific cleanup needed for this command
        await Task.CompletedTask;
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
    public AddPathProtectionCmd(RootFolder rootFolder, FolderNode node)
    {
        this.folderNode = node;
        this.rootFolder = rootFolder;
        CommandManager.Instance.Add(this);
    }
    public override async Task<bool> Apply()
    {
        int path_retentiontype_id = rootFolder.RetentionConfiguration.Path_retentiontype.Id;

        PathProtectionDTO? parent_protection = rootFolder.FindClosestPathProtectedParent(folderNode);
        Retention? parent_path_retention = parent_protection == null ? null : new Retention(path_retentiontype_id, parent_protection.Id);

        PathProtectionDTO new_path_protection = new PathProtectionDTO
        {
            Rootfolder_Id = folderNode.Rootfolder_Id,
            Folder_Id = folderNode.Id,
            Path = folderNode.FullPath
        };

        int? pathprotection_id = await API.Instance.AddPathProtectionByRootFolder(new_path_protection);
        new_path_protection.Id = pathprotection_id == null ? 0 : pathprotection_id.Value;
        if (pathprotection_id == null)
        {
            throw new Exception("Failed to add path protection via API.");
        }
        this.pathProtection = new_path_protection;
        rootFolder.Path_protections.Add(this.pathProtection);

        Retention new_path_retention = new Retention(path_retentiontype_id, this.pathProtection.Id);
        List<RetentionUpdateDTO> retentionUpdates = new List<RetentionUpdateDTO>();
        if (parent_path_retention != null)
            retentionUpdates = await folderNode.ChangeRetentionsOfSubtree(new AddPathProtectionToParentPathProtectionDelegate(parent_path_retention, new_path_retention));
        else
            retentionUpdates = await folderNode.ChangeRetentionsOfSubtree(new AddPathProtectionOnMixedSubtreesDelegate(new_path_retention));

        bool result = await API.Instance.UpdateRootFolderRetentions(rootFolder.Id, retentionUpdates);
        if (!result)
        {
            throw new Exception("AddPathProtectionCmd:Failed to update retentions via API.");
        }

        return result;
    }
    public override async Task<bool> Rollback()
    {
        if (pathProtection != null)
        {
            // Remove from client state
            rootFolder.Path_protections.RemoveAll(p => p.Id == pathProtection.Id);

            // Remove from server
            await API.Instance.DeletePathProtectionByRootFolderAndPathProtection(rootFolder.Id, pathProtection.Id);

            // Update aggregation
            await rootFolder.UpdateAggregation();
        }
        return true;
    }
    protected override async Task OnFinally(bool wasSuccessful)
    {
        // Always update aggregation regardless of success/failure
        await rootFolder.UpdateAggregation();
    }
    protected override void Log(string action, bool wasSuccessful)
    {
        base.Log(action, wasSuccessful);

        if (action == "Apply" && wasSuccessful && pathProtection != null)
        {
            Console.WriteLine($"Added PathProtection ID: {pathProtection.Id} for Folder ID: {pathProtection.Folder_Id}");
        }
        else if (action == "Apply" && !wasSuccessful)
        {
            Console.WriteLine($"Error: Failed to create path protection for folder {folderNode.Name} ({folderNode.FullPath})");
        }
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
    public RemovePathProtectionCmd(RootFolder rootFolder, FolderNode folderNode, int to_retention_Id)
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

        return remove_count != 0;
    }

    protected override async Task OnFinally(bool wasSuccessful)
    {
        // Move the existing finally logic here
        await rootFolder.UpdateAggregation();
    }
    public override async Task<bool> Rollback()
    {
        Console.WriteLine("Rollback of RemovePathProtectionCmd not implemented");
        await Task.Delay(0);
        return true;
    }
    protected override void Log(string action, bool wasSuccessful)
    {
        base.Log(action, wasSuccessful);

        if (action == "Apply" && wasSuccessful)
        {
            Console.WriteLine($"Removed PathProtection for Folder: {folderNode.FullPath}");
        }
        else if (action == "Apply" && !wasSuccessful)
        {
            Console.WriteLine($"Error: Failed to remove path protection for folder {folderNode.FullPath}");
        }
    }
}

/// <summary>
/// Command to remove a path protection and update the retention value of the simulations
/// </summary>
public class ChangeRetentionsCmd : Command
{
    RootFolder rootFolder;
    FolderNode folderNode;
    int from_retention_Id;
    int to_retention_Id;
    public ChangeRetentionsCmd(RootFolder rootFolder, FolderNode folderNode, int from_retention_Id, int to_retention_Id)
    {
        this.rootFolder = rootFolder;
        this.folderNode = folderNode;
        this.from_retention_Id = from_retention_Id;
        this.to_retention_Id = to_retention_Id;
        CommandManager.Instance.Add(this);
    }

    public override async Task<bool> Apply()
    {
        List<RetentionUpdateDTO> retentionUpdates = await folderNode.ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(new Retention(from_retention_Id), new Retention(to_retention_Id)));

        bool result = await API.Instance.UpdateRootFolderRetentions(rootFolder.Id, retentionUpdates);
        if (!result)
        {
            throw new Exception("ChangeRetentionId2IdCmd: Failed to update retentions via API.");
        }

        return result;
    }

    protected override async Task OnFinally(bool wasSuccessful)
    {
        // Move the existing finally logic here
        await rootFolder.UpdateAggregation();
    }
    public override async Task<bool> Rollback()
    {
        Console.WriteLine("Rollback of ChangeRetentionsCmd not implemented");
        await Task.Delay(0);
        return true;
    }
    protected override void Log(string action, bool wasSuccessful)
    {
        base.Log(action, wasSuccessful);

        if (action == "Apply" && wasSuccessful)
        {
            Console.WriteLine($"ChangeRetentionsCmd: {from_retention_Id} to {to_retention_Id}");
        }
        else if (action == "Apply" && !wasSuccessful)
        {
            Console.WriteLine($"ChangeRetentionsCmd: failed to change retention for {from_retention_Id} to {to_retention_Id}");
        }
    }
}