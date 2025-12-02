using System.Runtime.InteropServices;
using VSM.Client.Datamodel;
using VSM.Client.SharedAPI;
/// <summary>
/// A singleton manager for handling commands. Not sure that we need this at present
/// Later we can use this as a first step towards implementing real-time multiuser support by just sending the commands
/// to a server and have the server broadcast the changes to all clients - in this way we avoid sending the state changes of the folder
/// Has to be validated that this will converge to the same state on all clients.
/// </summary>

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
            //Log("Apply", wasSuccessful);
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
    protected API Api { get; }
    public UpdateCleanupConfigurationCmd(API api, RootFolder rootFolder, CleanupConfigurationDTO newCleanupConfiguration)
    {
        Api = api;
        this.cleanup_configuration = newCleanupConfiguration;
        this.rootFolder = rootFolder;
    }
    public override async Task<bool> Apply()
    {
        // Store original state for rollback
        original_cleanup_configuration = rootFolder.CleanupConfiguration;

        var updatedConfig = await Api.UpdateCleanupConfigurationForRootFolderAsync(this.rootFolder.Id, this.cleanup_configuration);
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
            // await Api..UpdateCleanupConfigurationForRootFolder(rootFolder.Id, original_cleanup_configuration);
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
    FolderNode pathprotectionNode;
    protected API Api { get; }
    public AddPathProtectionCmd(API api, RootFolder rootFolder, FolderNode pathprotectionNode)
    {
        Api = api;
        this.pathprotectionNode = pathprotectionNode;
        this.rootFolder = rootFolder;
    }
    public override async Task<bool> Apply()
    {
        int path_retentiontype_id = rootFolder.RetentionTypes.PathRetentionType.Id;
        PathProtectionDTO? existing_pathprotection = rootFolder.FindPathProtection(pathprotectionNode);
        if (existing_pathprotection != null){
            // it already exists so nothing to add
            return true; // already exists
        }
        PathProtectionDTO? parent_protection = rootFolder.FindClosestPathProtectedParent(pathprotectionNode);
        Retention? parent_path_retention = parent_protection == null ? null : new Retention(path_retentiontype_id, parent_protection.Id);

        PathProtectionDTO new_path_protection = new PathProtectionDTO
        {
            RootfolderId = pathprotectionNode.RootfolderId,
            FolderId = pathprotectionNode.Id,
            Path = pathprotectionNode.FullPath
        };
        pathProtection = new_path_protection;

        PathProtectionDTO pathprotection = await Api.AddPathProtectionByRootFolderAsync(new_path_protection);
        if (pathprotection == null || pathprotection.Id == 0)
            throw new Exception("Failed to add path protection via API.");
        else {
            new_path_protection.Id = pathprotection.Id;            
            rootFolder.PathProtections.Add(new_path_protection);

            Retention new_path_retention = new Retention(path_retentiontype_id,  new_path_protection.Id);
            List<FolderRetention> retentionUpdates;
            if (parent_path_retention != null)
                retentionUpdates = await pathprotectionNode.ChangeRetentionsOfSubtree(new AddPathProtectionToParentPathProtectionDelegate(parent_path_retention, new_path_retention));
            else
                retentionUpdates = await pathprotectionNode.ChangeRetentionsOfSubtree(new AddPathProtectionOnMixedSubtreesDelegate(new_path_retention));

            List<FolderRetention> result = await Api.UpdateRootFolderRetentionsAsync(rootFolder.Id, retentionUpdates);
            if (result==null)
                throw new Exception("AddPathProtectionCmd:Failed to update retentions via API.");

            return true ;
        }
    }
    public override async Task<bool> Rollback()
    {
        if (pathProtection != null)
        {
            // Remove from client state
            rootFolder.PathProtections.RemoveAll(p => p.Id == pathProtection.Id);

            // Remove from server
            await Api.DeletePathProtectionByRootFolderAndPathProtectionAsync(rootFolder.Id, pathProtection.Id);

            // Update aggregation
            await rootFolder.UpdateAggregation();
        }
        return true;
    }
    protected override async Task OnFinally(bool wasSuccessful)
    {
        //Console.WriteLine($"AddPathProtectionCmd OnFinally entry . Success: {wasSuccessful}");
        // Always update aggregation regardless of success/failure
        await rootFolder.UpdateAggregation();
        //Console.WriteLine($"AddPathProtectionCmd OnFinally exit . Success: {wasSuccessful}");
    }
    protected override void Log(string action, bool wasSuccessful)
    {
        base.Log(action, wasSuccessful);

        if (action == "Apply" && wasSuccessful && pathProtection != null)
        {
            //Console.WriteLine($"Added PathProtection ID: {pathProtection.Id} for Folder ID: {pathProtection.FolderId}");
        }
        else if (action == "Apply" && !wasSuccessful)
        {
            Console.WriteLine($"Error: Failed to create path protection for folder {pathprotectionNode.Name} ({pathprotectionNode.FullPath})");
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
    FolderNode pathprotectionNode;
    int to_retention_Id;
    protected API Api { get; }
    public RemovePathProtectionCmd(API api, RootFolder rootFolder, FolderNode pathprotectionNode, int to_retention_Id)
    {
        Api = api;
        this.pathprotectionNode = pathprotectionNode;
        this.rootFolder = rootFolder;
        this.to_retention_Id = to_retention_Id;
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
        int path_retentiontype_id = rootFolder.RetentionTypes.PathRetentionType.Id;

        PathProtectionDTO? from_path_protection = rootFolder.PathProtections.FirstOrDefault(p => p.FolderId == pathprotectionNode.Id);
        if (from_path_protection == null)
            throw new ArgumentException("Invalid new path protection folder specified.");
        else{
            Retention from_path_retention = new Retention(path_retentiontype_id, from_path_protection.Id);

            // check for the presence of a parent of path_protection_folder before choosing the to_retention
            PathProtectionDTO? parent_path_protection = rootFolder.FindClosestPathProtectedParent(pathprotectionNode);
            Retention to_retention = parent_path_protection == null ? new Retention(to_retention_Id) : new Retention(path_retentiontype_id, parent_path_protection.Id);

            //change retention of client. Maybe we shoudl do this after server update instead?
            remove_count = rootFolder.PathProtections.RemoveAll(p => p.FolderId == pathprotectionNode.Id);
            List<FolderRetention> retentionUpdates = await pathprotectionNode.ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(from_path_retention, to_retention));

            //change retention on server. by removing the path protection first and then updating the retentions
            bool result = await Api.DeletePathProtectionByRootFolderAndPathProtectionAsync(rootFolder.Id, from_path_protection.Id);
            if (!result)
                throw new Exception("Failed to delete path protection via API.");
            
            List<FolderRetention> retention_results = await Api.UpdateRootFolderRetentionsAsync(rootFolder.Id, retentionUpdates);
            if (retention_results==null)
                throw new Exception("RemovePathProtectionCmd: Failed to update retentions via API.");

            return true;
        }
    }

    protected override async Task OnFinally(bool wasSuccessful)
    {
        //Console.WriteLine($"RemovePathProtectionCmd OnFinally entry . Success: {wasSuccessful}");
        // Always update aggregation regardless of success/failure
        await rootFolder.UpdateAggregation();
        //Console.WriteLine($"RemovePathProtectionCmd OnFinally exit . Success: {wasSuccessful}");
    }
    public override async Task<bool> Rollback()
    {
        //Console.WriteLine("Rollback of RemovePathProtectionCmd not implemented");
        await Task.Delay(0);
        return true;
    }
    protected override void Log(string action, bool wasSuccessful)
    {
        base.Log(action, wasSuccessful);

        if (action == "Apply" && wasSuccessful)
        {
            //Console.WriteLine($"Removed PathProtection for Folder: {pathprotectionNode.FullPath}");
        }
        else if (action == "Apply" && !wasSuccessful)
        {
            Console.WriteLine($"Error: Failed to remove path protection for folder {pathprotectionNode.FullPath}");
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
    protected API Api { get; }
    public ChangeRetentionsCmd(API api, RootFolder rootFolder, FolderNode folderNode, int from_retention_Id, int to_retention_Id)
    {
        Api = api;
        this.rootFolder = rootFolder;
        this.folderNode = folderNode;
        this.from_retention_Id = from_retention_Id;
        this.to_retention_Id = to_retention_Id;
    }

    public override async Task<bool> Apply()
    {
        List<FolderRetention> retentionUpdates = await folderNode.ChangeRetentionsOfSubtree(new ChangeOnFullmatchDelegate(new Retention(from_retention_Id), new Retention(to_retention_Id)));

        List<FolderRetention> results = await Api.UpdateRootFolderRetentionsAsync(rootFolder.Id, retentionUpdates);
        if (results==null)
            throw new Exception("ChangeRetentionId2IdCmd: Failed to update retentions via API.");

        return results!=null;
    }

    protected override async Task OnFinally(bool wasSuccessful)
    {
        //Console.WriteLine($"ChangeRetentionsCmd OnFinally entry . Success: {wasSuccessful}");
        // Move the existing finally logic here
        await rootFolder.UpdateAggregation();
        //Console.WriteLine($"ChangeRetentionsCmd OnFinally exit . Success: {wasSuccessful}");
    }
    public override async Task<bool> Rollback()
    {
        //Console.WriteLine("Rollback of ChangeRetentionsCmd not implemented");
        await Task.Delay(0);
        return true;
    }
    protected override void Log(string action, bool wasSuccessful)
    {
        base.Log(action, wasSuccessful);

        if (action == "Apply" && wasSuccessful)
        {
            //Console.WriteLine($"ChangeRetentionsCmd: {from_retention_Id} to {to_retention_Id}");
        }
        else if (action == "Apply" && !wasSuccessful)
        {
            Console.WriteLine($"ChangeRetentionsCmd: failed to change retention for {from_retention_Id} to {to_retention_Id}");
        }
    }
}