using System;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Components;
using Microsoft.FluentUI.AspNetCore.Components;
using VSM.Client.Datamodel;

namespace VSM.Client.Pages
{
    public partial class Fluent_Retention : ComponentBase
    {
        //Data for visualization
        VisibleTable visibleTable = new();
        bool isLoading = true;
        RetentionCell? selected_cell = null;
        RetentionCell? target_retention_cell = null;
        bool isProcessing = false;
        RetentionKey new_retention_key = new();
        //data from the DataModel
        public RootFolder? rootFolder { get; set; }

        protected override void OnInitialized()
        {
            isLoading = true;
            // Just set up initial state, don't block with async operations
            StateHasChanged();
            if (rootFolder != DataModel.Instance.SelectedRootFolder)
            {
                rootFolder = DataModel.Instance.SelectedRootFolder;
            }
        }
        protected override async Task OnAfterRenderAsync(bool firstRender)
        {
            if (firstRender && isLoading)
            {
                StateHasChanged();
                // Load both data and retention options
                await LoadDataAsync();
            }
            isLoading = false;
        }
        private async Task LoadDataAsync()
        {
            try
            {
                // Ensure UI shows loading state
                await InvokeAsync(StateHasChanged);

                // first load retention options because we need them to generate testdata and folder structure
                if (rootFolder != null)
                {
                    await rootFolder.LoadFolderRetentions();
                    visibleTable.SetVisibleRoot(rootFolder.FolderTree);
                    await rootFolder.UpdateAggregation();
                }
            }
            finally
            {
                await InvokeAsync(StateHasChanged);
            }
        }
        private void ToggleExpand(ViewNode VisibleRootNode, ViewNode node)
        {
            visibleTable.ToggleExpand(node);
        }
        private void OnCellClick(RetentionCell cell)
        {
            //selected_cell = (node,key);
            selected_cell = cell;
            target_retention_cell = null;
            if (new_retention_key == null)
                new_retention_key = new RetentionKey { Id = cell.retention_key.Id };
            else
                new_retention_key.Id = cell.retention_key.Id;
            Console.WriteLine($"Cell focused: {cell.Node.Name}, {cell.retention_key.Name}");
        }
        // the following update the retention for one cell and all its children
        // Concerning path protection. One pathprotection can be added and only one path protection can be removed
        // The dealing with hierachies of path protection must be done on the userinterface so that it is explicit for the user what will happen
        private async Task OnRetentionChangedAsync()
        {
            try
            {
                if (selected_cell != null && new_retention_key != null && rootFolder != null)
                {
                    StateHasChanged(); // Update UI to show progress
                    isProcessing = true;

                    //start by verifying if we need to change the list of pathprotections
                    if (new_retention_key.Id == rootFolder.RetentionConfiguration.Path_retentiontype.Id)
                    {
                        //add a path protection if it doesn't already exist
                        if (rootFolder.RetentionConfiguration.Path_protections.Any(p => p.Folder_Id == selected_cell.Node.Id))
                        {
                            Console.WriteLine($"Path protection already exists for folder {selected_cell.Node.Name} ({selected_cell.Node.FullPath})");
                            return;
                        }

                        // Create and persist the new path protection in order to get an ID assigned
                        var path_protection = await rootFolder.AddPathProtection(selected_cell.Node, rootFolder.RetentionConfiguration);
                        if (path_protection == null)//|| path_protection.Id == 0)
                        {
                            Console.WriteLine($"Error: Failed to create path protection for folder {selected_cell.Node.Name} ({selected_cell.Node.FullPath})");
                            return;
                        }
                        Console.WriteLine($"OnRetentionChangedAsync: {selected_cell.retention_key.Id}, to {new_retention_key.Id_AsString} and path retention {path_protection.Path}");
                    }
                    else if (selected_cell.retention_key.Id == rootFolder.RetentionConfiguration.Path_retentiontype.Id)
                    {
                        //remove existing path protection.
                        int remove_count = await rootFolder.RemovePathProtection(selected_cell.Node, rootFolder.RetentionConfiguration, new_retention_key.Id);
                        Console.WriteLine($"OnRetentionChangedAsync: {selected_cell.retention_key.Id}, {(new_retention_key != null ? new_retention_key.Id.ToString() : "null")} count of remove pathprotections {remove_count}");
                        //no removal happened so abandon the update
                        if (remove_count == 0)
                            return;
                    }
                    else if (new_retention_key != null) // case for change of retention that does not involved pathRetention
                    {
                        await selected_cell.Node.ChangeRetentions(new Retention(selected_cell.retention_key.Id), new Retention(new_retention_key.Id));
                        Console.WriteLine($"OnRetentionChangedAsync: {selected_cell.retention_key.Id}, {(new_retention_key != null ? new_retention_key.Id.ToString() : "null")}");
                    }

                    // All done for the selected_cell. 
                    // Now show where the change has gone to by assigning target_retention_cell
                    if (new_retention_key != null)
                    {
                        selected_cell.retention_key.Id = new_retention_key.Id; // Update the selected cell's retention key
                        selected_cell.retention_key.Name = rootFolder.RetentionConfiguration.Find_by_Id(new_retention_key.Id)?.Name ?? "unknown";
                        target_retention_cell = selected_cell;
                        selected_cell = null;
                    }

                    // Update the aggregation from the root folder. 
                    // This could be optimsed by only updating the modifed branch and its ancestors
                    await rootFolder.UpdateAggregation();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error syncing retentions: {ex.Message}");
            }
            finally
            {
                //ensure that the user can see the changes
                isProcessing = false;
                await InvokeAsync(StateHasChanged);
            }
        }

        private async Task SelectPathRetention(PathProtectionDTO pathprotection)
        {
            FolderNode? folder = rootFolder == null ? null : await rootFolder.FolderTree.find_by_folder_id(pathprotection.Folder_Id);
            if (rootFolder != null && folder != null && visibleTable.VisibleRootNode != null)
            {
                //unfolder the VisibleRows to show folder and select the node where the pathprotection is defined
                visibleTable.ExpandToNode(folder);
                selected_cell = new RetentionCell(folder, rootFolder.RetentionConfiguration.Path_retentiontype);
                target_retention_cell = null;
                new_retention_key.Id = rootFolder.RetentionConfiguration.Path_retentiontype.Id;
            }
            else
            {
                Console.WriteLine($"failed to select retention for pathprotection {pathprotection.Id}, {pathprotection.Path}");
            }
        }
    }

    class RetentionKey
    {
        public byte Id = 0;
        public string Id_AsString
        {
            get
            {
                return Id.ToString();
            }
            set
            {
                Id = byte.TryParse(value, out byte byte_value) ? byte_value : Id;
            }
        }
    }

    //client node with fields to manage display and navigation
    public class ViewNode
    {
        protected FolderNode data;
        public ViewNode(FolderNode data)
        {
            this.data = data;
        }
        public FolderNode Data => data;
        public bool IsLeaf { get { return data.IsLeaf; } }
        public int Level { get; set; } = 0;
        public bool IsExpanded { get; set; } = false;
        public string Name => data.Name;
        public List<ViewNode> Children = [];

    }
    public class VisibleTable
    {
        public ViewNode? VisibleRootNode = null;
        public List<ViewNode> VisibleRows = new();

        public void SetVisibleRoot(FolderNode root)
        {

            VisibleRootNode = new ViewNode(root);
            VisibleRootNode.IsExpanded = false; // Expand the root node by default
            VisibleRootNode.Level = 0; // Set the root level to 0

            VisibleRows.Clear();
            ToggleExpand(VisibleRootNode); // Expand the root node to show its children
        }
        public void AddVisibleNode(ViewNode node)
        {
            node.IsExpanded = !node.IsExpanded;
            if (!node.IsExpanded)
            {
                node.Children.Clear(); // Collapse the node by clearing its children
            }
            else if (node.IsExpanded && !node.IsLeaf &&
                node.Children.Count == 0 && node.Data.Children.Count > 0)
            {
                // If the node is expanded and has children, flatten its children
                // add the children as new ViewNodes 
                foreach (var child in node.Data.Children)
                {
                    // Create a new ViewNode for each child
                    var childNode = new ViewNode(child);
                    childNode.IsExpanded = false; // Keep the child node expanded
                    childNode.Level = node.Level + 1; // Increment level for children
                    node.Children.Add(childNode);
                }
            }
        }
        public void CollapseNoRefresh(ViewNode node)
        {
            if (node.IsExpanded)
            {
                node.IsExpanded = false;
                node.Children.Clear();
            }
        }
        public void ExpandNoRefresh(ViewNode node)
        {
            if ((!node.IsExpanded && !node.IsLeaf) || node.Children.Count != node.Data.Children.Count)
            {
                node.Children.Clear();

                // If the node is expanded and has children then add the children as new ViewNodes 
                foreach (var child in node.Data.Children)
                {
                    // Create a new ViewNode for each child
                    var childNode = new ViewNode(child);
                    childNode.IsExpanded = false; // Keep the child node expanded
                    childNode.Level = node.Level + 1; // Increment level for children
                    node.Children.Add(childNode);
                }
                node.IsExpanded = true;
            }
        }
        public void ToggleExpand(ViewNode node)
        {
            if (node.IsExpanded)
                CollapseNoRefresh(node);
            else
                ExpandNoRefresh(node);

            RefreshVisibleRows();
        }
        /// <summary>
        ///  The refresh is required in order to show the updated folder structure in order.
        /// without the refresh expanding a node would result in the expanded children being shown at the end of the list
        /// </summary>
        public void RefreshVisibleRows()
        {
            VisibleRows.Clear();

            if (VisibleRootNode != null)
            {
                // In-order traversal without recursion
                Stack<ViewNode> stack = new();
                stack.Push(VisibleRootNode);

                while (stack.Count > 0)
                {
                    var currentNode = stack.Pop();
                    VisibleRows.Add(currentNode);

                    // Push children onto the stack in reverse order to maintain in-order traversal
                    for (int i = currentNode.Children.Count - 1; i >= 0; i--)
                    {
                        stack.Push(currentNode.Children[i]);
                    }
                }
            }
        }
        public void ExpandToNode(FolderNode node)
        {
            //create a hashlist for fast lookup which folder are already expanded. Basically a look of all id ViewNode.Data.Id list for 
            HashSet<int> visiblefolder_Ids = VisibleRows.Select(r => r.Data.Id).ToHashSet();
            // print the visible folder ids to the console for debugging
            Console.WriteLine($"Visible folder IDs ({visiblefolder_Ids.Count}): {string.Join(", ", visiblefolder_Ids)}");

            //add all nodes (node and its parents) that are not expanded (that not in the expandedFolders) to a list closed nodes. 
            //stop at the first node found in expandedFolders
            var closedNodes = new Stack<FolderNode>();
            FolderNode? current = node;
            while (current != null && !visiblefolder_Ids.Contains(current.Id))
            {
                closedNodes.Push(current);
                current = current.Parent;
                if (current != null) Console.WriteLine($"current: ID:{current.Id}, Name: {current.Name}, IsVisible:{visiblefolder_Ids.Contains(current.Id)}");
            }

            // Find the viewnode for current which is the highest ViewNode with one of node's parent FolderNodes
            ViewNode? currentViewNode = current == null ? null : VisibleRows.FirstOrDefault(v => v.Data.Id == current.Id);
            // Continue while we can find children to expand
            FolderNode? child = closedNodes.Count > 0 ? closedNodes.Pop() : null;
            while (currentViewNode != null && child != null)
            {
                ExpandNoRefresh(currentViewNode);

                currentViewNode = currentViewNode.Children.FirstOrDefault(v => v.Data.Id == child.Id);
                child = closedNodes.Count > 0 ? closedNodes.Pop() : null;
            }
            RefreshVisibleRows();
        }
    }
    public class RetentionCell : IEquatable<RetentionCell>
    {
        public FolderNode Node { get; set; }
        public RetentionTypeDTO retention_key { get; set; }
        public RetentionCell(FolderNode node, RetentionTypeDTO retention_key)
        {
            this.Node = node;
            this.retention_key = new RetentionTypeDTO
            {
                Id = retention_key.Id,
                Name = retention_key.Name,
            };
        }
        public override bool Equals(Object? other)
        {
            if (other is RetentionCell otherCell)
            {
                return Equals(otherCell);
            }
            return false;
        }
        public bool Equals(RetentionCell? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;

            return Node?.Id == other.Node?.Id && retention_key.Id == other.retention_key.Id;
        }
        public override int GetHashCode()
        {
            return HashCode.Combine(Node.GetHashCode(), retention_key.Id);
        }
        public static bool operator ==(RetentionCell? left, RetentionCell? right)
        {
            if (left is null) return right is null;
            return left.Equals(right);
        }
        public static bool operator !=(RetentionCell? left, RetentionCell? right)
        {
            return !(left == right);
        }
    }
}