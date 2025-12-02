using Microsoft.AspNetCore.Components;
using VSM.Client.Datamodel;
using VSM.Client.SharedAPI;
using System.Threading.Tasks;

namespace VSM.Client.Pages
{
    public partial class RetentionPage : ComponentBase
    {
        //Data for visualization
        VisibleTable visibleTable = new();
        bool isLoading = true;
        RetentionCell? selected_cell = null;
        RetentionCell? target_retention_cell = null;
        bool isProcessing = false;
        public RootFolder rootFolder = null!;
        public RetentionTypeDTO? selectedRetentionOption = null;
        public RetentionTypeDTO? retentionCallback
        {
            get => selectedRetentionOption;
            set
            {
                selectedRetentionOption = value;
                //call async but uses a local discard task wrapper to catch exceptions.
                _ = OnRetentionChangedAsync(value).ContinueWith(t => {
                    if (t.Exception != null)
                        Console.Error.WriteLine(t.Exception);
                });           
            }
        }

        protected override void OnInitialized()
        {
            isLoading = true;
            //retentionCallback = new RetentionCallback(this);
            if( NavService.CurrentRootFolder==null )
            {
                Navigation.NavigateTo("library");
                return;
            } else{
                rootFolder = NavService.CurrentRootFolder ;
            }
            // Just set up initial state, don't block with async operations
            StateHasChanged();
        }
        protected override async Task OnAfterRenderAsync(bool firstRender)
        {
            if (firstRender && isLoading && rootFolder != null)
            {
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
        private async Task OnRetentionChangedAsync(RetentionTypeDTO? selectedRetentionOption)
        {
            // the following update the retention for one cell and all its children
            // Concerning path protection. One pathprotection can be added and only one path protection can be removed
            // The dealing with hierachies of path protection must be done on the userinterface so that it is explicit for the user what will happen
            
            // Fallbaclin case false==cmd.ExecuteAsync is to provoke a reload of the data from the server by sending the user 
            // to the library page. Not elegant but it works for now.
            try
            {
                if (selected_cell != null && selectedRetentionOption != null && rootFolder != null)
                {
                    isProcessing = true;
                    await InvokeAsync(StateHasChanged);

                    //start by verifying if we need to change the list of pathprotections
                    if (selectedRetentionOption.Id == rootFolder.RetentionTypes.PathRetentionType.Id)
                    {
                        //add a path protection if it doesn't already exist
                        if (rootFolder.PathProtections.Any(p => p.FolderId == selected_cell.Node.Id))
                        {
                            Console.WriteLine($"Path protection already exists for folder {selected_cell.Node.Name} ({selected_cell.Node.FullPath})");
                            return;
                        }

                        AddPathProtectionCmd cmd = new AddPathProtectionCmd(Api, rootFolder, selected_cell.Node);
                        if (false == await cmd.ExecuteAsync()){
                            Navigation.NavigateTo("library");
                            return;
                        }
                    }
                    else if (selected_cell.retention.Id == rootFolder.RetentionTypes.PathRetentionType.Id)
                    {
                        //remove existing path protection.
                        RemovePathProtectionCmd cmd = new RemovePathProtectionCmd(Api,rootFolder, selected_cell.Node, selectedRetentionOption.Id);
                        if (false == await cmd.ExecuteAsync()){
                            Navigation.NavigateTo("library");
                            return;
                        }
                    }
                    else //if (selectedOption != null) // case for change of retention that does not involved pathRetention
                    {
                        ChangeRetentionsCmd cmd = new ChangeRetentionsCmd(Api,rootFolder, selected_cell.Node, selected_cell.retention.Id, selectedRetentionOption.Id);
                        if (false == await cmd.ExecuteAsync()){
                            Navigation.NavigateTo("library");
                            return;
                        }
                    }

                    // All done for the selected_cell. Now use target_retention_cell to show where the change has gone to
                    if (selectedRetentionOption != null)
                    {
                        //Console.WriteLine($"Selected retention option before change: {selectedRetentionOption.Name} ({selectedRetentionOption.Id})");
                        selected_cell.retention =new RetentionTypeDTO
                        {
                            Id = selectedRetentionOption.Id,
                            Name = selectedRetentionOption.Name
                        };
                        target_retention_cell = selected_cell;
                        selected_cell = null;
                    }
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
                StateHasChanged();
                await InvokeAsync(StateHasChanged);
            }
        }
        private void OnCellClick(RetentionCell cell)
        {
            selected_cell = cell;
            target_retention_cell = null;
            if( rootFolder!=null)                                     
            {   
                selectedRetentionOption = rootFolder.RetentionTypes.FindById( cell.retention.Id );
            }
        }
        private async Task SelectPathRetention(PathProtectionDTO pathprotection)
        {
            FolderNode? folder = rootFolder == null ? null : await rootFolder.FolderTree.FindByFolderId(pathprotection.FolderId);
            if (rootFolder != null && folder != null && visibleTable.VisibleRootNode != null)
            {
                //unfolde the VisibleRows to show folder and select the node where the pathprotection is defined
                visibleTable.ExpandToNode(folder);
                selected_cell = new RetentionCell(folder, rootFolder.RetentionTypes.PathRetentionType);
                target_retention_cell = null;
                selectedRetentionOption = rootFolder.RetentionTypes.PathRetentionType;
                await InvokeAsync(StateHasChanged);
            }
            else
            {
                Console.WriteLine($"failed to select retention for pathprotection {pathprotection.Id}, {pathprotection.Path}");
            }
        }
        private async Task AddPathRetention(RetentionCell cell)
        {
            //Console.WriteLine($"AddPathRetention for a child to a pathprotectionfolder: {cell.Node.Name}, {cell.retention.Name}");
            if( cell.retention.Id==rootFolder.RetentionTypes.PathRetentionType.Id){
                AddPathProtectionCmd cmd = new AddPathProtectionCmd(Api, rootFolder, cell.Node);
                if (false == await cmd.ExecuteAsync())
                    return;
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
            //Console.WriteLine($"Visible folder IDs ({visiblefolder_Ids.Count}): {string.Join(", ", visiblefolder_Ids)}");

            //add all nodes (node and its parents) that are not expanded (that not in the expandedFolders) to a list closed nodes. 
            //stop at the first node found in expandedFolders
            var closedNodes = new Stack<FolderNode>();
            FolderNode? current = node;
            while (current != null && !visiblefolder_Ids.Contains(current.Id))
            {
                closedNodes.Push(current);
                current = current.Parent;
                //if (current != null) Console.WriteLine($"current: ID:{current.Id}, Name: {current.Name}, IsVisible:{visiblefolder_Ids.Contains(current.Id)}");
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
        //The retention cell is a place in the grid (foldernode rows vs retention columns)
        public FolderNode Node { get; set; }
        public RetentionTypeDTO retention { get; set; }
        public RetentionCell(FolderNode node, RetentionTypeDTO retention_key)
        {
            this.Node = node;
            this.retention = new RetentionTypeDTO
            {
                Id = retention_key.Id,
                Name = retention_key.Name,
            };
        }
        public bool CanChangeRetention()
        {
            //returns true if the retention can be changed
            return this.retention.Name != "Clean" && this.retention.Name != "Missing";
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

            return Node?.Id == other.Node?.Id && retention.Id == other.retention.Id;
        }
        public override int GetHashCode()
        {
            return HashCode.Combine(Node.GetHashCode(), retention.Id);
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