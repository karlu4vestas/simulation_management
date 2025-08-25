using System.Diagnostics.Contracts;
using Microsoft.AspNetCore.Components;
using Microsoft.FluentUI.AspNetCore.Components;
using VSM.Client.Datamodel;

namespace VSM.Client.Pages
{

    public partial class Fluent_Retention : ComponentBase
    {
        //Data for visualization
        private ViewNode? VisibleRootNode = null;
        private List<ViewNode> VisibleRows = new();
        private bool isLoading = true;
        private bool hasRendered = false;
        RetentionCell? selected_cell = null;
        RetentionCell? target_retention_cell = null;
        private bool isProcessing = false;
        RetentionKey new_retention_key = new RetentionKey();

        //data from the DataModel
        private List<FolderNode> TreeData = new();
        public RootFolder? rootFolder { get; set; }
        private List<RetentionType> retentionOptions = new();

        protected override void OnInitialized()
        {
            // Just set up initial state, don't block with async operations
            if (rootFolder != DataModel.Instance.SelectedRootFolder)
            {
                rootFolder = DataModel.Instance.SelectedRootFolder;
                // Don't await here - let the UI render first
            }
        }
        protected override async Task OnAfterRenderAsync(bool firstRender)
        {
            if (firstRender && !hasRendered)
            {
                hasRendered = true;
                // Load both data and retention options
                await LoadDataAsync();
            }
        }
        private async Task LoadRetentionOptionsAsync()
        {
            try
            {
                retentionOptions = await DataModel.Instance.GetRetentionOptionsAsync();
                await InvokeAsync(StateHasChanged);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading retention options: {ex.Message}");
                retentionOptions = new List<RetentionType>();
            }
        }
        private async Task LoadRootFolderTreeAsync(RootFolder rootFolder)
        {
            try
            {
                if (rootFolder != null)
                {
                    var folderTree = await rootFolder.GetFolderTreeAsync();
                    if (folderTree == null)
                    {
                        TreeData.Clear();
                    }
                    else if (!folderTree.IsLeaf)
                    {
                        TreeData = folderTree.Children;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading RootFolderTree: {ex.Message}");
            }
        }
        private async Task LoadDataAsync()
        {
            try
            {
                // Ensure UI shows loading state
                await InvokeAsync(StateHasChanged);

                // first load retention options because we need them to generate testdata and folder structure
                await LoadRetentionOptionsAsync();

                if (rootFolder != null)
                {
                    await LoadRootFolderTreeAsync(rootFolder);
                    await rootFolder.UpdateAggregation();
                }
            }
            finally
            {
                VisibleRootNode = new ViewNode(TreeData.First());
                VisibleRootNode.IsExpanded = false; // Expand the root node by default
                VisibleRootNode.Level = 0; // Set the root level to 0
                VisibleRows = ViewNode.ToggleExpand(VisibleRootNode, VisibleRootNode); // Expand the root node to show its children

                //RefreshVisibleRows();
                isLoading = false;
                await InvokeAsync(StateHasChanged);
            }
        }
        private void ToggleExpand(ViewNode VisibleRootNode, ViewNode node)
        {
            VisibleRows = ViewNode.ToggleExpand(VisibleRootNode, node);
        }
        private void OnCellClick(RetentionCell cell)
        {
            //selected_cell = (node,key);
            selected_cell = cell;
            target_retention_cell = null;
            new_retention_key.Id = cell.retention_key.Id;
            Console.WriteLine($"Cell focused: {cell.Node.Name}, {cell.retention_key.Name}");
        }
        private async Task OnRetentionChangedAsync()
        {
            if (selected_cell != null)
            {
                isProcessing = true;
                Console.WriteLine($"OnRetentionChangedAsync: {selected_cell.retention_key.Id}, {new_retention_key.Id}");
                StateHasChanged(); // Update UI to show progress
                try
                {
                    await selected_cell.Node.ChangeRetentions(selected_cell.retention_key.Id, new_retention_key.Id);
                    selected_cell.retention_key.Id = new_retention_key.Id; // Update the selected cell's retention key

                    // Update the aggregation from the root folder. 
                    // This could be optimsed by only updating the modifed branch and the parente
                    if (rootFolder != null)
                        await rootFolder.UpdateAggregation();

                    if (VisibleRows != null && VisibleRootNode != null)
                        VisibleRows = ViewNode.RefreshVisibleRows(VisibleRootNode);

                    await InvokeAsync(StateHasChanged);
                    target_retention_cell = selected_cell;
                    selected_cell = null;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error syncing retentions: {ex.Message}");
                }
                finally
                {
                    isProcessing = false;
                    StateHasChanged(); // Hide progress indicator
                }
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
        public static List<ViewNode> ToggleExpand(ViewNode VisibleRootNode, ViewNode node)
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
                //add the children as new ViewNodes 
                foreach (var child in node.Data.Children)
                {
                    // Create a new ViewNode for each child
                    var childNode = new ViewNode(child);
                    childNode.IsExpanded = false; // Keep the child node expanded
                    childNode.Level = node.Level + 1; // Increment level for children
                    node.Children.Add(childNode);
                }
            }
            return RefreshVisibleRows(VisibleRootNode);
        }
        public static List<ViewNode> RefreshVisibleRows(ViewNode VisibleRootNode)
        {
            List<ViewNode> VisibleRows = [];

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
            return VisibleRows;
        }
    }

    public class RetentionCell : IEquatable<RetentionCell>
    {
        public FolderNode Node { get; set; }
        public RetentionType retention_key { get; set; }
        public RetentionCell(FolderNode Node, RetentionType retention_key)
        {
            this.Node = Node;
            this.retention_key = new RetentionType
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