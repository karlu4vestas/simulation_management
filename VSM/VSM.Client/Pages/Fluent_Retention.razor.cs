using System.Diagnostics.Contracts;
using Microsoft.AspNetCore.Components;
using Microsoft.FluentUI.AspNetCore.Components;
using VSM.Client.Datamodel;

namespace VSM.Client.Pages
{

    public partial class Fluent_Retention : ComponentBase
    {
        //Data for visualization
        FluentDataGrid<ViewNode>? grid;
        private ViewNode? VisibleRootNode = null;
        private List<ViewNode> VisibleRows = new();
        private bool isLoading = true;
        private bool hasRendered = false;
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
                    if (rootFolder != null)
                    {
                        await LoadRootFolderTreeAsync(rootFolder);
                        await rootFolder.UpdateAggregation();
                    }
                }
            }
            finally
            {
                VisibleRootNode = new ViewNode(TreeData.First());
                VisibleRootNode.IsExpanded = false; // Expand the root node by default
                VisibleRootNode.Level = 0; // Set the root level to 0
                ToggleExpand(VisibleRootNode); // Expand the root node to show its children

                //RefreshVisibleRows();
                isLoading = false;
                await InvokeAsync(StateHasChanged);
            }
        }

        private void ToggleExpand(ViewNode node)
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
            RefreshVisibleRows();
        }

        private void RefreshVisibleRows()
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
        public class ViewNode
        {
            protected FolderNode data;

            public ViewNode(FolderNode data)
            {
                this.data = data;
            }

            public FolderNode Data => data;

            //@todo client side help fields for display and navigation
            public bool IsLeaf { get { return data.IsLeaf; } }
            public int Level { get; set; } = 0;
            public bool IsExpanded { get; set; } = false;
            public string Name => data.Name;
            public List<ViewNode> Children = new();
        }

        public class RetentionCell : IEquatable<RetentionCell>
        {
            public FolderNode Node { get; set; }
            public RetentionType retention_key { get; set; }
            public RetentionType new_retention_key { get; set; }
            List<RetentionType> _retentionOptions;
            public string new_retention_key_string
            {
                get
                {
                    return new_retention_key.Id.ToString();
                }
                set
                {
                    new_retention_key.Id = byte.TryParse(value, out byte byte_value) ? byte_value : new_retention_key.Id;
                    new_retention_key.Name = _retentionOptions.FirstOrDefault(o => o.Id == new_retention_key.Id)?.Name ?? string.Empty;
                }
            }
            public RetentionCell(FolderNode Node, RetentionType retention_key, List<RetentionType> retentionOptions)
            {
                this._retentionOptions = retentionOptions;
                this.Node = Node;
                this.retention_key = new RetentionType
                {
                    Id = retention_key.Id,
                    Name = retention_key.Name,
                };
                this.new_retention_key = new RetentionType
                {
                    Id = retention_key.Id,
                    Name = retention_key.Name,
                };
            }

            public bool Equals(RetentionCell? other)
            {
                if (other is null) return false;
                if (ReferenceEquals(this, other)) return true;

                return Node?.Id == other.Node?.Id && retention_key.Id == other.retention_key.Id;
            }
            public override bool Equals(object? obj)
            {
                return obj is RetentionCell other && Equals(other);
            }
            public override int GetHashCode()
            {
                return HashCode.Combine(Node?.Id, retention_key.Id);
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

            public async Task SyncRetentions()
            {
                if (retention_key.Id != new_retention_key.Id)
                {
                    // Console.WriteLine($"sync the TreeNode tree' retentions {Node.Name} retention {retention_key.Id} new_retention {new_retention_key.Id}");
                    // Change retention for all leaf nodes in this inner node without recursion
                    await Node.ChangeRetentions(retention_key, new_retention_key);
                    retention_key.Id = new_retention_key.Id;
                    retention_key.Name = new_retention_key.Name;
                }
            }
        }
        RetentionCell? selected_cell = null;
        private bool isProcessing = false;


        private void OnCellClick(RetentionCell cell)
        {
            //selected_cell = (node,key);
            selected_cell = cell;
            Console.WriteLine($"Cell focused, clickCount: {cell.Node.Name}, {cell.retention_key.Name}");
        }
        private async Task OnRetentionChangedAsync()
        {
            if (selected_cell != null)
            {
                isProcessing = true;
                StateHasChanged(); // Update UI to show progress

                try
                {
                    await selected_cell.SyncRetentions();

                    // U    pdate the aggregation from the root folder. 
                    // This could be uptimsed by only updating the modifed branch and the parente
                    if (rootFolder != null)
                        await rootFolder.UpdateAggregation();
                    RefreshVisibleRows();
                    await InvokeAsync(StateHasChanged);
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
}