using System.Threading.Tasks.Dataflow;
using Microsoft.FluentUI.AspNetCore.Components;
using VSM.Client.Datamodel;

namespace VSM.Client.Pages
{
    public partial class LibraryPage
    {
        public class NamedPeriod
        {
            public string Name { get; set; } = "";
            public int Days { get; set; }
        }
        public class NamedPeriodConverter
        {
            private readonly string _defaultName = "";
            private readonly int _defaultDays = 0;
            private List<NamedPeriod> _itemList = new List<NamedPeriod>();
            private readonly Dictionary<string, int> _nameToDays = new();
            private readonly Dictionary<int, string> _daysToName = new();
            public NamedPeriodConverter() { }
            public NamedPeriodConverter(IEnumerable<NamedPeriod> items)
            {
                _itemList = items.ToList();
                // store first entry as fallback if any
                if (_itemList.Count > 0)
                {
                    _defaultName = _itemList[0].Name;
                    _defaultDays = _itemList[0].Days;
                }
                _nameToDays = _itemList.ToDictionary(x => x.Name, x => x.Days);
                _daysToName = _itemList.ToDictionary(x => x.Days, x => x.Name);
            }
            public List<string> Names() => _nameToDays.Keys.ToList();
            public int NameToDays(string name) => _nameToDays.TryGetValue(name, out var days) ? days : _defaultDays;
            public string DaysToName(int days) => _daysToName.TryGetValue(days, out var name) ? name : _defaultName;
        }

        static int pagevisits = 0;
        string user_name = "";
        bool has_loaded_rootFolders = false;
        string frequency_name = "";
        string cycle_time_name = "";
        NamedPeriodConverter frequencyConverter = new NamedPeriodConverter();
        NamedPeriodConverter cycleTimeConverter = new NamedPeriodConverter();
        protected override void OnInitialized()
        {
            pagevisits++;
            Console.WriteLine("OnInitialized - Page visits: " + pagevisits);
            Console.WriteLine("user_name: " + user_name);
            Console.WriteLine("LibraryPage initialized");
            Console.WriteLine("has_loaded_rootFolders: " + has_loaded_rootFolders);
            Console.WriteLine("frequency_name: " + frequency_name);
            Console.WriteLine("cycle_time_name: " + cycle_time_name);
            Console.WriteLine("frequencies available: " + string.Join(", ", frequencyConverter.Names()));
            Console.WriteLine("cycle times available: " + string.Join(", ", cycleTimeConverter.Names()));
        }
        async Task OnLoginClicked()
        {
            if (user_name.Length > 0)
            {
                Library.Instance.User = user_name;
                try
                {
                    has_loaded_rootFolders = false;
                    // Ensure UI shows loading state. well later if we implement it
                    await InvokeAsync(StateHasChanged);

                    // first load retention options because we need them to generate testdata and folder structure
                    await Library.Instance.Load();
                    List<NamedPeriod> frequencyItems = Library.Instance.CleanupFrequencies.Select(x => new NamedPeriod
                    {
                        Name = x.Name,
                        Days = x.Days
                    }).ToList();
                    frequencyConverter = new NamedPeriodConverter(frequencyItems);
                    List<NamedPeriod> cycleTimeItems = Library.Instance.CycleTimes.Select(x => new NamedPeriod
                    {
                        Name = x.Name,
                        Days = x.Days
                    }).ToList();
                    cycleTimeConverter = new NamedPeriodConverter(cycleTimeItems);

                    has_loaded_rootFolders = true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error loading root folders: {ex.Message}");
                    has_loaded_rootFolders = true;
                }
                finally
                {
                    await InvokeAsync(StateHasChanged);
                }
            }
        }
        //selecte the folder for the details panel
        private void OnSelectRootFolder(FluentDataGridRow<RootFolder> row)
        {
            if (row.Item != null)
            {
                Library.Instance.SelectedRootFolder = row.Item;
                RootFolder rootFolder = row.Item;
                frequency_name = frequencyConverter.DaysToName(rootFolder.CleanupConfiguration.Frequency);
                cycle_time_name = cycleTimeConverter.DaysToName(rootFolder.CleanupConfiguration.Lead_time);
            }
            StateHasChanged();
        }
        private async Task UpdateCleanUpConfiguration()
        {
            RootFolder? rootFolder = Library.Instance.SelectedRootFolder;
            if (rootFolder == null)
                return;

            rootFolder.CleanupConfiguration.Frequency = frequencyConverter.NameToDays(frequency_name);
            rootFolder.CleanupConfiguration.Lead_time = cycleTimeConverter.NameToDays(cycle_time_name);
            if (rootFolder.CleanupConfiguration.Frequency > 0 && rootFolder.CleanupConfiguration.Lead_time > 0 && rootFolder.CleanupConfiguration.Start_date == null)
            {
                rootFolder.CleanupConfiguration.Start_date = DateTime.Now;
            }

            if (rootFolder.CleanupConfiguration.IsValid)
            {
                UpdateCleanupConfigurationCmd cmd = new UpdateCleanupConfigurationCmd(rootFolder, rootFolder.CleanupConfiguration);
                bool success = await cmd.Apply();
                if (!success)
                {
                    Console.WriteLine("Failed to update cleanup configuration.");
                }
                StateHasChanged();
                Console.WriteLine($"new frequency:{Library.Instance.SelectedRootFolder?.CleanupConfiguration}");
            }
        }
        private void Go2retention(RootFolder root_folder)
        {
            Library.Instance.SelectedRootFolder = root_folder;
            Navigation.NavigateTo("retention");
        }
    }
}