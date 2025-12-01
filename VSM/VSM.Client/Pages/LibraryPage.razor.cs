using System.Threading.Tasks.Dataflow;
using Microsoft.FluentUI.AspNetCore.Components;
using VSM.Client.Datamodel;
using System.ComponentModel.DataAnnotations;
//using VSM.Client.SharedAPI

namespace VSM.Client.Pages
{
    public partial class LibraryPage
    {
        //FluentCombobox<string> cycleTimeComboBox = null!;
        static int pagevisits = 0;
        string user_name = "";
        bool has_loaded_rootFolders = false;
        NamedPeriodConverter frequencyConverter = new NamedPeriodConverter();
        NamedPeriodConverter cycleTimeConverter = new NamedPeriodConverter();
        public CleanupFrequencyDTO? selectedFrequencyOption = null;
        public CleanupFrequencyDTO? frequencyCallback
        {
            get => selectedFrequencyOption;
            set{
                selectedFrequencyOption = value;
                //call async but uses a local discard task wrapper to catch exceptions.
                _ = OnFrequencyChangedAsync(value).ContinueWith(t => {
                    if (t.Exception != null)
                        Console.Error.WriteLine(t.Exception);
                });           
            }
        }
        public LeadTimeDTO? selectedLeadTimeOption = null;
        public LeadTimeDTO? leadTimeCallback
        {
            get => selectedLeadTimeOption;
            set{
                selectedLeadTimeOption = value;
                //call async but uses a local discard task wrapper to catch exceptions.
                _ = OnLeadTimeChangedAsync(value).ContinueWith(t => {
                    if (t.Exception != null)
                        Console.Error.WriteLine(t.Exception);
                });           
            }
        }
        
        Library library = null!;
        RootFolder? selected_rootFolder = null;
        protected override void OnInitialized()
        {
            library = new Library(Api);
            pagevisits++;
        }
        async Task OnLoginClicked()
        {
            if (user_name.Length > 0)
            {
                library.User = user_name;
                try
                {
                    has_loaded_rootFolders = false;
                    // Ensure UI shows loading state. well later if we implement it
                    await InvokeAsync(StateHasChanged);

                    // first load retention options because we need them to generate testdata and folder structure
                    await library.Load();
                    frequencyConverter = new NamedPeriodConverter(library.CleanupFrequencies.Select(x => new NamedPeriod(x.Name, x.Days)));
                    cycleTimeConverter = new NamedPeriodConverter(library.CycleTimes.Select(x => new NamedPeriod(x.Name, x.Days)));

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
                selected_rootFolder = row.Item;
                CleanupFrequencyDTO? freq = library.CleanupFrequencies.FirstOrDefault(f => f.Days == selected_rootFolder.CleanupConfiguration.Frequency);
                int leadtime_corrected = selected_rootFolder.CleanupConfiguration.LeadTime; 
                if (leadtime_corrected == 0) 
                    leadtime_corrected = -1;
                LeadTimeDTO? leadtime = library.CycleTimes.FirstOrDefault(c => c.Days == leadtime_corrected);
                selectedFrequencyOption = freq;
                selectedLeadTimeOption = leadtime;
            } else
            {
                selected_rootFolder = null;
                selectedFrequencyOption = null;
                selectedLeadTimeOption = null;
            }
            StateHasChanged();
        }
        private async Task OnFrequencyChangedAsync(CleanupFrequencyDTO? frequency)
        {
            if (frequency != null)
                await UpdateCleanUpConfiguration();
        }
        private async Task OnLeadTimeChangedAsync(LeadTimeDTO? leadtime)
        {
            if (leadtime != null)
                await UpdateCleanUpConfiguration();
        }
        private async Task UpdateCleanUpConfiguration()
        {
            if (selected_rootFolder == null)
                return;

            selected_rootFolder.CleanupConfiguration.Frequency = frequencyCallback?.Days ?? -1;
            selected_rootFolder.CleanupConfiguration.LeadTime  = leadTimeCallback?.Days ?? -1;
            if (selected_rootFolder.CleanupConfiguration.Frequency > 0 && selected_rootFolder.CleanupConfiguration.LeadTime > 0 )
            {
                selected_rootFolder.CleanupConfiguration.StartDate = DateTime.UtcNow;
            } else{
                selected_rootFolder.CleanupConfiguration.StartDate = null;
            }

            UpdateCleanupConfigurationCmd cmd = new UpdateCleanupConfigurationCmd(Api, selected_rootFolder, selected_rootFolder.CleanupConfiguration);
            bool success = await cmd.Apply();
            
            if (!success)
            {
                Console.WriteLine("Failed to update cleanup configuration.");
            }
            StateHasChanged();
        }
        private void Go2retention(RootFolder root_folder)
        {
            selected_rootFolder = root_folder;
            NavService.CurrentRootFolder = selected_rootFolder;
            Navigation.NavigateTo("retention");
        }
    }

    public class NamedPeriod
    {
        public NamedPeriod( string name, int days )
        {
            Name = name;
            Days = days;
        }
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
}