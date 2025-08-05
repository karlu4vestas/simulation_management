namespace VSM.Client.Datamodel
{

    public class RootFolder
    {
        public int Id { get; set; }
        public bool Is_registeredfor_cleanup { get; set; } = false;
        public string Root_path { get; set; } = ""; // the folder to scan if we do not know it already
        public Folder? FolderTree { get; set; } // Seet this when you have request the folder tree for this RootFolder 
        public List<User> Users { get; set; } = [];
        public List<string> RetentionHeaders => FolderTree == null ? [] : FolderTree.AttributDict.Keys.ToList();
    }

}