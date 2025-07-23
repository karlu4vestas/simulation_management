namespace VSM.Client.Datamodel
{

    public class RootFolder
    {
        public int Id { get; set; }
        public required string root_path { get; set; } // the folder to scan if we do not know it already
        public Folder? FolderTree { get; set; } // Seet this when you have request the folder tree for this RootFolder 
        public required List<User> Users { get; set; }

    }

}