namespace VSM.Client.Datamodel
{
    public class User
    {
        public User(string initials)
        {
            Initials = initials;
        }

        public string Initials { get; set; }
    }
}