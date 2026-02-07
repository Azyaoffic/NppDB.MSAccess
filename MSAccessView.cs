
namespace NppDB.MSAccess
{
    internal class MsAccessView : MsAccessTable
    {
        public MsAccessView()
        {
            TypeName = "VIEW";
            SelectedImageKey = ImageKey = "View";
        }
    }
}
