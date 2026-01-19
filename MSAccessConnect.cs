using System;
using System.Data;
using System.Data.OleDb;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Windows.Forms;
using System.Xml.Serialization;
using NppDB.Comm;
using NppDB.MSAccess.Properties;

namespace NppDB.MSAccess
{
    [XmlRoot]
    [ConnectAttr(Id = "MSAccessConnect", Title = "MS Access")]
    public class MsAccessConnect : TreeNode, IDbConnect, IRefreshable, IMenuProvider, IIconProvider, INppDBCommandClient
    {
        [XmlElement]
        public string Title { set => Text = value; get => Text; }
        [XmlElement]
        public string ServerAddress { set; get; }

        public string Account { set; get; }

        [XmlIgnore]
        public string Password { set; get; }
        private OleDbConnection _conn;
        private string _engineVersion;

        public string GetDefaultTitle()
        {
            return string.IsNullOrEmpty(ServerAddress) ? "MS Access" : Path.GetFileName(ServerAddress);
        }

        public Bitmap GetIcon()
        {
            return Resources.Access;
        }

        public bool CheckLogin()
        {
            ToolTipText = ServerAddress;
            var isNewFileScenario = false;

            if (string.IsNullOrEmpty(ServerAddress) || !File.Exists(ServerAddress))
            {
                if (!string.IsNullOrEmpty(ServerAddress) &&
                    MessageBox.Show("file(" + ServerAddress + ") don't existed.\ncreate a new database?", @"Alert",
                        MessageBoxButtons.YesNo, MessageBoxIcon.Warning) == DialogResult.No)
                {
                     return false;
                }

                using(var dlg = new FrmMsAccessConnect())
                {
                    if (dlg.ShowDialog() == DialogResult.OK)
                    {
                        Stream resource = null;
                        FileDialog fdlg;
                        var creatingNewFile = dlg.IsNew;

                        if (creatingNewFile)
                        {
                             fdlg = new SaveFileDialog { Title = @"New Access File" };
                             resource = Assembly.GetExecutingAssembly().GetManifestResourceStream("NppDB.MSAccess.Resources.empty.accdb");
                             if (resource == null) {
                                 MessageBox.Show("Error: Embedded empty database resource not found.", @"Resource Error", MessageBoxButtons.OK, MessageBoxIcon.Error); return false; }
                             fdlg.Filter = @"Access Database (*.accdb)|*.accdb|Access 2000-2003 Database (*.mdb)|*.mdb";
                             fdlg.DefaultExt = "accdb";
                        }
                        else
                        {
                             fdlg = new OpenFileDialog { Title = @"Open Access File" };
                             fdlg.Filter = @"Access Databases (*.accdb;*.mdb)|*.accdb;*.mdb|All Files(*.*)|*.*";
                        }

                        if (!string.IsNullOrEmpty(ServerAddress))
                            fdlg.InitialDirectory = Path.GetDirectoryName(ServerAddress);
                        fdlg.AddExtension = true;

                        var fileDialogResult = fdlg.ShowDialog();

                        if (fileDialogResult == DialogResult.OK)
                        {
                            if (resource != null)
                            {
                                try
                                {
                                    using (resource)
                                    using (var destinationFile = File.Create(fdlg.FileName))
                                    {
                                         resource.Seek(0, SeekOrigin.Begin);
                                         resource.CopyTo(destinationFile);
                                    }
                                }
                                catch(Exception ex)
                                {
                                     MessageBox.Show($"Failed to create new database file: {ex.Message}", @"Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                                     return false;
                                }
                            }
                            ServerAddress = fdlg.FileName;
                            Text = GetDefaultTitle();
                            Password = "";
                            isNewFileScenario = creatingNewFile;
                        }
                        else
                        {
                             return false;
                        }
                    }
                    else
                    {
                         return false;
                    }
                }
            }

            if (!isNewFileScenario)
            {
                var testBuilder = new OleDbConnectionStringBuilder
                {
                    Provider = "Microsoft.ACE.OLEDB.12.0",
                    DataSource = ServerAddress,
                    OleDbServices = -4
                };
                using (var testConn = new OleDbConnection(testBuilder.ConnectionString))
                {
                    try
                    {
                        testConn.Open();
                        Password = "";
                        return true;
                    }
                    catch (OleDbException oleEx)
                    {
                        if (oleEx.ErrorCode == -2147217900 || (oleEx.Message != null && oleEx.Message.ToLowerInvariant().Contains("password")))
                        {
                            using (var pdlg = new FrmPassword { VisiblePassword = false })
                            {
                                if (pdlg.ShowDialog() != DialogResult.OK) return false;
                                Password = pdlg.Password;
                                return true;
                            }
                        }

                        MessageBox.Show($"Failed to test connection:\n{oleEx.Message}", @"Connection Error", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                        return false;
                    }
                    catch (Exception ex)
                    {
                         MessageBox.Show($"An unexpected error occurred while testing the connection:\n{ex.Message}", @"Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
                         return false;
                    }
                }
            }

            Password = "";
            return true;
        }


        public void Connect()
        {
            if (_conn != null)
            {
                 if (_conn.State != ConnectionState.Closed)
                 {
                     try { _conn.Close(); }
                     catch
                     {
                         // ignored
                     }
                 }
                 _conn.Dispose();
                 _conn = null;
                 _engineVersion = null;
            }

            _conn = new OleDbConnection();
            var connectionStringToUse = GetConnectionString();

            var maskedConnectionString = connectionStringToUse;
            if(connectionStringToUse.IndexOf("Password=", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                try {
                    maskedConnectionString = Regex.Replace(connectionStringToUse, @"(Jet\sOLEDB:Database\sPassword=)[^;]+", "$1*****", RegexOptions.IgnoreCase);
                } catch {
                    maskedConnectionString = connectionStringToUse.Replace(Password,"*****");
                }
            }

            _conn.ConnectionString = connectionStringToUse;

            if (_conn.State == ConnectionState.Open)
            {
                if (_engineVersion == null) FetchEngineVersionInternal();
                return;
            }

            try
            {
                _conn.Open();
                FetchEngineVersionInternal();
            }
            catch (OleDbException oleEx)
            {
                _engineVersion = null;
                var errorDetails = $"Connect FAILED (OleDbException):\n\nErrorCode: {oleEx.ErrorCode}\nHResult: {oleEx.HResult}\nMessage: {oleEx.Message}\n\nConnectionString Used (masked):\n{maskedConnectionString}";
                MessageBox.Show(errorDetails, @"Connect OLEDB Error", MessageBoxButtons.OK, MessageBoxIcon.Error);

                if (_conn == null) throw;
                _conn.Dispose(); _conn = null;
                throw;
            }
            catch (Exception ex)
            {
                _engineVersion = null;
                var errorDetails = $"Connect FAILED (Generic Exception):\n\nType: {ex.GetType().Name}\nMessage: {ex.Message}\n\nConnectionString Used (masked):\n{maskedConnectionString}";
                MessageBox.Show(errorDetails, @"Connect Generic Error", MessageBoxButtons.OK, MessageBoxIcon.Error);

                if (_conn == null) throw;
                _conn.Dispose(); _conn = null;
                throw;
            }
        }

        public void Attach()
        {
            var id = CommandHost.Execute(NppDbCommandType.GET_ATTACHED_BUFFER_ID, null);
            if (id != null)
            {
                CommandHost.Execute(NppDbCommandType.NEW_FILE, null);
            }
            id = CommandHost.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
            CommandHost.Execute(NppDbCommandType.CREATE_RESULT_VIEW, new[] { id, this, CreateSqlExecutor() });
        }

        public string ConnectAndAttach()
        {
            if (IsOpened) return "CONTINUE";
            if (!CheckLogin()) return "FAIL";
            try
            {
                Connect();
                Attach();
                Refresh();
                return "FRESH_NODES";
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message + (ex.InnerException != null ? " : " + ex.InnerException.Message : ""));
            }
            return "FAIL";
        }

        public void Disconnect()
        {
            _engineVersion = null;
            if (_conn == null || _conn.State == ConnectionState.Closed) return;
            _conn.Close();
        }

        public bool IsOpened => _conn != null && _conn.State == ConnectionState.Open;

        public string DatabaseSystemName
        {
            get
            {
                const string baseName = "MSAccess";

                if (string.IsNullOrEmpty(_engineVersion)) return baseName;
                string accessYear;

                var versionParts = _engineVersion.Split('.');
                if (versionParts.Length > 0 && int.TryParse(versionParts[0], out var majorVersion))
                {
                    switch (majorVersion)
                    {
                        case 4:
                            accessYear = "2000-2003 (Jet 4.0)";
                            break;
                        case 12:
                            accessYear = "2007";
                            break;
                        case 14:
                            accessYear = "2010";
                            break;
                        case 15:
                            accessYear = "2013";
                            break;
                        case 16:
                            accessYear = "2016+";
                            break;
                        default:
                            accessYear = $"(Engine: {_engineVersion})";
                            break;
                    }
                }
                else
                {
                    accessYear = $"(Engine: {_engineVersion})";
                }

                return $"{baseName} {accessYear}";

            }
        }

        public SqlDialect Dialect => SqlDialect.MS_ACCESS;
        
        [XmlIgnore]
        public INppDbCommandHost CommandHost { get; set; }

        internal OleDbConnection GetConnection()
        {
            return new OleDbConnection(GetConnectionString());
        }

        private void FetchEngineVersionInternal()
        {
            if (_conn == null || _conn.State != ConnectionState.Open)
            {
                _engineVersion = null;
                return;
            }
            try
            {
                _engineVersion = _conn.ServerVersion;
            }
            catch (Exception ex)
            {
                Console.WriteLine($@"Error fetching MS Access engine version: {ex.Message}");
                _engineVersion = null;
            }
        }

        public ISqlExecutor CreateSqlExecutor()
        {
            var settingsPath = MsAccessBehaviorSettings.TryGetSettingsPath(CommandHost);
            return new MsAccessExecutor(GetConnection, settingsPath);
            
        }

        public void Refresh()
        {
            using (var conn = GetConnection())
            {
                if (TreeView == null) return;

                TreeView.Cursor = Cursors.WaitCursor;
                TreeView.Enabled = false;
                Nodes.Clear();

                try
                {
                    conn.Open();

                    var dbNode = new MsAccessDatabase { Text = GetDefaultTitle() };

                    if (DatabaseHasChildren(conn))
                    {
                        dbNode.Nodes.Add(new TreeNode(""));
                    }

                    Nodes.Add(dbNode);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error Refreshing MsAccessConnect Node: {ex.Message}");
                    Nodes.Clear();
                    Nodes.Add(new TreeNode($"Error: {ex.Message.Split('\n')[0]}") { ImageKey="Error", SelectedImageKey="Error"});
                }
                finally
                {
                    TreeView.Enabled = true;
                    TreeView.Cursor = null;
                }
            }
        }


        /// <summary>
        /// Checks if the current Access database contains any user Tables or Views.
        /// </summary>
        private bool DatabaseHasChildren(OleDbConnection conn)
        {
            DataTable dt = null;
            try
            {
                dt = conn.GetSchema(OleDbMetaDataCollectionNames.Tables);
                if ((from DataRow row in dt.Rows let tableName = row["TABLE_NAME"] as string let tableType = row.Table.Columns.Contains("TABLE_TYPE") ? row["TABLE_TYPE"] as string : "TABLE" where tableType != null && (tableType.ToUpperInvariant() == "TABLE" ||
                        tableType.ToUpperInvariant() == "BASE TABLE") select tableName).Any(tableName => tableName != null && (!tableName.ToUpperInvariant().StartsWith("MSYS") &&
                        !tableName.ToUpperInvariant().StartsWith("USYS"))))
                {
                    dt.Dispose();
                    return true;
                }
                dt.Dispose();

                dt = conn.GetSchema(OleDbMetaDataCollectionNames.Views);
                if (dt != null && dt.Rows.Count > 0)
                {
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error checking for children in Access DB: {ex.Message}");
                return true;
            }
            finally
            {
                 dt?.Dispose();
            }

            return false;
        }


        public ContextMenuStrip GetMenu()
        {
            var menuList = new ContextMenuStrip { ShowImageMargin = false };
            var connect = this;
            var host = CommandHost;
            if (host != null)
            {
                menuList.Items.Add(new ToolStripButton("Open a new query window", null, (s, e) =>
                {
                    try
                    {
                        host.Execute(NppDbCommandType.NEW_FILE, null);
                        var idObj = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                        if (idObj == null) return;
                        var bufferId = (IntPtr)idObj;
                        host.Execute(NppDbCommandType.CREATE_RESULT_VIEW, new object[] { bufferId, connect, CreateSqlExecutor() });
                    }
                    catch (Exception ex) { Console.WriteLine($@"Error in 'Open new query': {ex.Message}"); }
                }));

                if (host.Execute(NppDbCommandType.GET_ATTACHED_BUFFER_ID, null) == null)
                {
                    menuList.Items.Add(new ToolStripButton("Attach to the open query window", null, (s, e) =>
                    {
                        try
                        {
                            var idObj = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                            if (idObj == null) { Console.WriteLine(@"Attach failed: Could not get Activated Buffer ID."); return; }
                            var bufferId = (IntPtr)idObj;

                            host.Execute(NppDbCommandType.CREATE_RESULT_VIEW, new object[] { bufferId, connect, CreateSqlExecutor() });
                        }
                        catch (Exception attachEx) { Console.WriteLine($@"Error during Attach: {attachEx.Message}"); }
                    }));
                }
                else
                {
                     menuList.Items.Add(new ToolStripButton("Detach from the query window", null, (s, e) => { try { host.Execute(NppDbCommandType.DESTROY_RESULT_VIEW, null); } catch (Exception ex) { Console.WriteLine($@"Error during Detach: {ex.Message}"); } }));
                }
                menuList.Items.Add(new ToolStripSeparator());
            }

            menuList.Items.Add(new ToolStripButton("Refresh the database connection", null, (s, e) => { try { Refresh(); } catch (Exception ex) { Console.WriteLine($@"Error during Refresh: {ex.Message}"); } }));
            return menuList;
        }

        internal string GetConnectionString()
        {
            var builder = new OleDbConnectionStringBuilder
            {
                Provider = "Microsoft.ACE.OLEDB.12.0",
                DataSource = ServerAddress,
                OleDbServices = -4
            };

            if (!string.IsNullOrEmpty(Password))
            {
                builder.Add("Jet OLEDB:Database Password", Password);
            }

            return builder.ConnectionString;
        }

        public void Reset()
        {
            Title = ""; ServerAddress = ""; Account = ""; Password = "";
            _engineVersion = null;
            Disconnect();
            _conn = null;
        }

        public void SetCommandHost(INppDbCommandHost host)
        {
            CommandHost = host;
        }
    }
}