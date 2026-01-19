using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using NppDB.Comm;

namespace NppDB.MSAccess
{
    public class MsAccessTable : TreeNode, IRefreshable, IMenuProvider
    {
        protected string TypeName { get; set; } = "TABLE";

        public MsAccessTable()
        {
            SelectedImageKey = ImageKey = @"Table";
        }

        public virtual void Refresh()
        {
            var conn = (MsAccessConnect)Parent.Parent.Parent;
            using (var cnn = conn.GetConnection())
            {
                TreeView.Enabled = false;
                TreeView.Cursor = Cursors.WaitCursor;
                try
                {
                    cnn.Open();

                    Nodes.Clear();

                    var columns = new List<MSAccessColumnInfo>();

                    var primaryKeyColumnNames = CollectPrimaryKeys(cnn, ref columns);
                    var foreignKeyColumnNames = CollectForeignKeys(cnn, ref columns);
                    var indexedColumnNames = CollectIndices(cnn, ref columns);

                    var columnCount = CollectColumns(cnn, ref columns, primaryKeyColumnNames, foreignKeyColumnNames,
                        indexedColumnNames);
                    if (columnCount == 0) return;

                    var maxLength = columns.Max(c => c.ColumnName.Length);
                    columns.ForEach(c => c.AdjustColumnNameFixedWidth(maxLength));
                    Nodes.AddRange(columns.ToArray<TreeNode>());
                }
                catch (Exception ex)
                {
                    MessageBox.Show(ex.Message, @"Exception");
                }
                finally
                {
                    TreeView.Enabled = true;
                    TreeView.Cursor = null;
                }
            }
        }

        public virtual ContextMenuStrip GetMenu()
        {
            var menuList = new ContextMenuStrip { ShowImageMargin = false };
            var connect = GetDbConnect();
            menuList.Items.Add(new ToolStripButton("Refresh", null, (s, e) => { Refresh(); }));
            menuList.Items.Add(new ToolStripSeparator());

            if (connect?.CommandHost == null) return menuList;

            var host = connect.CommandHost;
            var objectNameQuoted = $"[{Text}]";

            menuList.Items.Add(new ToolStripButton("Select all rows", null, (s, e) =>
            {
                host.Execute(NppDbCommandType.NEW_FILE, null);
                var id = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                var query = $"SELECT * FROM {objectNameQuoted}";
                host.Execute(NppDbCommandType.APPEND_TO_CURRENT_VIEW, new object[] { query });
                host.Execute(NppDbCommandType.CREATE_RESULT_VIEW, new[] { id, connect, connect.CreateSqlExecutor() });
                host.Execute(NppDbCommandType.EXECUTE_SQL, new[] { id, query });
            }));
            menuList.Items.Add(new ToolStripButton("Select top 100 rows", null, (s, e) =>
            {
                host.Execute(NppDbCommandType.NEW_FILE, null);
                var id = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                var query = $"SELECT TOP 100 * FROM {objectNameQuoted}";
                host.Execute(NppDbCommandType.APPEND_TO_CURRENT_VIEW, new object[] { query });
                host.Execute(NppDbCommandType.CREATE_RESULT_VIEW, new[] { id, connect, connect.CreateSqlExecutor() });
                host.Execute(NppDbCommandType.EXECUTE_SQL, new[] { id, query });
            }));
            menuList.Items.Add(new ToolStripSeparator());

            var dropObjectText = TypeName == "VIEW" ? "Drop view" : "Drop table";

            menuList.Items.Add(new ToolStripButton($"{dropObjectText} (RESTRICT)", null, (s, e) =>
            {
                var currentObjectName = Text;
                var message = $"Are you sure you want to {TypeName.ToLower()} '{currentObjectName}' (RESTRICT)?\n" +
                              $"This action cannot be undone and will fail if other objects depend on this {TypeName.ToLower()}.";
                if (MessageBox.Show(message, $@"Confirm Drop {TypeName}", MessageBoxButtons.YesNo,
                        MessageBoxIcon.Warning) != DialogResult.Yes) return;
                var id = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                var query = $"DROP {TypeName} {objectNameQuoted}";
                host.Execute(NppDbCommandType.EXECUTE_SQL, new[] { id, query });
                System.Threading.Thread.Sleep(500);
                if (Parent is IRefreshable parentGroupNode)
                {
                    parentGroupNode.Refresh();
                }
                else if (TreeView != null)
                {
                    Remove();
                }
            }));

            if (TypeName == "TABLE")
            {
                menuList.Items.Add(new ToolStripButton("Drop table (CASCADE)", null, (s, e) =>
                {
                    var currentTableName = Text;
                    var message = $"Are you sure you want to drop the table '{currentTableName}' (CASCADE)?\n" +
                                  "WARNING: MS Access 'DROP TABLE' behaves like RESTRICT by default. To achieve a true CASCADE effect (dropping dependent objects like relationships), those dependencies must often be removed manually *before* dropping the table.\n" +
                                  "This action cannot be undone.";
                    if (MessageBox.Show(message, @"Confirm Drop Table", MessageBoxButtons.YesNo,
                            MessageBoxIcon.Exclamation) != DialogResult.Yes) return;
                    var id = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                    var query = $"DROP {TypeName} {objectNameQuoted}";
                    host.Execute(NppDbCommandType.EXECUTE_SQL, new[] { id, query });
                    System.Threading.Thread.Sleep(500);
                    if (Parent is IRefreshable parentGroupNode)
                    {
                        parentGroupNode.Refresh();
                    }
                    else if (TreeView != null)
                    {
                        Remove();
                    }
                }));
            }

            return menuList;
        }

        private MsAccessConnect GetDbConnect()
        {
            var connect = Parent.Parent.Parent as MsAccessConnect;
            return connect;
        }


        private static string QuoteIdentifier(string name)
        {
            return $"[{(name ?? string.Empty).Replace("]", "]]")}]"; // escape closing bracket
        }

        private static HashSet<string> CollectAutoIncrementColumns(OleDbConnection connection, string tableOrViewName)
        {
            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            try
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = $"SELECT * FROM {QuoteIdentifier(tableOrViewName)} WHERE 1=0";
                    using (var rd = cmd.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo))
                    {
                        var schema = rd?.GetSchemaTable();
                        if (schema == null) return result;

                        var hasIsAutoIncrement = schema.Columns.Contains("IsAutoIncrement");
                        var hasIsIdentity = schema.Columns.Contains("IsIdentity");

                        foreach (DataRow r in schema.Rows)
                        {
                            var colName = r["ColumnName"]?.ToString();
                            if (string.IsNullOrWhiteSpace(colName)) continue;

                            var isAuto = false;
                            if (hasIsAutoIncrement && r["IsAutoIncrement"] is bool b && b) isAuto = true;
                            else if (hasIsIdentity && r["IsIdentity"] is bool i && i) isAuto = true;

                            if (isAuto) result.Add(colName);
                        }
                    }
                }
            }
            catch
            {
                // if error - just ignore
            }

            return result;
        }
        
        private static (int seed, int inc)? TryGetSeedInc(string oleDbConnStr, string table, string column)
        {
            try
            {
                var adoConnType = Type.GetTypeFromProgID("ADODB.Connection");
                dynamic adoConn = Activator.CreateInstance(adoConnType);
                adoConn.Open(oleDbConnStr);

                var catType = Type.GetTypeFromProgID("ADOX.Catalog");
                dynamic cat = Activator.CreateInstance(catType);
                cat.ActiveConnection = adoConn;

                dynamic col = cat.Tables[table].Columns[column];
                int seed = (int)col.Properties["Seed"].Value;
                int inc  = (int)col.Properties["Increment"].Value;

                adoConn.Close();
                return (seed, inc);
            }
            catch
            {
                return null;
            }
        }

        private int CollectColumns(OleDbConnection connection, ref List<MSAccessColumnInfo> columns,
            in List<string> primaryKeyColumnNames,
            in List<string> foreignKeyColumnNames,
            in List<string> indexedColumnNames)
        {
            var autoIncrementColumns = CollectAutoIncrementColumns(connection, Text);
            var dt = connection.GetSchema(OleDbMetaDataCollectionNames.Columns, new[] { null, null, Text, null });

            var count = 0;
            foreach (var row in dt.AsEnumerable().OrderBy(r => r["ordinal_position"]))
            {
                var columnName = row["column_name"].ToString();
                var isNullable = Convert.ToBoolean(row["is_nullable"]);
                var isAutoIncrement = autoIncrementColumns.Contains(columnName);

                var seed = 0;
                var inc = 0;
                if (isAutoIncrement)
                {
                    // Try to get seed and increment values
                    var seedInc = TryGetSeedInc(connection.ConnectionString, Text, columnName);
                    if (seedInc.HasValue)
                    {
                        seed = seedInc.Value.seed;
                        inc = seedInc.Value.inc;
                    }
                }

                var oleDbType = (OleDbType)int.Parse(row["data_type"].ToString());
                var typeName = oleDbType.ToString().ToUpper();
                var typeDetails = typeName;
                var maxLengthObj = row["character_maximum_length"];
                var numericPrecisionObj = row["numeric_precision"];
                var numericScaleObj = row["numeric_scale"];

                if (!(maxLengthObj is DBNull) && maxLengthObj != null)
                    typeDetails += $"({maxLengthObj})";
                else if (!(numericPrecisionObj is DBNull) && numericPrecisionObj != null)
                {
                    if (!(numericScaleObj is DBNull) && numericScaleObj != null && Convert.ToInt32(numericScaleObj) > 0)
                        typeDetails += $"({numericPrecisionObj},{numericScaleObj})";
                    else
                        typeDetails += $"({numericPrecisionObj})";
                }
                
                if (isAutoIncrement) typeDetails += " AUTOINCREMENT";
                if (seed != 0 || inc != 0) typeDetails += $"({seed},{inc})";

                var options = 0;
                if (!isNullable) options += 1;
                if (indexedColumnNames.Contains(columnName)) options += 10;
                if (primaryKeyColumnNames.Contains(columnName)) options += 100;
                if (foreignKeyColumnNames.Contains(columnName)) options += 1000;

                var columnInfoNode = new MSAccessColumnInfo(columnName, typeDetails, 0, options);


                var tooltipText = new StringBuilder();
                tooltipText.AppendLine($"Column: {columnName}");
                tooltipText.AppendLine($"Type: {typeDetails}");
                tooltipText.AppendLine($"Nullable: {(isNullable ? "Yes" : "No")}");
                if (isAutoIncrement)
                    tooltipText.Append("Auto-increment: Yes");
                if (seed != 0 || inc != 0)
                    tooltipText.AppendLine($" ({seed}, {inc})");
                else 
                    tooltipText.AppendLine();

                var defaultValueObj = row["column_default"];
                if (!(defaultValueObj is DBNull) && defaultValueObj != null)
                {
                    tooltipText.AppendLine($"Default: {defaultValueObj}");
                }

                if (primaryKeyColumnNames.Contains(columnName))
                    tooltipText.AppendLine("Primary Key Member");
                if (foreignKeyColumnNames.Contains(columnName))
                    tooltipText.AppendLine("Foreign Key Member");

                columnInfoNode.ToolTipText = tooltipText.ToString().TrimEnd();


                columns.Insert(count++, columnInfoNode);
            }

            return count;
        }

        private List<string> CollectPrimaryKeys(OleDbConnection connection, ref List<MSAccessColumnInfo> columns)
        {
            var dataTable =
                connection.GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys, new object[] { null, null, Text });

            var names = new List<string>();
            if (dataTable == null) return names;

            foreach (DataRow row in dataTable.Rows)
            {
                var primaryKeyName = row["pk_name"].ToString();
                var columnName = row["column_name"].ToString();
                var primaryKeyType = $"({columnName})";

                var pkNode = new MSAccessColumnInfo(primaryKeyName, primaryKeyType, 1, 0);

                var tooltipText = new StringBuilder();
                tooltipText.AppendLine($"Primary Key Constraint: {primaryKeyName}");
                tooltipText.AppendLine($"Column: {columnName}");
                pkNode.ToolTipText = tooltipText.ToString().TrimEnd();

                columns.Add(pkNode);
                names.Add(columnName);
            }

            return names;
        }

        private List<string> CollectForeignKeys(OleDbConnection connection, ref List<MSAccessColumnInfo> columns)
        {
            var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Foreign_Keys,
                new object[] { null, null, null, null, null, Text });

            var names = new List<string>();
            if (schema == null) return names;

            foreach (DataRow row in schema.Rows)
            {
                var foreignKeyName = row["fk_name"].ToString();
                var fkColumnName = row["fk_column_name"].ToString();
                var pkTableName = row["pk_table_name"].ToString();
                var pkColumnName = row["pk_column_name"].ToString();
                var foreignKeyType = $"({fkColumnName}) -> {pkTableName} ({pkColumnName})";

                var fkNode = new MSAccessColumnInfo(foreignKeyName, foreignKeyType, 2, 0);

                var tooltipText = new StringBuilder();
                tooltipText.AppendLine($"Foreign Key Constraint: {foreignKeyName}");
                tooltipText.AppendLine($"Local Column: {fkColumnName}");
                tooltipText.AppendLine($"References Table: {pkTableName}");
                tooltipText.AppendLine($"References Column: {pkColumnName}");
                fkNode.ToolTipText = tooltipText.ToString().TrimEnd();

                columns.Add(fkNode);
                names.Add(fkColumnName);
            }

            return names;
        }

        private List<string> CollectIndices(OleDbConnection connection, ref List<MSAccessColumnInfo> columns)
        {
            var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Indexes,
                new object[] { null, null, null, null, Text });

            var names = new List<string>();
            if (schema == null) return names;

            var processedIndexNames = new HashSet<string>();

            foreach (DataRow row in schema.Rows)
            {
                var indexName = row["index_name"].ToString();
                var columnName = row["column_name"].ToString();
                var indexType = $"({columnName})";
                var isUnique = Convert.ToBoolean(row["unique"]);

                if (!processedIndexNames.Contains(indexName))
                {
                    var indexNode = new MSAccessColumnInfo(indexName, indexType, isUnique ? 4 : 3, 0);

                    var tooltipText = new StringBuilder();
                    tooltipText.AppendLine($"Index: {indexName}");
                    tooltipText.AppendLine($"Column: {columnName}");
                    tooltipText.AppendLine($"Unique: {(isUnique ? "Yes" : "No")}");
                    indexNode.ToolTipText = tooltipText.ToString().TrimEnd();

                    columns.Add(indexNode);
                    processedIndexNames.Add(indexName);
                }

                names.Add(columnName);
            }

            return names;
        }
    }
}