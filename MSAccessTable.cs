using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Newtonsoft.Json;
using NppDB.Comm;

namespace NppDB.MSAccess
{
    public struct PromptItemNoPlaceholder
    {
        public string Id;
        public string Title;
        public string Description;
        public string Type; // "TablePrompt", "LlmPrompt"
        public string Text;
    }

    public class MsAccessTable : TreeNode, IRefreshable, IMenuProvider
    {
        protected string TypeName { get; set; } = "TABLE";

        public MsAccessTable()
        {
            SelectedImageKey = ImageKey = @"Table";
        }
        
        private List<PromptItemNoPlaceholder> _tableAiPrompts = new List<PromptItemNoPlaceholder>();

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

                    var columns = new List<MsAccessColumn>();

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
                host.Execute(NppDbCommandType.SET_SQL_LANGUAGE, null);
                var id = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                var query = $"SELECT * FROM {objectNameQuoted}";
                host.Execute(NppDbCommandType.APPEND_TO_CURRENT_VIEW, new object[] { query });
                host.Execute(NppDbCommandType.CREATE_RESULT_VIEW, new[] { id, connect, connect.CreateSqlExecutor() });
                host.Execute(NppDbCommandType.EXECUTE_SQL, new[] { id, query });
            }));
            menuList.Items.Add(new ToolStripButton("Select top 100 rows", null, (s, e) =>
            {
                host.Execute(NppDbCommandType.NEW_FILE, null);
                host.Execute(NppDbCommandType.SET_SQL_LANGUAGE, null);
                var id = host.Execute(NppDbCommandType.GET_ACTIVATED_BUFFER_ID, null);
                var query = $"SELECT TOP 100 * FROM {objectNameQuoted}";
                host.Execute(NppDbCommandType.APPEND_TO_CURRENT_VIEW, new object[] { query });
                host.Execute(NppDbCommandType.CREATE_RESULT_VIEW, new[] { id, connect, connect.CreateSqlExecutor() });
                host.Execute(NppDbCommandType.EXECUTE_SQL, new[] { id, query });
            }));
            var exportMenu = new ToolStripMenuItem("Select all as");
            exportMenu.DropDownItems.Add(new ToolStripMenuItem("JSON", null, (s, e) => { SelectAllAsJson(); }));
            exportMenu.DropDownItems.Add(new ToolStripMenuItem("CSV", null, (s, e) => { SelectAllAsCsv(); }));
            menuList.Items.Add(exportMenu);

            menuList.Items.Add(new ToolStripSeparator());
            
            if (TypeName == "TABLE")
            {
                // this one only includes PK as anything else is much harder to extract
                menuList.Items.Add(new ToolStripButton("Generate CREATE TABLE query", null, (s, e) =>
                {
                    var ddl = GenerateCreateTableQuery(connect);
                    if (string.IsNullOrWhiteSpace(ddl)) return;

                    try
                    {
                        Clipboard.SetText(ddl);
                    }
                    catch (Exception)
                    {
                        // ignore
                    }

                    host.Execute(NppDbCommandType.NEW_FILE, null);
                    host.Execute(NppDbCommandType.SET_SQL_LANGUAGE, null);
                    host.Execute(NppDbCommandType.APPEND_TO_CURRENT_VIEW, new object[] { ddl });

                    MessageBox.Show(
                        "CREATE TABLE query copied to clipboard and opened in a new tab.",
                        "NppDB",
                        MessageBoxButtons.OK,
                        MessageBoxIcon.Information
                    );
                }));
                
                menuList.Items.Add(new ToolStripSeparator());
            }

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
            
            // options to generate AI prompt
            _tableAiPrompts = LoadTablePromptsFromFile(host);

            if (_tableAiPrompts.Count > 0)
            {
                menuList.Items.Add(new ToolStripSeparator());

                var aiMenu = new ToolStripMenuItem("AI Prompts");
                
                foreach (var prompt in _tableAiPrompts)
                {
                    aiMenu.DropDownItems.Add(new ToolStripMenuItem(prompt.Title, null,
                        (s, e) => ShowTablePrompt(prompt)));
                }

                menuList.Items.Add(aiMenu);
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

        private void SelectAllAsJson()
        {
            var connect = GetDbConnect();
            if (connect?.CommandHost == null) return;

            var tableQuoted = QuoteIdentifier(Text);

            SelectAllAsText("JSON", () =>
            {
                using (var cnn = connect.GetConnection())
                {
                    cnn.Open();

                    var dt = new DataTable();
                    using (var cmd = cnn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT * FROM " + tableQuoted;
                        using (var da = new OleDbDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }
                    }

                    var json = JsonConvert.SerializeObject(dt, Formatting.Indented);
                    if (string.IsNullOrWhiteSpace(json)) json = "[]";
                    return json;
                }
            });
        }

        private void SelectAllAsCsv()
        {
            var connect = GetDbConnect();
            if (connect?.CommandHost == null) return;

            var tableQuoted = QuoteIdentifier(Text);

            SelectAllAsText("CSV", () =>
            {
                using (var cnn = connect.GetConnection())
                {
                    cnn.Open();

                    var dt = new DataTable();
                    using (var cmd = cnn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT * FROM " + tableQuoted;
                        using (var da = new OleDbDataAdapter(cmd))
                        {
                            da.Fill(dt);
                        }
                    }

                    return ConvertDataTableToCsv(dt);
                }
            });
        }

        private void SelectAllAsText(string kind, Func<string> loader)
        {
            var connect = GetDbConnect();
            if (connect?.CommandHost == null) return;
            var host = connect.CommandHost;

            try
            {
                if (TreeView != null)
                {
                    TreeView.Enabled = false;
                    TreeView.Cursor = Cursors.WaitCursor;
                }

                var text = loader?.Invoke() ?? "";

                try
                {
                    Clipboard.SetText(text);
                }
                catch (Exception exClipboard)
                {
                    MessageBox.Show(
                        "Export succeeded but copying to clipboard failed: " + exClipboard.Message,
                        "NppDB",
                        MessageBoxButtons.OK,
                        MessageBoxIcon.Warning
                    );
                }

                host.Execute(NppDbCommandType.NEW_FILE, null);
                host.Execute(NppDbCommandType.APPEND_TO_CURRENT_VIEW, new object[] { text });

                MessageBox.Show(
                    kind + " exported to a new tab. Output was also copied to clipboard.",
                    "NppDB",
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Information
                );
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message, @"Exception");
            }
            finally
            {
                if (TreeView != null)
                {
                    TreeView.Enabled = true;
                    TreeView.Cursor = null;
                }
            }
        }

        private static string ConvertDataTableToCsv(DataTable dt)
        {
            if (dt == null) return string.Empty;

            var csv = "";

            for (var i = 0; i < dt.Columns.Count; i++)
            {
                if (i > 0) csv += ",";
                csv += ToCsvValue(dt.Columns[i].ColumnName);
            }
            csv += "\n";

            foreach (DataRow row in dt.Rows)
            {
                for (var i = 0; i < dt.Columns.Count; i++)
                {
                    if (i > 0) csv += ",";

                    var val = row[i];
                    if (val == null || val == DBNull.Value) csv = csv + "";
                    else
                    {
                        var s = Convert.ToString(val, CultureInfo.InvariantCulture);
                        csv += ToCsvValue(s);
                    }
                }
                csv += "\n";
            }

            return csv;
        }

        private static string ToCsvValue(string value)
        {
            if (value == null) return string.Empty;

            var needsQuotes = value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) >= 0;
            if (value.Contains("\"")) value = value.Replace("\"", "\"\"");
            if (needsQuotes) value = "\"" + value + "\"";

            return value;
        }
        
        private string GenerateCreateTableQuery(MsAccessConnect connect)
        {
            using (var cnn = connect.GetConnection())
            {
                try
                {
                    cnn.Open();

                    var tableName = Text;
                    var tableQuoted = QuoteIdentifier(tableName);

                    var dtCols = cnn.GetSchema(OleDbMetaDataCollectionNames.Columns, new[] { null, null, tableName, null });
                    if (dtCols == null || dtCols.Rows.Count == 0) return null;

                    var hasAuto = dtCols.Columns.Contains("IS_AUTOINCREMENT");
                    var hasNullable = dtCols.Columns.Contains("IS_NULLABLE");

                    var colDefs = "";
                    foreach (var r in dtCols.AsEnumerable().OrderBy(x => Convert.ToInt32(x["ORDINAL_POSITION"])))
                    {
                        var colName = r["COLUMN_NAME"]?.ToString();
                        if (string.IsNullOrWhiteSpace(colName)) continue;

                        var isAuto = false;
                        if (hasAuto)
                        {
                            try { isAuto = Convert.ToBoolean(r["IS_AUTOINCREMENT"]); } catch { }
                        }

                        var isNullable = true;
                        if (hasNullable)
                        {
                            try { isNullable = Convert.ToBoolean(r["IS_NULLABLE"]); } catch { }
                        }

                        var oleDbType = (OleDbType)Convert.ToInt32(r["DATA_TYPE"]);
                        var maxLen = r.Table.Columns.Contains("CHARACTER_MAXIMUM_LENGTH") ? r["CHARACTER_MAXIMUM_LENGTH"] : null;
                        var prec = r.Table.Columns.Contains("NUMERIC_PRECISION") ? r["NUMERIC_PRECISION"] : null;
                        var scale = r.Table.Columns.Contains("NUMERIC_SCALE") ? r["NUMERIC_SCALE"] : null;

                        var typeSql = isAuto ? "AUTOINCREMENT" : ToAccessSqlType(oleDbType, maxLen, prec, scale);

                        var line = "    " + QuoteIdentifier(colName) + " " + typeSql;
                        if (!isNullable && !isAuto) line += " NOT NULL";

                        if (colDefs != "") colDefs += ",\n";
                        colDefs += line;
                    }

                    var pkLine = BuildPrimaryKeyLine(cnn, tableName);

                    var ddl =
                        "CREATE TABLE " + tableQuoted + " (\n" +
                        colDefs +
                        (string.IsNullOrWhiteSpace(pkLine) ? "" : ",\n" + pkLine) +
                        "\n);\n";

                    return ddl;
                }
                catch (Exception ex)
                {
                    MessageBox.Show(ex.Message, @"Exception");
                    return null;
                }
                finally
                {
                    cnn.Close();
                }
            }
        }

        private static string BuildPrimaryKeyLine(OleDbConnection cnn, string tableName)
        {
            try
            {
                var pk = cnn.GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys, new object[] { null, null, tableName });
                if (pk == null || pk.Rows.Count == 0) return null;

                var pkName = pk.Rows[0]["PK_NAME"]?.ToString();
                if (string.IsNullOrWhiteSpace(pkName)) pkName = "PK_" + tableName;

                var hasSeq = pk.Columns.Contains("KEY_SEQ");
                var rows = pk.AsEnumerable();
                if (hasSeq) rows = rows.OrderBy(r => Convert.ToInt32(r["KEY_SEQ"]));

                var cols = rows
                    .Select(r => QuoteIdentifier(r["COLUMN_NAME"]?.ToString()))
                    .Where(s => !string.IsNullOrWhiteSpace(s))
                    .ToArray();

                if (cols.Length == 0) return null;

                return "    CONSTRAINT " + QuoteIdentifier(pkName) + " PRIMARY KEY (" + string.Join(", ", cols) + ")";
            }
            catch
            {
                return null;
            }
        }
        
        private static string ToAccessSqlType(OleDbType oleDbType, object maxLengthObj, object precisionObj, object scaleObj)
        {
            int? maxLength = null;
            int? precision = null;
            int? scale = null;

            if (!(maxLengthObj is DBNull) && maxLengthObj != null) maxLength = Convert.ToInt32(maxLengthObj);
            if (!(precisionObj is DBNull) && precisionObj != null) precision = Convert.ToInt32(precisionObj);
            if (!(scaleObj is DBNull) && scaleObj != null) scale = Convert.ToInt32(scaleObj);

            switch (oleDbType)
            {
                case OleDbType.VarWChar:
                case OleDbType.WChar:
                case OleDbType.VarChar:
                case OleDbType.Char:
                    if (maxLength > 0 && maxLength.Value <= 255)
                        return "TEXT(" + maxLength.Value + ")";
                    return "LONGTEXT";

                case OleDbType.LongVarWChar:
                case OleDbType.LongVarChar:
                    return "LONGTEXT";

                case OleDbType.Boolean:
                    return "YESNO";

                case OleDbType.TinyInt:
                case OleDbType.UnsignedTinyInt:
                    return "BYTE";

                case OleDbType.SmallInt:
                case OleDbType.UnsignedSmallInt:
                    return "SHORT";

                case OleDbType.Integer:
                case OleDbType.UnsignedInt:
                    return "LONG";

                case OleDbType.BigInt:
                case OleDbType.UnsignedBigInt:
                    return "BIGINT";

                case OleDbType.Single:
                    return "SINGLE";

                case OleDbType.Double:
                    return "DOUBLE";

                case OleDbType.Currency:
                    return "CURRENCY";

                case OleDbType.Date:
                case OleDbType.DBDate:
                case OleDbType.DBTime:
                case OleDbType.DBTimeStamp:
                    return "DATETIME";

                case OleDbType.Guid:
                    return "GUID";

                case OleDbType.Decimal:
                case OleDbType.Numeric:
                    if (precision.HasValue && scale.HasValue) return "DECIMAL(" + precision.Value + "," + scale.Value + ")";
                    if (precision.HasValue) return "DECIMAL(" + precision.Value + ")";
                    return "DECIMAL";

                case OleDbType.Binary:
                case OleDbType.VarBinary:
                case OleDbType.LongVarBinary:
                    return "LONGBINARY";

                default:
                    return oleDbType.ToString().ToUpper();
            }
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

        private int CollectColumns(OleDbConnection connection, ref List<MsAccessColumn> columns,
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

                var columnInfoNode = new MsAccessColumn(columnName, typeDetails, 0, options);


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

        private List<string> CollectPrimaryKeys(OleDbConnection connection, ref List<MsAccessColumn> columns)
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

                var pkNode = new MsAccessColumn(primaryKeyName, primaryKeyType, 1, 0);

                var tooltipText = new StringBuilder();
                tooltipText.AppendLine($"Primary Key Constraint: {primaryKeyName}");
                tooltipText.AppendLine($"Column: {columnName}");
                pkNode.ToolTipText = tooltipText.ToString().TrimEnd();

                columns.Add(pkNode);
                names.Add(columnName);
            }

            return names;
        }

        private List<string> CollectForeignKeys(OleDbConnection connection, ref List<MsAccessColumn> columns)
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

                var fkNode = new MsAccessColumn(foreignKeyName, foreignKeyType, 2, 0);

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

        private List<string> CollectIndices(OleDbConnection connection, ref List<MsAccessColumn> columns)
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
                    var indexNode = new MsAccessColumn(indexName, indexType, isUnique ? 4 : 3, 0);

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

        private void InitializePathsForPromptReading(INppDbCommandHost commandHost)
        {
            var dir = commandHost?.Execute(NppDbCommandType.GET_PLUGIN_CONFIG_DIRECTORY, null) as string;
            if (string.IsNullOrWhiteSpace(dir)) return;
            
            MSAccessPromptReading.LibraryFilePath = Path.Combine(dir, "promptLibrary.xml");
            
            MSAccessPromptReading.PreferencesFilePath = Path.Combine(dir, "prompt_preferences.json");
        }

        private List<PromptItemNoPlaceholder> LoadTablePromptsFromFile(INppDbCommandHost commandHost)
        {
            InitializePathsForPromptReading(commandHost);
            
            var prompts = MSAccessPromptReading.ReadPromptLibraryFromFile();
            if (prompts == null || prompts.Count == 0)
            {
                return new List<PromptItemNoPlaceholder>();
            }
            
            return prompts.Where(p => p.Type.Equals("TablePrompt", StringComparison.OrdinalIgnoreCase)).ToList();
        }

        private void ShowTablePrompt(PromptItemNoPlaceholder promptItem)
        {
            var tableName = Text;
            var columnsWithTypes = GetColumnsWithTypesFromTree();
            if (columnsWithTypes == null) return; // error already shown

            var title = promptItem.Title;
            var prompt = promptItem.Text
                .Replace("{{table_name}}", tableName)
                .Replace("{{columns_with_types}}", columnsWithTypes)
                .Replace("{{dialect}}", "MS Access");
            
            var userPreferences = MSAccessPromptReading.LoadUserPromptPreferences();

            if (!string.IsNullOrWhiteSpace(userPreferences))
            {
                prompt = prompt + "\n\n" + userPreferences;
            }

            CopyPromptToClipboardAndShow(title, prompt);
        }

        private string GetColumnsWithTypesFromTree()
        {
            var sb = new StringBuilder();

            foreach (TreeNode node in Nodes)
            {
                if (node == null) continue;
                sb.AppendLine(node.Text);
            }

            var text = sb.ToString().TrimEnd('\r', '\n');
            if (string.IsNullOrWhiteSpace(text))
            {
                MessageBox.Show(
                    "No columns loaded in tree. Please expand the table node once to load columns, then retry.",
                    "NppDB",
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Warning
                );
                return null;

            }

            return text;
        }

        private void CopyPromptToClipboardAndShow(string title, string prompt)
        {
            try
            {
                Clipboard.SetText(prompt);

                var dialogMessage =
                    "AI prompt copied to clipboard!\n\n" +
                    "--- Prompt Content: ---\n" +
                    prompt;

                MessageBox.Show(dialogMessage, "NppDB - " + title, MessageBoxButtons.OK, MessageBoxIcon.Information);
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    "Error copying prompt to clipboard or displaying prompt: " + ex.Message,
                    "NppDB",
                    MessageBoxButtons.OK,
                    MessageBoxIcon.Error
                );
            }
        }

    }
}