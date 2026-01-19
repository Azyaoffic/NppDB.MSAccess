using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Threading;
using Antlr4.Runtime;
using NppDB.Comm;

namespace NppDB.MSAccess
{
    public class MsAccessLexerErrorListener : ConsoleErrorListener<int>
    {
        public new static readonly MsAccessLexerErrorListener Instance = new MsAccessLexerErrorListener();

        public override void SyntaxError(TextWriter output, IRecognizer recognizer, 
            int offendingSymbol, int line, int col, string msg, RecognitionException e)
        {
            Console.WriteLine($@"LEXER ERROR: {e?.GetType().ToString() ?? ""}: {msg} ({line}:{col})");
        }
    }

    public class MsAccessParserErrorListener : BaseErrorListener
    {
        public IList<ParserError> Errors { get; } = new List<ParserError>();

        public override void SyntaxError(TextWriter output, IRecognizer recognizer, 
            IToken offendingSymbol, int line, int col, string msg, RecognitionException e)
        {
            Console.WriteLine($@"PARSER ERROR: {e?.GetType().ToString() ?? ""}: {msg} ({line}:{col})");
            Errors.Add(new ParserError
            {
                Text = msg,
                StartLine = line,
                StartColumn = col,
                StartOffset = offendingSymbol.StartIndex,
                StopOffset = offendingSymbol.StopIndex,
            }); 
        }
    }

    public sealed class MsAccessExecutor : ISqlExecutor
    {
        private Thread _execTh;
        private readonly Func<OleDbConnection> _connector;
        private readonly string _behaviorSettingsPath;

        public MsAccessExecutor(Func<OleDbConnection> connector, string behaviorSettingsPath)
        {
            _connector = connector;
            _behaviorSettingsPath = behaviorSettingsPath;
        }

        public ParserResult Parse(string sqlText, CaretPosition caretPosition)
        {
            var input = CharStreams.fromString(sqlText);

            var lexer = new MSAccessLexer(input);
            lexer.RemoveErrorListeners();
            lexer.AddErrorListener(MsAccessLexerErrorListener.Instance);

            CommonTokenStream tokens;
            try
            {
                tokens = new CommonTokenStream(lexer);
            }
            catch (Exception e)
            {
                Console.WriteLine($@"Lexer Exception: {e}");
                throw;
            }

            var parserErrorListener = new MsAccessParserErrorListener();
            var parser = new MSAccessParser(tokens);
            parser.RemoveErrorListeners();
            parser.AddErrorListener(parserErrorListener);
            try
            {
                var tree = parser.parse();
                var enclosingCommandIndex = tree.CollectCommands(caretPosition, " ", MSAccessParser.SCOL, out var commands);
                return new ParserResult
                {
                    Errors = parserErrorListener.Errors, 
                    Commands = commands.ToList<ParsedCommand>(), 
                    EnclosingCommandIndex = enclosingCommandIndex
                };
            }
            catch (Exception e)
            {
                Console.WriteLine($@"Parser Exception: {e}");
                throw;
            }
        }

        public SqlDialect Dialect => SqlDialect.MS_ACCESS;

        public void Execute(IList<string> sqlQueries, Action<IList<CommandResult>> callback)
        {
            _execTh = new Thread(new ThreadStart(
                delegate
                {
                    var results = new List<CommandResult>();
                    string lastSql = null;
                    try
                    {
                        var destructiveEnabled = MsAccessBehaviorSettings.IsDestructiveSelectIntoEnabled(_behaviorSettingsPath);

                        using (var conn = _connector())
                        {
                            conn.Open();
                            
                            foreach (var sql in sqlQueries)
                            {
                                if (string.IsNullOrWhiteSpace(sql)) continue;
                                lastSql = sql;

                            if (destructiveEnabled && TryGetTopLevelSelectIntoTarget(sql, out var targetTable))
                            {
                                if (TableExists(conn, targetTable))
                                {
                                    var dropSql = $"DROP TABLE {QuoteAccess(targetTable)}";
                                    lastSql = dropSql;

                                    using (var dropCmd = new OleDbCommand(dropSql, conn))
                                    {
                                        dropCmd.ExecuteNonQuery();
                                    }

                                    lastSql = sql;
                                }
                            }

                            Console.WriteLine($@"SQL: <{sql}>");
                            var cmd = new OleDbCommand(sql, conn);
                            var rd = cmd.ExecuteReader();
                            {
                                var dt = new DataTable();
                                dt.Load(rd);
                                results.Add(new CommandResult
                                    { CommandText = sql, QueryResult = dt, RecordsAffected = rd.RecordsAffected });
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        results.Add(new CommandResult {CommandText = lastSql, Error = ex});
                        callback(results);
                        return;
                    }
                    callback(results);
                    _execTh = null;
                }));
            _execTh.IsBackground = true;
            _execTh.Start();
        }

        public bool CanExecute()
        {
            return !CanStop();
        }

        public void Stop()
        {
            if (!CanStop()) return;
            _execTh?.Abort();
            _execTh = null;
        }

        public bool CanStop()
        {
            return _execTh != null && (_execTh.ThreadState & ThreadState.Running) != 0;
        }

        private static bool TryGetTopLevelSelectIntoTarget(string sql, out string targetTableName)
        {
            targetTableName = null;
            if (string.IsNullOrWhiteSpace(sql)) return false;

            var input = CharStreams.fromString(sql);

            var lexer = new MSAccessLexer(input);
            lexer.RemoveErrorListeners();
            lexer.AddErrorListener(MsAccessLexerErrorListener.Instance);

            var tokens = new CommonTokenStream(lexer);

            var parserErrorListener = new MsAccessParserErrorListener();
            var parser = new MSAccessParser(tokens);
            parser.RemoveErrorListeners();
            parser.AddErrorListener(parserErrorListener);

            var tree = parser.parse();

            if (parserErrorListener.Errors.Count > 0) return false;

            var stmtList = tree.sql_stmt_list();
            if (stmtList == null) return false;

            var stmt = stmtList.sql_stmt(0);
            if (stmt == null) return false;

            var into = stmt.select_into_stmt();
            if (into == null) return false;

            var raw = into.tableName?.GetText();
            if (string.IsNullOrWhiteSpace(raw)) return false;

            targetTableName = NormalizeIdentifier(raw);
            return !string.IsNullOrWhiteSpace(targetTableName);
        }

        private static string NormalizeIdentifier(string raw)
        {
            raw = raw.Trim();

            while (raw.Length >= 2 && raw[0] == '(' && raw[raw.Length - 1] == ')')
                raw = raw.Substring(1, raw.Length - 2).Trim();

            if (raw.Length >= 2 && raw[0] == '[' && raw[raw.Length - 1] == ']')
                return raw.Substring(1, raw.Length - 2).Replace("]]", "]");

            if (raw.Length >= 2 && raw[0] == '"' && raw[raw.Length - 1] == '"')
                return raw.Substring(1, raw.Length - 2).Replace("\"\"", "\"");

            if (raw.Length >= 2 && raw[0] == '\'' && raw[raw.Length - 1] == '\'')
                return raw.Substring(1, raw.Length - 2).Replace("''", "'");

            return raw;
        }

        private static string QuoteAccess(string name)
        {
            return $"[{(name ?? "").Replace("]", "]]")}]";
        }

        private static bool TableExists(OleDbConnection conn, string tableName)
        {
            using (var dt = conn.GetSchema(OleDbMetaDataCollectionNames.Tables,
                       new[] { null, null, tableName, "TABLE" }))
            {
                return dt.Rows.Count > 0;
            }
        }
    }
}
