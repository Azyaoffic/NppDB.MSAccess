using System;
using System.Collections.Generic;
using System.IO;
using System.Windows.Forms;
using System.Xml;
using Newtonsoft.Json;

namespace NppDB.MSAccess
{
    public class PromptPreferences
    {
        public string ResponseLanguage {get; set;}
        public string CustomInstructions {get; set;}
    }

    public class MSAccessPromptReading
    {
        public static string PreferencesFilePath;
        public static string LibraryFilePath;
        
        
        public static List<PromptItemNoPlaceholder> ReadPromptLibraryFromFile()
        {
            try
            {
                var xmlDoc = new XmlDocument();
                var results = new List<PromptItemNoPlaceholder>();

                try
                {
                    xmlDoc.Load(LibraryFilePath);
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Failed to load XML from file:\n{ex.Message}", "NppDb", MessageBoxButtons.OK,
                        MessageBoxIcon.Error);
                    return null;
                }

                var root = xmlDoc.DocumentElement;
                if (root == null || !string.Equals(root.Name, "Prompts", StringComparison.OrdinalIgnoreCase))
                {
                    MessageBox.Show(
                        $"Invalid prompt library format in `{LibraryFilePath}`. Root element `<Prompts>` not found.",
                        "NppDb", MessageBoxButtons.OK, MessageBoxIcon.Error);
                    return results;
                }
                
                var promptNodes = root.SelectNodes("Prompt");
                if (promptNodes == null)
                {
                    return results;
                }
                
                foreach (XmlNode promptNode in promptNodes)
                {
                    if (promptNode.NodeType != XmlNodeType.Element)
                        continue;

                    var typeAttr = (promptNode as XmlElement)?.GetAttribute("type") ?? string.Empty;

                    var id = promptNode.SelectSingleNode("Id")?.InnerText.Trim() ?? string.Empty;
                    var title = promptNode.SelectSingleNode("Title")?.InnerText.Trim() ?? string.Empty;
                    var description = promptNode.SelectSingleNode("Description")?.InnerText.Trim() ?? string.Empty;
                    var text = promptNode.SelectSingleNode("Text")?.InnerText ?? string.Empty;

                    var item = new PromptItemNoPlaceholder
                    {
                        Id = id,
                        Title = title,
                        Description = description,
                        Type = typeAttr,
                        Text = text,
                    };

                    results.Add(item);
                }
                return results;
            }

            catch (Exception ex)
            {
                MessageBox.Show($"Error reading prompt library from file:\n{ex.Message}", "NppDb", MessageBoxButtons.OK,
                    MessageBoxIcon.Error);
                return null;
            }
        }

        private static PromptPreferences ReadUserPreferences()
        {
            try
            {
                if (File.Exists(PreferencesFilePath))
                {
                    var readData = File.ReadAllText(PreferencesFilePath);
                    if (!string.IsNullOrEmpty(readData))
                    {
                        return JsonConvert.DeserializeObject<PromptPreferences>(readData);
                    }
                }

                return new PromptPreferences
                {
                    ResponseLanguage = "English",
                    CustomInstructions = ""
                };
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error reading preferences: {ex.Message}", "Error", MessageBoxButtons.OK,
                    MessageBoxIcon.Error);
                return new PromptPreferences
                {
                    ResponseLanguage = "English",
                    CustomInstructions = ""
                };
            }
        }
        
        public static string LoadUserPromptPreferences()
        {
            var preferences = ReadUserPreferences();
            return $"\nRespond in the following language: {preferences.ResponseLanguage}." +
                   $"\nAlso follow user's custom instructions: {preferences.CustomInstructions}.";
        }
    }
}