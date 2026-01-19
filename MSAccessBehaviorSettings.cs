using System.IO;
using Newtonsoft.Json;
using NppDB.Comm;

namespace NppDB.MSAccess
{
    internal static class MsAccessBehaviorSettings
    {
        internal static string TryGetSettingsPath(INppDbCommandHost commandHost)
        {
            var dir = commandHost?.Execute(NppDbCommandType.GET_PLUGIN_CONFIG_DIRECTORY, null) as string;
            if (string.IsNullOrWhiteSpace(dir)) return null;
            return Path.Combine(dir, "behavior_settings.json");
        }

        internal static bool IsDestructiveSelectIntoEnabled(string settingsPath)
        {
            if (string.IsNullOrWhiteSpace(settingsPath)) return false;
            if (!File.Exists(settingsPath)) return false;

            var json = File.ReadAllText(settingsPath);
            if (string.IsNullOrWhiteSpace(json)) return false;

            var value = JsonConvert.DeserializeObject<BehaviorSettings>(json);
            return value.EnableDestructiveSelectInto;
        }

        private struct BehaviorSettings
        {
            public bool EnableDestructiveSelectInto { get; set; }
        }
    }
}