using System.Diagnostics;
using System.Net;
using System.Runtime.InteropServices;
using Beskar.Datagram.Example.ArenaGame2D.Protocol;
using Beskar.Datagram.Example.ArenaGame2D.Testing;
using Beskar.Datagram.Example.ArenaGame2D.UI;

namespace Beskar.Datagram.Example.ArenaGame2D;

internal static class Program
{
   [STAThread]
   public static int Main(string[] args)
   {
      ApplicationConfiguration.Initialize();

      if (args.Length > 0)
      {
         if (args[0].Equals("--self-test", StringComparison.OrdinalIgnoreCase))
         {
            AttachParentConsole();
            var outDir = args.Length > 1 ? args[1] : Path.Combine(AppContext.BaseDirectory, "selftest-output");
            return ArenaSelfTestRunner.Run(outDir);
         }

         if (args[0].Equals("--host", StringComparison.OrdinalIgnoreCase))
         {
            var port = args.Length > 1 && int.TryParse(args[1], out var p) ? p : GameProtocol2D.DefaultPort;
            var name = ReadFlag(args, "--name") ?? "Host";
            var bots = int.TryParse(ReadFlag(args, "--bots"), out var b) ? b : 2;
            Application.Run(new GameCanvasForm(isHost: true, new IPEndPoint(IPAddress.Any, port), name, bots));
            return 0;
         }

         if (args[0].Equals("--join", StringComparison.OrdinalIgnoreCase))
         {
            var target = args.Length > 1 && !args[1].StartsWith("--") ? args[1] : $"127.0.0.1:{GameProtocol2D.DefaultPort}";
            var name = ReadFlag(args, "--name") ?? $"Pilot{Random.Shared.Next(10, 99)}";
            Application.Run(new GameCanvasForm(isHost: false, ParseEndPoint(target), name));
            return 0;
         }
      }

      using var launcher = new LauncherDialog();
      if (launcher.ShowDialog() != DialogResult.OK)
      {
         return 0;
      }

      if (launcher.LaunchPair)
      {
         var exe = Environment.ProcessPath;
         if (!string.IsNullOrEmpty(exe))
         {
            Process.Start(new ProcessStartInfo
            {
               FileName = exe,
               Arguments = $"--join 127.0.0.1:{launcher.Port} --name Client1",
               UseShellExecute = false
            });
         }

         Application.Run(new GameCanvasForm(isHost: true, new IPEndPoint(IPAddress.Any, launcher.Port), launcher.PlayerName, initialBots: 1));
         return 0;
      }

      if (launcher.IsHost)
      {
         Application.Run(new GameCanvasForm(isHost: true, new IPEndPoint(IPAddress.Any, launcher.Port), launcher.PlayerName, initialBots: 2));
      }
      else
      {
         Application.Run(new GameCanvasForm(isHost: false, ParseEndPoint(launcher.JoinAddress), launcher.PlayerName));
      }

      return 0;
   }

   private static IPEndPoint ParseEndPoint(string raw)
   {
      var parts = raw.Split(':', 2);
      var ip = IPAddress.Parse(parts[0].Trim());
      var port = parts.Length == 2 && int.TryParse(parts[1], out var p) ? p : GameProtocol2D.DefaultPort;
      return new IPEndPoint(ip, port);
   }

   private static string? ReadFlag(string[] args, string flag)
   {
      for (var i = 0; i < args.Length - 1; i++)
      {
         if (args[i].Equals(flag, StringComparison.OrdinalIgnoreCase)) return args[i + 1];
      }

      return null;
   }

   private static void AttachParentConsole()
   {
      if (OperatingSystem.IsWindows())
      {
         AttachConsole(-1);
      }
   }

   [DllImport("kernel32.dll")]
   private static extern bool AttachConsole(int dwProcessId);
}
