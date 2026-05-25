using System.Diagnostics;
using Me.Memory.Buffers;
using Beskar.Utilities.Console.Rendering;

namespace Beskar.Utilities.Tracing;

/// <summary>
/// For dev purposes only in DEBUG builds
/// </summary>
public static class TraceLogger
{
   private const string Conditional = "DEBUG";
   public static bool IsEnabled { get; set; } = false;
   
   private static readonly Lock ConsoleLock = new();

   private static object?[] ColorizeArgs(object?[] args)
   {
      if (args == null || args.Length == 0) return Array.Empty<object?>();
      var colorized = new object?[args.Length];
      for (var i = 0; i < args.Length; i++)
      {
         colorized[i] = args[i] is null ? "[yellow]null[/yellow]" : $"[yellow]{args[i]}[/yellow]";
      }
      return colorized;
   }

   [Conditional(Conditional)]
   public static void LogNeutralWarning(string template, params object?[] args)
   {
#if DEBUG
      if (!IsEnabled) return;
      Log(TraceLogLevel.Warning, string.Format(template, ColorizeArgs(args)), TraceLogOrigin.None);
#endif
   }

   [Conditional(Conditional)]
   public static void LogNeutralError(string template, params object?[] args)
   {
#if DEBUG
      if (!IsEnabled) return;
      Log(TraceLogLevel.Error, string.Format(template, ColorizeArgs(args)), TraceLogOrigin.None);
#endif
   }

   [Conditional(Conditional)]
   public static void LogNeutralInfo(string template, params object?[] args)
   {
#if DEBUG
      if (!IsEnabled) return;
      Log(TraceLogLevel.Info, string.Format(template, ColorizeArgs(args)), TraceLogOrigin.None);
#endif
   }

   [Conditional(Conditional)]
   public static void LogClientWarning(string template, params object?[] args)
   {
#if DEBUG
      if (!IsEnabled) return;
      Log(TraceLogLevel.Warning, string.Format(template, ColorizeArgs(args)), TraceLogOrigin.Client);
#endif
   }

   [Conditional(Conditional)]
   public static void LogClientError(string template, params object?[] args)
   {
#if DEBUG
      if (!IsEnabled) return;
      Log(TraceLogLevel.Error, string.Format(template, ColorizeArgs(args)), TraceLogOrigin.Client);
#endif
   }

   [Conditional(Conditional)]
   public static void LogClientInfo(string template, params object?[] args)
   {
#if DEBUG
      if (!IsEnabled) return;
      Log(TraceLogLevel.Info, string.Format(template, ColorizeArgs(args)), TraceLogOrigin.Client);
#endif
   }

   [Conditional(Conditional)]
   public static void LogServerWarning(string template, params object?[] args)
   {
#if DEBUG
      if (!IsEnabled) return;
      Log(TraceLogLevel.Warning, string.Format(template, ColorizeArgs(args)), TraceLogOrigin.Server);
#endif
   }

   [Conditional(Conditional)]
   public static void LogServerError(string template, params object?[] args)
   {
#if DEBUG
      if (!IsEnabled) return;
      Log(TraceLogLevel.Error, string.Format(template, ColorizeArgs(args)), TraceLogOrigin.Server);
#endif
   }

   [Conditional(Conditional)]
   public static void LogServerInfo(string template, params object?[] args)
   {
#if DEBUG
      if (!IsEnabled) return;
      Log(TraceLogLevel.Info, string.Format(template, ColorizeArgs(args)), TraceLogOrigin.Server);
#endif
   }

   [Conditional(Conditional)]
   public static void LogWarning(string template, TraceLogOrigin origin = TraceLogOrigin.Server, params object?[] args)
   {
#if DEBUG
      if (!IsEnabled) return;
      Log(TraceLogLevel.Warning, string.Format(template, ColorizeArgs(args)), origin);
#endif
   }

   [Conditional(Conditional)]
   public static void LogError(string template, TraceLogOrigin origin = TraceLogOrigin.Server, params object?[] args)
   {
#if DEBUG
      if (!IsEnabled) return;
      Log(TraceLogLevel.Error, string.Format(template, ColorizeArgs(args)), origin);
#endif
   }

   [Conditional(Conditional)]
   public static void LogInfo(string template, TraceLogOrigin origin = TraceLogOrigin.Server, params object?[] args)
   {
#if DEBUG
      if (!IsEnabled) return;
      Log(TraceLogLevel.Info, string.Format(template, ColorizeArgs(args)), origin);
#endif
   }

   [Conditional(Conditional)]
   public static void LogWarning(string message, TraceLogOrigin origin = TraceLogOrigin.Server)
      => Log(TraceLogLevel.Warning, message, origin);

   [Conditional(Conditional)]
   public static void LogError(string message, TraceLogOrigin origin = TraceLogOrigin.Server)
      => Log(TraceLogLevel.Error, message, origin);

   [Conditional(Conditional)]
   public static void LogInfo(string message, TraceLogOrigin origin = TraceLogOrigin.Server)
      => Log(TraceLogLevel.Info, message, origin);

   [Conditional(Conditional)]
   public static void Log(TraceLogLevel level, string message, TraceLogOrigin origin = TraceLogOrigin.Server)
   {
#if DEBUG
      if (!IsEnabled) return;

      lock (ConsoleLock)
      {
         var messageWriter = new TextWriterIndentSlim(stackalloc char[512], stackalloc char[1]);
         try
         {
            WriteOrigin(ref messageWriter, origin);
            WriteLevel(ref messageWriter, level);

            messageWriter.WriteLine($" {message}");
            ConsoleRender.WriteMarkupLine(messageWriter.WrittenSpan.ToString());
         }
         finally
         {
            messageWriter.Dispose();
         }
      }
#endif
   }

   private static void WriteLevel(ref TextWriterIndentSlim writer, TraceLogLevel level)
   {
      switch (level)
      {
         case TraceLogLevel.Info:
            writer.Write("[info]INFO[/info] |");
            break;
         case TraceLogLevel.Warning:
            writer.Write("[warning]WARN[/warning] |");
            break;
         case TraceLogLevel.Error:
            writer.Write("[error]FAIL[/error] |");
            break;
         default:
            throw new ArgumentOutOfRangeException(nameof(level), level, null);
      }
   }

   private static void WriteOrigin(ref TextWriterIndentSlim writer, TraceLogOrigin origin)
   {
      switch (origin)
      {
         case TraceLogOrigin.Server:
            writer.Write("[server]SERVER[/server] | ");
            break;
         case TraceLogOrigin.Client:
            writer.Write("[client]CLIENT[/client] | ");
            break;
         case TraceLogOrigin.None:
            break;
         default:
            throw new ArgumentOutOfRangeException(nameof(origin), origin, null);
      }
   }
}
