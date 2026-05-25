using System.Diagnostics;
using Me.Memory.Buffers;

namespace Beskar.Utilities.Tracing;

/// <summary>
/// For dev purposes only in DEBUG builds
/// </summary>
public static class TraceLogger
{
   private const string Conditional = "DEBUG";
   public static bool IsEnabled { get; set; } = false;

   [Conditional(Conditional)]
   public static void LogNeutralWarning(string template, params object?[] args)
      => Log(TraceLogLevel.Warning, string.Format(template, args), TraceLogOrigin.None);

   [Conditional(Conditional)]
   public static void LogNeutralError(string template, params object?[] args)
      => Log(TraceLogLevel.Error, string.Format(template, args), TraceLogOrigin.None);

   [Conditional(Conditional)]
   public static void LogNeutralInfo(string template, params object?[] args)
      => Log(TraceLogLevel.Info, string.Format(template, args), TraceLogOrigin.None);

   [Conditional(Conditional)]
   public static void LogClientWarning(string template, params object?[] args)
      => Log(TraceLogLevel.Warning, string.Format(template, args), TraceLogOrigin.Client);

   [Conditional(Conditional)]
   public static void LogClientError(string template, params object?[] args)
      => Log(TraceLogLevel.Error, string.Format(template, args), TraceLogOrigin.Client);

   [Conditional(Conditional)]
   public static void LogClientInfo(string template, params object?[] args)
      => Log(TraceLogLevel.Info, string.Format(template, args), TraceLogOrigin.Client);

   [Conditional(Conditional)]
   public static void LogServerWarning(string template, params object?[] args)
      => Log(TraceLogLevel.Warning, string.Format(template, args), TraceLogOrigin.Server);

   [Conditional(Conditional)]
   public static void LogServerError(string template, params object?[] args)
      => Log(TraceLogLevel.Error, string.Format(template, args), TraceLogOrigin.Server);

   [Conditional(Conditional)]
   public static void LogServerInfo(string template, params object?[] args)
      => Log(TraceLogLevel.Info, string.Format(template, args), TraceLogOrigin.Server);

   [Conditional(Conditional)]
   public static void LogWarning(string template, TraceLogOrigin origin = TraceLogOrigin.Server, params object?[] args)
      => Log(TraceLogLevel.Warning, string.Format(template, args), origin);

   [Conditional(Conditional)]
   public static void LogError(string template, TraceLogOrigin origin = TraceLogOrigin.Server, params object?[] args)
      => Log(TraceLogLevel.Error, string.Format(template, args), origin);

   [Conditional(Conditional)]
   public static void LogInfo(string template, TraceLogOrigin origin = TraceLogOrigin.Server, params object?[] args)
      => Log(TraceLogLevel.Info, string.Format(template, args), origin);

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

      var messageWriter = new TextWriterIndentSlim(stackalloc char[512], stackalloc char[1]);
      try
      {
         WriteOrigin(ref messageWriter, origin);
         WriteLevel(ref messageWriter, level);

         messageWriter.WriteLine($" {message}");
         System.Console.WriteLine(messageWriter.WrittenSpan);
      }
      finally
      {
         messageWriter.Dispose();
      }
#endif
   }

   private static void WriteLevel(ref TextWriterIndentSlim writer, TraceLogLevel level)
   {
      switch (level)
      {
         case TraceLogLevel.Info:
            writer.Write("[info][INFO][/info]");
            break;
         case TraceLogLevel.Warning:
            writer.Write("[warning][⚠][/warning]");
            break;
         case TraceLogLevel.Error:
            writer.Write("[error][✘][/error]");
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
            writer.Write("[server][SERVER][/server]");
            break;
         case TraceLogOrigin.Client:
            writer.Write("[client][CLIENT][/client]");
            break;
         case TraceLogOrigin.None:
            break;
         default:
            throw new ArgumentOutOfRangeException(nameof(origin), origin, null);
      }
   }
}
