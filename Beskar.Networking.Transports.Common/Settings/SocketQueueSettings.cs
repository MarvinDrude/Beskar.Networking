using System.IO.Pipelines;

namespace Beskar.Networking.Transports.Common.Settings;

/// <summary>
/// Represents the settings for a socket connection queue.
/// </summary>
public sealed class SocketQueueSettings
   : BaseQueueSettings
{
   /// <summary>
   /// The scheduler used for the pipe processing.
   /// </summary>
   public required PipeScheduler PipeScheduler { get; set; }
}
