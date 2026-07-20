using Beskar.Networking.Transports.Common.Options;

namespace Beskar.Networking.Transports.NamedPipes;

/// <summary>
/// Represents the options for a Named Pipe transport.
/// </summary>
public class NamedPipeTransportOptions
{
   /// <summary>
   /// The options for the underlying stream transport.
   /// </summary>
   public StreamTransportOptions StreamOptions { get; set; } = new();

   /// <summary>
   /// The number of IO queues for the transport.
   /// </summary>
   public int IoQueueCount => StreamOptions.IoQueueCount;

   /// <summary>
   /// The delay in milliseconds to wait before retrying to accept a new connection
   /// after an accept exception occurs. Defaults to 10ms.
   /// </summary>
   public int AcceptExceptionDelay { get; set; } = 10;

   /// <summary>
   /// The maximum number of concurrent client connections/handshakes allowed. Defaults to 512.
   /// </summary>
   public int MaxConcurrentHandshakes { get; set; } = 512;

   /// <summary>
   /// The maximum number of pending connections that can be queued in the listener's session channel. Defaults to 1024.
   /// </summary>
   public int MaxPendingConnections { get; set; } = 1024;

   /// <summary>
   /// The input buffer size to allocate for each named pipe instance. Defaults to 64 KB.
   /// </summary>
   public int InputBufferSize { get; set; } = 64 * 1024;

   /// <summary>
   /// The output buffer size to allocate for each named pipe instance. Defaults to 64 KB.
   /// </summary>
   public int OutputBufferSize { get; set; } = 64 * 1024;
}
