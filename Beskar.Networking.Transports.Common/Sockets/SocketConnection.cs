using System.IO.Pipelines;

namespace Beskar.Networking.Transports.Common.Sockets;

/// <summary>
/// Represents a connection for a socket.
/// </summary>
public sealed class SocketConnection 
   : IDuplexPipe, IAsyncDisposable
{
   /// <summary>
   /// The input pipe reader.
   /// </summary>
   public PipeReader Input { get; }
   
   /// <summary>
   /// The output pipe writer.
   /// </summary>
   public PipeWriter Output { get; }
   
   /// <summary>
   /// The socket receiver.
   /// </summary>
   public SocketReceiver Receiver { get; private set; }
   
   /// <summary>
   /// The socket sender.
   /// </summary>
   public SocketSender Sender { get; private set; }
   
   /// <summary>
   /// Indicates whether the connection is closed.
   /// </summary>
   public bool IsClosed { get; private set; }

   /// <summary>
   /// The exception that caused the connection to be closed.
   /// </summary>
   public Exception? ShutdownException { get; set; }
   
   private readonly Lock _shutdownLock = new();
   private bool _isShutdown;
   
   
   
   public async ValueTask DisposeAsync()
   {
      // TODO release managed resources here
   }
}