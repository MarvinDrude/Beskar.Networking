using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces.Pools;

namespace Beskar.Networking.Transports.Common.Sockets;

/// <summary>
/// Represents a duplex pipe connection over a socket.
/// </summary>
public sealed class SocketConnection 
   : IDuplexPipe, IAsyncDisposable, IPooledObject
{
   private SocketSender? _sender;
   private SocketReceiver? _receiver;
   private Socket? _socket;
   
   private readonly object _shutdownLock = new();
   private bool _isDisposed;
   private bool _isAborted;

   /// <summary>
   /// Gets the reader that reads incoming data from the socket.
   /// </summary>
   public PipeReader Input => _receiver?.Pipe.Reader 
      ?? throw new InvalidOperationException("Connection not initialized.");

   /// <summary>
   /// Gets the writer that writes outgoing data to the socket.
   /// </summary>
   public PipeWriter Output => _sender?.Pipe.Writer 
      ?? throw new InvalidOperationException("Connection not initialized.");

   /// <summary>
   /// Initializes the connection with the active socket, sender, and receiver.
   /// </summary>
   public void Initialize(Socket socket, SocketSender sender, SocketReceiver receiver)
   {
      _socket = socket;
      _sender = sender;
      _receiver = receiver;

      _sender.Initialize(this, socket);
      _receiver.Initialize(this, socket);
   }

   /// <summary>
   /// Starts the transmission loops for both the sender and receiver.
   /// </summary>
   public void Start()
   {
      if (_sender == null || _receiver == null)
      {
         throw new InvalidOperationException("SocketConnection must be initialized before starting.");
      }

      _sender.Start();
      _receiver.Start();
   }

   /// <summary>
   /// Aborts the connection immediately due to an error or an intentional connection reset.
   /// </summary>
   public void Abort(Exception? exception = null)
   {
      lock (_shutdownLock)
      {
         if (_isAborted || _isDisposed) return;
         _isAborted = true;
      }
 
      _sender?.Stop();
      _receiver?.Stop();

      if (_socket != null)
      {
         try
         {
            _socket.Close(timeout: 0);
         }
         catch
         {
            // Suppress
         }
      }
   }

   public bool TryResetState()
   {
      throw new NotImplementedException();
   }

   public async ValueTask DisposeAsync()
   {
      lock (_shutdownLock)
      {
         if (_isDisposed) return;
         _isDisposed = true;
      }

      _sender?.Stop();
      _receiver?.Stop();

      if (_receiver != null)
      {
         await _receiver.DisposeAsync();
      }

      if (!_isAborted && _socket != null)
      {
         try
         {
            _socket.Shutdown(SocketShutdown.Both);
         }
         catch
         {
            // Suppress if socket is already closed
         }
         finally
         {
            _socket.Dispose();
         }
      }
   }
}