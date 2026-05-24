using System.Buffers;
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
   private readonly SocketSender _sender;
   private readonly SocketReceiver _receiver;
   private Socket? _socket;
   
   private readonly Lock _shutdownLock = new();
   private bool _isDisposed;
   private bool _isAborted;

   /// <summary>
   /// Gets the reader that reads incoming data from the socket.
   /// </summary>
   public PipeReader Input => _receiver.Pipe.Reader;

   /// <summary>
   /// Gets the writer that writes outgoing data to the socket.
   /// </summary>
   public PipeWriter Output => _sender.Pipe.Writer;

   /// <summary>
   /// Initializes a new instance of the <see cref="SocketConnection"/> class with pool invariants.
   /// </summary>
   public SocketConnection(PipeScheduler scheduler, MemoryPool<byte> bufferPool)
   {
      var pipeOptions = new PipeOptions(
         pool: bufferPool,
         readerScheduler: scheduler,
         writerScheduler: scheduler,
         useSynchronizationContext: false);

      _sender = new SocketSender(pipeOptions);
      _receiver = new SocketReceiver(pipeOptions);
   }

   /// <summary>
   /// Initializes the connection with the active socket for a rented session.
   /// </summary>
   public void Initialize(Socket socket)
   {
      _socket = socket;

      _sender.Initialize(this, socket);
      _receiver.Initialize(this, socket);
   }
   
   

   /// <summary>
   /// Starts the transmission loops for both the sender and receiver.
   /// </summary>
   public void Start()
   {
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

      _sender.Stop();
      _receiver.Stop();

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

   /// <summary>
   /// Disposes the connection and closes the socket gracefully.
   /// </summary>
   public async ValueTask DisposeAsync()
   {
      lock (_shutdownLock)
      {
         if (_isDisposed) return;
         _isDisposed = true;
      }

      _sender.Stop();
      _receiver.Stop();

      await _receiver.DisposeAsync();

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

   /// <summary>
   /// Resets the connection and its sub-components back to their clean initial state for reuse in the pool.
   /// </summary>
   public bool TryResetState()
   {
      if (!_sender.TryResetState() || !_receiver.TryResetState())
      {
         return false;
      }

      _socket = null;
      _isDisposed = false;
      _isAborted = false;

      return true;
   }
}