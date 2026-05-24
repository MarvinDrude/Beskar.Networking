using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces.Pools;

namespace Beskar.Networking.Transports.Common.Sockets;

public sealed class SocketConnection
   : IDuplexPipe, IAsyncDisposable, IPooledObject
{
   private readonly SocketSender _sender;
   private readonly SocketReceiver _receiver;
   private Socket? _socket;

   private readonly Lock _shutdownLock = new();
   private bool _isDisposed;
   private bool _isAborted;

   public PipeReader Input => _receiver.Pipe.Reader;
   public PipeWriter Output => _sender.Pipe.Writer;

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

   public void Initialize(Socket socket)
   {
      _socket = socket;

      _sender.Initialize(this, socket);
      _receiver.Initialize(this, socket);
   }

   public void Start()
   {
      _sender.Start();
      _receiver.Start();
   }

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
            // Expected
         }
      }
   }

   public async ValueTask StopAsync()
   {
      await _sender.StopAsync();
      await _receiver.StopAsync();

      if (!_isAborted && _socket != null)
      {
         try
         {
            _socket.Shutdown(SocketShutdown.Both);
         }
         catch
         {
            // Expected
         }
         finally
         {
            _socket.Dispose();
         }
      }
   }

   public async ValueTask DisposeAsync()
   {
      lock (_shutdownLock)
      {
         if (_isDisposed) return;
         _isDisposed = true;
      }

      await StopAsync();
   }

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
