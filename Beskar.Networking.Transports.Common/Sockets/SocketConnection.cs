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

   private Action? _connectionClosedHandler;
   private int _connectionClosed;

   public event Action? ConnectionClosed
   {
      add
      {
         _connectionClosedHandler += value;
         if (Volatile.Read(ref _connectionClosed) == 1)
         {
            try
            {
               value?.Invoke();
            }
            catch
            {
               // Ignored
            }
         }
      }
      remove => _connectionClosedHandler -= value;
   }

   public PipeReader Input => _receiver.Pipe.Reader;
   public PipeWriter Output => _sender.Pipe.Writer;

   public SocketConnection(PipeScheduler scheduler, MemoryPool<byte> bufferPool)
      : this(
         new PipeOptions(
            pool: bufferPool,
            readerScheduler: scheduler,
            writerScheduler: scheduler,
            useSynchronizationContext: false),
         bufferPool)
   {
   }

   public SocketConnection(PipeOptions receiveOptions, MemoryPool<byte> bufferPool)
      : this(
         receiveOptions,
         new PipeOptions(
            pool: bufferPool,
            readerScheduler: receiveOptions.ReaderScheduler,
            writerScheduler: receiveOptions.WriterScheduler,
            useSynchronizationContext: false))
   {
   }

   public SocketConnection(PipeOptions receiveOptions, PipeOptions sendOptions)
   {
      _sender = new SocketSender(sendOptions);
      _receiver = new SocketReceiver(receiveOptions);
   }

   public void Initialize(Socket socket)
   {
      _socket = socket;
      Volatile.Write(ref _connectionClosed, 0);

      _sender.Initialize(this, socket);
      _receiver.Initialize(this, socket);
   }

   public void Start()
   {
      _sender.Start();
      _receiver.Start();
   }

   internal void NotifyConnectionClosed()
   {
      if (Interlocked.Exchange(ref _connectionClosed, 1) == 1)
      {
         return;
      }

      try
      {
         _connectionClosedHandler?.Invoke();
      }
      catch
      {
         // Ignored
      }
   }

   public void Abort(Exception? exception = null)
   {
      lock (_shutdownLock)
      {
         if (_isAborted) return;
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

      NotifyConnectionClosed();
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

      await _sender.DisposeAsync();
      await _receiver.DisposeAsync();
   }

   public bool TryResetState()
   {
      if (!_sender.TryResetState() || !_receiver.TryResetState())
      {
         return false;
      }

      _connectionClosedHandler = null;
      Volatile.Write(ref _connectionClosed, 0);
      _socket = null;

      _isDisposed = false;
      _isAborted = false;

      return true;
   }
}
