using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Transports.Common.Sockets;
using Beskar.Networking.Transports.Common.Streams;
using Me.Memory.Pools;

namespace Beskar.Networking.Transports.Tcp;

public sealed class TcpIoQueueRegistry : IDisposable
{
   private readonly int _ioQueueCount;
   private readonly ulong _ioQueueCountLong;

   private readonly TcpIoQueue[] _ioQueues;

   private ulong _currentIndex;
   private bool _isDisposed;

   private readonly AsyncDisposableObjectPool<SocketConnection>? _socketConnectionPool;
   private readonly AsyncDisposableObjectPool<StreamConnection>? _streamConnectionPool;

   public TcpIoQueueRegistry(TcpTransportOptions options)
   {
      _ioQueueCount = options.IoQueueCount;
      _ioQueueCountLong = (ulong)_ioQueueCount;

      _ioQueues = new TcpIoQueue[_ioQueueCount];

      if (options.IsStreamBased)
      {
         _streamConnectionPool = new AsyncDisposableObjectPool<StreamConnection>(new ObjectPoolOptions<StreamConnection>()
         {
            FactoryFunc = static () => throw new InvalidOperationException(),
            InitialSize = options.StreamOptions.InitialConnectionPoolSize,
            MaxSize = options.StreamOptions.MaxConnectionPoolSize,
         });
      }
      else
      {
         _socketConnectionPool = new AsyncDisposableObjectPool<SocketConnection>(new ObjectPoolOptions<SocketConnection>()
         {
            FactoryFunc = static () => throw new InvalidOperationException(),
            InitialSize = options.SocketOptions.InitialConnectionPoolSize,
            MaxSize = options.SocketOptions.MaxConnectionPoolSize,
         });
      }

      for (var e = 0; e < _ioQueueCount; e++)
      {
         _ioQueues[e] = new TcpIoQueue(options)
         {
            SocketConnectionPool = _socketConnectionPool,
            StreamConnectionPool = _streamConnectionPool
         };
      }
   }

   public IDuplexPipe Create(Socket socket, Stream? stream = null)
   {
      var ioQueue = _ioQueues[Interlocked.Increment(ref _currentIndex) % _ioQueueCountLong];
      return ioQueue.Create(socket, stream);
   }

   public void Dispose()
   {
      if (_isDisposed) return;
      _isDisposed = true;

      foreach (var setting in _ioQueues)
      {
         setting.Dispose();
      }
   }
}
