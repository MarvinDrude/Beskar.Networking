using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Transports.Common.Sockets;
using Beskar.Networking.Transports.Common.Streams;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Pools;

namespace Beskar.Networking.Transports.Tcp;

public sealed class TcpIoQueueRegistry : IDisposable
{
   private readonly ulong _ioQueueCountLong;

   private readonly TcpIoQueue[] _ioQueues;

   private ulong _currentIndex;
   private bool _isDisposed;

   private readonly AsyncDisposableObjectPool<SocketConnection>? _socketConnectionPool;
   private readonly AsyncDisposableObjectPool<StreamConnection>? _streamConnectionPool;

   public TcpIoQueueRegistry(TcpTransportOptions options)
   {
      _ioQueueCountLong = (ulong)options.IoQueueCount;
      _ioQueues = new TcpIoQueue[options.IoQueueCount];

      if (options.IsStreamBased)
      {
         _streamConnectionPool = new AsyncDisposableObjectPool<StreamConnection>(new ObjectPoolOptions<StreamConnection>()
         {
            FactoryFunc = static () => throw new InvalidOperationException(),
            ReturnFunc = static connection => connection.TryResetState(),
            InitialSize = 0,
            MaxSize = options.StreamOptions.MaxConnectionPoolSize,
         });
      }
      else
      {
         _socketConnectionPool = new AsyncDisposableObjectPool<SocketConnection>(new ObjectPoolOptions<SocketConnection>()
         {
            FactoryFunc = static () => throw new InvalidOperationException(),
            ReturnFunc = static connection => connection.TryResetState(),
            InitialSize = 0,
            MaxSize = options.SocketOptions.MaxConnectionPoolSize,
         });
      }

      for (var e = 0; e < options.IoQueueCount; e++)
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
      TraceLogger.LogNeutralInfo("TCP IO Registry: Creating duplex pipe connection for socket {0} (Stream-based: {1})", socket.RemoteEndPoint, stream is not null);
      var ioQueue = _ioQueues[Interlocked.Increment(ref _currentIndex) % _ioQueueCountLong];
      return ioQueue.Create(socket, stream);
   }

   public async ValueTask ReturnAsync(IDuplexPipe connection)
   {
      if (connection is StreamConnection streamConn)
      {
         if (_streamConnectionPool is not null)
         {
            TraceLogger.LogNeutralInfo("TCP IO Registry: Returning stream connection to pool");
            await _streamConnectionPool.ReturnAsync(streamConn);
         }
         else
         {
            TraceLogger.LogNeutralInfo("TCP IO Registry: Disposing unpooled stream connection");
            await streamConn.DisposeAsync();
         }
      }
      else if (connection is SocketConnection socketConn)
      {
         if (_socketConnectionPool is not null)
         {
            TraceLogger.LogNeutralInfo("TCP IO Registry: Returning socket connection to pool");
            await _socketConnectionPool.ReturnAsync(socketConn);
         }
         else
         {
            TraceLogger.LogNeutralInfo("TCP IO Registry: Disposing unpooled socket connection");
            await socketConn.DisposeAsync();
         }
      }
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
