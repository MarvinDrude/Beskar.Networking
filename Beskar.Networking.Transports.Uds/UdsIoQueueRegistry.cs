using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Transports.Common.Sockets;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Pools;

namespace Beskar.Networking.Transports.Uds;

public sealed class UdsIoQueueRegistry : IAsyncDisposable
{
   private readonly ulong _ioQueueCountLong;
   private readonly UdsIoQueue[] _ioQueues;

   private ulong _currentIndex;
   private bool _isDisposed;

   private readonly AsyncDisposableObjectPool<SocketConnection> _socketConnectionPool;

   public UdsIoQueueRegistry(UdsTransportOptions options)
   {
      _ioQueueCountLong = (ulong)options.IoQueueCount;
      _ioQueues = new UdsIoQueue[options.IoQueueCount];

      _socketConnectionPool = new AsyncDisposableObjectPool<SocketConnection>(new ObjectPoolOptions<SocketConnection>()
      {
         FactoryFunc = static () => throw new InvalidOperationException(),
         ReturnFunc = static connection => connection.TryResetState(),
         InitialSize = 0,
         MaxSize = options.SocketOptions.MaxConnectionPoolSize,
      });

      for (var e = 0; e < options.IoQueueCount; e++)
      {
         _ioQueues[e] = new UdsIoQueue(options)
         {
            SocketConnectionPool = _socketConnectionPool
         };
      }
   }

   public IDuplexPipe Create(Socket socket)
   {
      TraceLogger.LogNeutralInfo("UDS IO Registry: Creating duplex pipe connection for socket");
      var ioQueue = _ioQueues[Interlocked.Increment(ref _currentIndex) % _ioQueueCountLong];
      return ioQueue.Create(socket);
   }

   public async ValueTask ReturnAsync(IDuplexPipe connection)
   {
      if (connection is SocketConnection socketConn)
      {
         await socketConn.StopAsync();

         TraceLogger.LogNeutralInfo("UDS IO Registry: Returning socket connection to pool");
         await _socketConnectionPool.ReturnAsync(socketConn);
      }
   }

   public async ValueTask DisposeAsync()
   {
      if (_isDisposed) return;
      _isDisposed = true;

      foreach (var setting in _ioQueues)
      {
         setting.Dispose();
      }

      await _socketConnectionPool.DisposeAsync();
   }
}
