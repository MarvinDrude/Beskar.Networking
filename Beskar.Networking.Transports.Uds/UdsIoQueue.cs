using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Transports.Common.Settings;
using Beskar.Networking.Transports.Common.Sockets;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Pools;

namespace Beskar.Networking.Transports.Uds;

public sealed class UdsIoQueue(UdsTransportOptions options) : IDisposable
{
   public SocketQueueSettings SocketSettings { get; } = options.SocketOptions.CreateQueueSettings();

   public AsyncDisposableObjectPool<SocketConnection> SocketConnectionPool { get; init; } = null!;

   public IDuplexPipe Create(Socket socket)
   {
      TraceLogger.LogNeutralInfo("Creating uds socket connection for socket");
      var connection = SocketConnectionPool.Get(() => new SocketConnection(
         SocketSettings.PipeScheduler, SocketSettings.MemoryPool));

      connection.Initialize(socket);
      connection.Start();

      return connection;
   }

   public void Dispose()
   {
      SocketSettings.Dispose();
   }
}
