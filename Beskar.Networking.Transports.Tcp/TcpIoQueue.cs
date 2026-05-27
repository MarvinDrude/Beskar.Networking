using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Transports.Common.Settings;
using Beskar.Networking.Transports.Common.Sockets;
using Beskar.Networking.Transports.Common.Streams;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Pools;

namespace Beskar.Networking.Transports.Tcp;

public sealed class TcpIoQueue : IDisposable
{
   public SocketQueueSettings? SocketSettings { get; set; }
   public StreamQueueSettings? StreamSettings { get; set; }

   public AsyncDisposableObjectPool<SocketConnection>? SocketConnectionPool { get; init; }
   public AsyncDisposableObjectPool<StreamConnection>? StreamConnectionPool { get; init; }

   [MemberNotNullWhen(true, nameof(StreamSettings), nameof(StreamConnectionPool))]
   [MemberNotNullWhen(false, nameof(SocketSettings), nameof(SocketConnectionPool))]
   private bool UseStreamBased { get; init; }

   public TcpIoQueue(TcpTransportOptions options)
   {
      if (options.IsStreamBased)
      {
         StreamSettings = options.StreamOptions.CreateQueueSettings();
         UseStreamBased = true;
         return;
      }

      SocketSettings = options.SocketOptions.CreateQueueSettings();
   }

   public IDuplexPipe Create(Socket socket, Stream? stream = null)
   {
      if (UseStreamBased)
      {
         ArgumentNullException.ThrowIfNull(stream);
         TraceLogger.LogNeutralInfo("Creating tcp stream connection of socket {0}", socket.RemoteEndPoint);

         var connection = StreamConnectionPool.Get(() => new StreamConnection(
            StreamSettings.ReceiveOptions, StreamSettings.SendOptions));

         connection.Initialize(stream);
         connection.Start();

         return connection;
      }
      else
      {
         TraceLogger.LogNeutralInfo("Creating tcp socket connection of socket {0}", socket.RemoteEndPoint);
         var connection = SocketConnectionPool.Get(() => new SocketConnection(
            SocketSettings.PipeScheduler, SocketSettings.MemoryPool));

         connection.Initialize(socket);
         connection.Start();

         return connection;
      }
   }

   public void Dispose()
   {
      SocketSettings?.Dispose();
      StreamSettings?.Dispose();
   }
}
