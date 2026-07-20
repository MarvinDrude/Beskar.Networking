using System.IO.Pipelines;
using Beskar.Networking.Transports.Common.Settings;
using Beskar.Networking.Transports.Common.Streams;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Pools;

namespace Beskar.Networking.Transports.NamedPipes;

public sealed class NamedPipeIoQueue(NamedPipeTransportOptions options) : IDisposable
{
   public StreamQueueSettings StreamSettings { get; } = options.StreamOptions.CreateQueueSettings();

   public AsyncDisposableObjectPool<StreamConnection> StreamConnectionPool { get; init; } = null!;

   public IDuplexPipe Create(Stream stream)
   {
      TraceLogger.LogNeutralInfo("Creating named pipe stream connection");
      var connection = StreamConnectionPool.Get(() => new StreamConnection(
         StreamSettings.ReceiveOptions, StreamSettings.SendOptions));

      connection.Initialize(stream);
      connection.Start();

      return connection;
   }

   public void Dispose()
   {
      StreamSettings.Dispose();
   }
}
