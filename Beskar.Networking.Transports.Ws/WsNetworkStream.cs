using System.IO.Pipelines;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Transports.Ws;

/// <summary>
/// Represents a WebSocket network stream wrapping a <see cref="WsDuplexPipe"/>.
/// </summary>
public sealed class WsNetworkStream(
   INetworkSession session,
   IDuplexPipe transport)
   : INetworkStream
{
   public long StreamId => 0;

   public INetworkSession Session { get; } = session;
   public IDuplexPipe Transport { get; } = transport;

   public NetworkStreamDirection Direction => NetworkStreamDirection.Bidirectional;

   public ValueTask DisposeAsync()
   {
      return ValueTask.CompletedTask;
   }
}
