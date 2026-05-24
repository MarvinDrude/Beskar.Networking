using System.IO.Pipelines;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Transports.Tcp;

/// <summary>
/// Represents a TCP network stream wrapper around an <see cref="IDuplexPipe"/>.
/// </summary>
public sealed class TcpNetworkStream(
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
