using System.Net;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Me.Memory.Results;

namespace Beskar.Networking.Transports.Tcp;

public sealed class TcpNetworkSession
   : INetworkSession
{
   public Guid Id { get; } = Guid.CreateVersion7();

   public EndPoint RemoteAddress { get; }
   public EndPoint LocalAddress { get; }

   public bool IsSupportingMultiplexing => false;
   public bool IsSupportingUnidirectional => false;

   public CancellationToken SessionClosedToken { get; }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(
      CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
      CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }
}
