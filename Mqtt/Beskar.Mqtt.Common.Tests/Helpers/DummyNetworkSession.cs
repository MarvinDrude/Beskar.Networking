using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Common.Tests.Helpers;

public class DummyNetworkSession : INetworkSession
{
   public Guid Id { get; } = Guid.NewGuid();
   public EndPoint RemoteAddress { get; } = new IPEndPoint(IPAddress.Loopback, 0);
   public EndPoint LocalAddress { get; } = new IPEndPoint(IPAddress.Loopback, 0);
   public bool IsSupportingMultiplexing => false;
   public bool IsSupportingUnidirectional => false;
   public CancellationToken SessionClosedToken => CancellationToken.None;

   public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public ValueTask DisposeAsync()
   {
      return ValueTask.CompletedTask;
   }
}
