using System.Net;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.Common.Hosting;

public sealed class NetworkClientBuilder
{
   private Func<EndPoint, CancellationToken, ValueTask<Result<INetworkSession, NetworkCodeError>>>? _connector;

   public static NetworkClientBuilder Create()
   {
      return new NetworkClientBuilder();
   }

   public NetworkClientBuilder UseConnector(
      Func<EndPoint, CancellationToken, ValueTask<Result<INetworkSession, NetworkCodeError>>> connector)
   {
      _connector = connector;
      return this;
   }

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint, CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(_connector);
      return await _connector(endPoint, ct);
   }
}
