using System.Net;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Memory.Results;

namespace Beskar.Networking.Abstractions.Interfaces;

/// <summary>
/// Represents a network client.
/// </summary>
public interface INetworkClient : IAsyncDisposable
{
   /// <summary>
   /// Tries to connect to a remote endpoint.
   /// </summary>
   public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint, CancellationToken ct = default);

   /// <summary>
   /// Disconnects any active session established by this client.
   /// </summary>
   public ValueTask DisconnectAsync(CancellationToken ct = default);
}
