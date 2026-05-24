using System.Net;
using Beskar.Networking.Abstractions.Errors;
using Me.Memory.Results;

namespace Beskar.Networking.Abstractions.Interfaces;

/// <summary>
/// Represents a network listener.
/// </summary>
public interface INetworkListener
{
   /// <summary>
   /// The local address of the listener.
   /// </summary>
   public EndPoint LocalAddress { get; }

   /// <summary>
   /// Binds the listener to the local address.
   /// </summary>
   public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default);

   /// <summary>
   /// Unbinds the listener from the local address.
   /// </summary>
   public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default);

   /// <summary>
   /// Accepts a new network session.
   /// </summary>
   public ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default);

}
