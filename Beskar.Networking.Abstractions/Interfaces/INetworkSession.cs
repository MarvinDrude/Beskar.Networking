using System.Net;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Memory.Results;

namespace Beskar.Networking.Abstractions.Interfaces;

/// <summary>
/// Represents a network session.
/// </summary>
public interface INetworkSession
{
   /// <summary>
   /// The unique identifier of the session.
   /// </summary>
   public Guid Id { get; }
   
   /// <summary>
   /// The remote address of the session.
   /// </summary>
   public EndPoint RemoteAddress { get; }
   
   /// <summary>
   /// The local address of the session.
   /// </summary>
   public EndPoint LocalAddress { get; }
   
   /// <summary>
   /// Indicates whether the session supports multiplexing.
   /// </summary>
   public bool IsSupportingMultiplexing { get; }
   
   /// <summary>
   /// Indicates whether the session supports unidirectional communication.
   /// </summary>
   public bool IsSupportingUnidirectional { get; }
   
   /// <summary>
   /// The <see cref="CancellationToken"/> that is triggered when the session is closed.
   /// </summary>
   public CancellationToken SessionClosedToken { get; }
   
   /// <summary>
   /// Accepts or gets the network stream of the current network session.
   /// </summary>
   public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default);
   
   /// <summary>
   /// Opens a new network stream with the specified direction.
   /// </summary>
   public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default);
}