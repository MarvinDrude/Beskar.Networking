using Beskar.Mqtt.Protocol.Packets;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Common.Handlers.Interfaces;

/// <summary>
/// Defines a handler for processing incoming MQTT <see cref="ConnAckPacket"/>s.
/// </summary>
public interface IConnAckHandler
{
   /// <summary>
   /// Executes the handler for the specified MQTT <see cref="ConnAckPacket"/>.
   /// </summary>
   /// <param name="stream">The connection stream.</param>
   /// <param name="packet">The incoming CONNACK packet.</param>
   /// <param name="ct">A cancellation token that can be used to cancel the execution.</param>
   /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
   public ValueTask ExecuteAsync(INetworkStream stream, in ConnAckPacket packet, CancellationToken ct = default);
}
