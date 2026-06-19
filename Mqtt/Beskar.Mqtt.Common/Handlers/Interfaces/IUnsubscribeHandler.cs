using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Handlers.Interfaces;

/// <summary>
/// Defines a handler for processing incoming MQTT <see cref="UnsubscribePacket"/>s.
/// </summary>
public interface IUnsubscribeHandler
{
   /// <summary>
   /// Executes the handler for the specified MQTT <see cref="UnsubscribePacket"/>.
   /// </summary>
   /// <param name="packet">The incoming UNSUBSCRIBE packet.</param>
   /// <param name="ct">A cancellation token that can be used to cancel the execution.</param>
   /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
   public ValueTask ExecuteAsync(in UnsubscribePacket packet, CancellationToken ct = default);
}
