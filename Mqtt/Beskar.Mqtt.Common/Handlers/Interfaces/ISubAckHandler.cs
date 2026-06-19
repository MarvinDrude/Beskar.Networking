using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Handlers.Interfaces;

/// <summary>
/// Defines a handler for processing incoming MQTT <see cref="SubAckPacket"/>s.
/// </summary>
public interface ISubAckHandler
{
   /// <summary>
   /// Executes the handler for the specified MQTT <see cref="SubAckPacket"/>.
   /// </summary>
   /// <param name="packet">The incoming SUBACK packet.</param>
   /// <param name="ct">A cancellation token that can be used to cancel the execution.</param>
   /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
   public ValueTask ExecuteAsync(in SubAckPacket packet, CancellationToken ct = default);
}
