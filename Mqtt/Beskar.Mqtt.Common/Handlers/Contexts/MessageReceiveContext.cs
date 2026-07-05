using System.Text;
using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Handlers.Contexts;

public sealed class MessageReceiveContext
{
   /// <summary>
   /// The received message data of the packet.
   /// </summary>
   public required MqttPublishMessage Message { get; init; }

   /// <summary>
   /// If true, the client will not send an acknowledgment packet.
   /// </summary>
   public bool HasFailed { get; set; }

   /// <summary>
   /// The reason code to send to the server in the acknowledgment.
   /// </summary>
   public PubAckReasonCode ReasonCode { get; set; } = PubAckReasonCode.Success;

   /// <summary>
   /// Whether the acknowledgment packet is automatically sent after your handler.
   /// </summary>
   public bool AutoAcknowledge { get; init; } = true;

   /// <summary>
   /// The current client identifier (must be unique)
   /// </summary>
   public string? ClientId { get; init; }

   /// <summary>
   /// The reason string to send to the server.
   /// </summary>
   public string ReasonString { get; set; } = string.Empty;

   /// <summary>
   /// Used to add new user properties which are send to the server.
   /// </summary>
   public UserPropertyListBuilder ResponseUserProperties { get; set; } = new();

   /// <summary>
   /// Sends the appropiate ack packages to the server if HasFailed is not true.
   /// In case of quality of service of "AtMostOnce", there is no ack needed and will do nothing.
   /// <remarks>Does not need to be called independently if you keep AutoAcknowledge true (default value).</remarks>
   /// </summary>
   public Task AcknowledgeAsync(CancellationToken ct = default)
   {
      if (HasFailed) return Task.CompletedTask;

      switch (Message.QualityOfService)
      {
         case QualityOfServiceType.AtLeastOnce:
            var pubAckPacket = new PubAckPacket()
            {
               PacketIdentifier = Message.PacketIdentifier,
               ReasonCode = ReasonCode,
               ReasonStringUtf8Bytes = Encoding.UTF8.GetBytes(ReasonString),
               PropertiesBytes = ResponseUserProperties.WrittenSpan.ToArray()
            };

            break;
         case QualityOfServiceType.ExactlyOnce:
            break;
      }

      return Task.CompletedTask;
   }
}
