using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Builders.Disconnecting;

/// <summary>
/// All options that are available for sending a DISCONNECT packet in MQTT.
/// </summary>
public sealed class DisconnectOptions(int builderCapacity = -1)
   : UserPropertiesBaseOptions(builderCapacity)
{
   /// <summary>
   /// Reason of why the disconnect happened.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public DisconnectReasonCode ReasonCode { get; set; } = DisconnectReasonCode.NormalDisconnection;

   /// <summary>
   /// Reason string
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public string? ReasonString { get; set; }

   /// <summary>
   /// Server reference (only sent by the server)
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public string? ServerReference { get; set; }

   /// <summary>
   /// Session expiry interval
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public uint? SessionExpiryInterval { get; set; }

   /// <summary>
   /// Clears the options back to their defaults.
   /// </summary>
   public override void Clear()
   {
      base.Clear();

      ReasonCode = DisconnectReasonCode.NormalDisconnection;
      ReasonString = null;
      ServerReference = null;
      SessionExpiryInterval = null;
   }

   public static DisconnectOptionsBuilder Create() => new();

   public static DisconnectOptions Create(in DisconnectPacket packet)
   {
      return new DisconnectOptions()
      {
         ReasonCode = packet.ReasonCode,
         ReasonString = packet.ReasonUtf8Bytes.GetUtf8String(),
         ServerReference = packet.ServerReferenceUtf8Bytes.GetUtf8String(),
         SessionExpiryInterval = packet.SessionExpiryInterval
      };
   }
}
