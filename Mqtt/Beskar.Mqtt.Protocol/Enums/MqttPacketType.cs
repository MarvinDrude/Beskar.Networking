using Beskar.Memory.Code.EnumGenerator.Attributes;

namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// MQTT control packet types (defined in the 4 high bits of the fixed header).
/// </summary>
[FastEnum]
public enum MqttPacketType : byte
{
   /// <summary>
   /// CONNECT (1). Connection request.
   /// <para>Sent by: Client.</para>
   /// </summary>
   Connect = 1,

   /// <summary>
   /// CONNACK (2). Connect acknowledgment.
   /// <para>Sent by: Server.</para>
   /// </summary>
   ConnAck = 2,

   /// <summary>
   /// PUBLISH (3). Publish message.
   /// <para>Sent by: Client or Server.</para>
   /// </summary>
   Publish = 3,

   /// <summary>
   /// PUBACK (4). Publish acknowledgment (QoS 1).
   /// <para>Sent by: Client or Server.</para>
   /// </summary>
   PubAck = 4,

   /// <summary>
   /// PUBREC (5). Publish received (QoS 2 delivery part 1).
   /// <para>Sent by: Client or Server.</para>
   /// </summary>
   PubRec = 5,

   /// <summary>
   /// PUBREL (6). Publish release (QoS 2 delivery part 2).
   /// <para>Sent by: Client or Server.</para>
   /// </summary>
   PubRel = 6,

   /// <summary>
   /// PUBCOMP (7). Publish complete (QoS 2 delivery part 3).
   /// <para>Sent by: Client or Server.</para>
   /// </summary>
   PubComp = 7,

   /// <summary>
   /// SUBSCRIBE (8). Subscribe request.
   /// <para>Sent by: Client.</para>
   /// </summary>
   Subscribe = 8,

   /// <summary>
   /// SUBACK (9). Subscribe acknowledgment.
   /// <para>Sent by: Server.</para>
   /// </summary>
   SubAck = 9,

   /// <summary>
   /// UNSUBSCRIBE (10). Unsubscribe request.
   /// <para>Sent by: Client.</para>
   /// </summary>
   Unsubscribe = 10,

   /// <summary>
   /// UNSUBACK (11). Unsubscribe acknowledgment.
   /// <para>Sent by: Server.</para>
   /// </summary>
   UnsubAck = 11,

   /// <summary>
   /// PINGREQ (12). PING request.
   /// <para>Sent by: Client.</para>
   /// </summary>
   PingReq = 12,

   /// <summary>
   /// PINGRESP (13). PING response.
   /// <para>Sent by: Server.</para>
   /// </summary>
   PingResp = 13,

   /// <summary>
   /// DISCONNECT (14). Disconnect notification.
   /// <para>Sent by: Client or Server.</para>
   /// </summary>
   Disconnect = 14,

   /// <summary>
   /// AUTH (15). Authentication exchange.
   /// <para>Sent by: Client or Server.</para>
   /// </summary>
   Auth = 15
}
