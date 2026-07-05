using System.Buffers;
using System.Text;
using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Results;

namespace Beskar.Mqtt.Common.Models;

public sealed class MqttAuthContext
{
   /// <summary>
   /// The incoming auth packet from the server.
   /// </summary>
   public required AuthPacketResult AuthPacket { get; init; }

   /// <summary>
   /// Which user properties are sent to the server in SendResponseAsync
   /// </summary>
   public UserPropertyListBuilder ResponseUserProperties { get; set; } = new();

   /// <summary>
   /// The packet sender used to transmit packets like acknowledgments.
   /// </summary>
   public required IMqttPacketSender PacketSender { get; init; }



   /// <summary>
   /// Send this as response to the auth request from the server.
   /// </summary>
   public Task SendResponseAsync(
      Memory<byte> data, string reasonString, CancellationToken ct = default)
   {
      var authPacket = new AuthPacket()
      {
         ReasonCode = AuthenticateReasonCode.ContinueAuthentication,
         AuthenticationMethodUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(AuthPacket.AuthenticationMethod ?? string.Empty)),
         AuthenticationDataBytes =  new ReadOnlySequence<byte>(data),
         ReasonUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(reasonString)),
         PropertiesBytes = new ReadOnlySequence<byte>([.. ResponseUserProperties.WrittenSpan])
      };

      return PacketSender.SendAsync(authPacket, ct);
   }
}
