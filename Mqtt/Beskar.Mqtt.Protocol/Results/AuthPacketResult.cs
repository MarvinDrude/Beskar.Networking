using System;
using System.Buffers;
using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Protocol.Results;

public sealed class AuthPacketResult
{
   public required AuthenticateReasonCode ReasonCode { get; init; }

   public string? Reason { get; init; }

   public string? AuthenticationMethod { get; init; }

   public ReadOnlyMemory<byte>? AuthenticationData { get; init; }

   public required UserPropertyCollection UserProperties { get; init; }

   public static AuthPacketResult Create(in AuthPacket packet)
   {
      return new AuthPacketResult
      {
         ReasonCode = packet.ReasonCode,
         Reason = packet.ReasonUtf8Bytes.GetUtf8String(),
         AuthenticationMethod = packet.AuthenticationMethodUtf8Bytes.GetUtf8String(),
         AuthenticationData = packet.AuthenticationDataBytes.ToNullableMemory(),
         UserProperties = UserPropertyCollection.Create(packet.PropertiesBytes)
      };
   }
}
