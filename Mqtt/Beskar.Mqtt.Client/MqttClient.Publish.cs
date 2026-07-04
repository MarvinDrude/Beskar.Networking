using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Encoders.Version3;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Results;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient
{
   public async Task<Result<PublishResult, StringError>> PublishAsync(
      PublishOptions options, CancellationToken ct = default)
   {
      var clientResult = ValidateClient();
      if (!clientResult.IsSuccess) return clientResult.Error;

      if (_controlStream is not { } stream)
      {
         return new StringError("Invalid control stream.");
      }

      return options.QualityOfService switch
      {
         QualityOfServiceType.ExactlyOnce => await PublishExactlyOnceAsync(options, stream, ct),
         QualityOfServiceType.AtLeastOnce => await PublishAtLeastOnceAsync(options, stream, ct),
         QualityOfServiceType.AtMostOnce => await PublishAtMostOnceAsync(options, stream, ct),
         _ => new StringError("Invalid quality of service.")
      };
   }

   private async Task<Result<PublishResult, StringError>> PublishAtMostOnceAsync(
      PublishOptions options, INetworkStream stream, CancellationToken ct = default)
   {
      try
      {
         using (await stream.AcquireWriterLock(ct))
         {
            var writer = stream.Transport.Output;
            switch (_protocolVersion)
            {
               case MqttProtocolVersion.V50:
                  new PacketVersion5Encoder(writer).WritePublish(options);
                  break;
               case MqttProtocolVersion.V31:
               case MqttProtocolVersion.V311:
                  new PacketVersion3Encoder(writer, _protocolVersion).WritePublish(options);
                  break;
               default:
                  throw new InvalidOperationException("Unkown protocol version.");
            }

            await writer.FlushAsync(ct);
         }

         return new PublishResult()
         {
            UserProperties = UserPropertyCollection.Empty,
            ReasonCode = PubAckReasonCode.Success
         };
      }
      catch (Exception error)
      {
         return new StringError(error.ToString());
      }
   }

   private async Task<Result<PublishResult, StringError>> PublishAtLeastOnceAsync(
      PublishOptions options, INetworkStream stream, CancellationToken ct = default)
   {
      try
      {
         var pubAckPacket = await SendAndAck<PublishOptions, PubAckPacket>(options, stream, ct);

         return new PublishResult()
         {
            PacketIdentifier = pubAckPacket.PacketIdentifier,
            ReasonCode = pubAckPacket.ReasonCode,
            ReasonString = pubAckPacket.ReasonStringUtf8Bytes.GetUtf8String(),
            UserProperties = UserPropertyCollection.Create(pubAckPacket.PropertiesBytes)
         };
      }
      catch (Exception error)
      {
         return new StringError(error.ToString());
      }
   }

   private async Task<Result<PublishResult, StringError>> PublishExactlyOnceAsync(
      PublishOptions options, INetworkStream stream, CancellationToken ct = default)
   {
      try
      {
         var pubRecPacket = await SendAndAck<PublishOptions, PubRecPacket>(options, stream, ct);

         var pubRelPacket = new PubRelPacket()
         {
            PacketIdentifier = pubRecPacket.PacketIdentifier,
            ReasonCode = PubRelReasonCode.Success
         };

         var pubCompPacket = await SendAndAck<PubRelPacket, PubCompPacket>(pubRelPacket, stream, ct);
         if (pubCompPacket.ReasonCode is PubCompReasonCode.PacketIdentifierNotFound)
         {
            return new PublishResult()
            {
               UserProperties = UserPropertyCollection.Create(pubCompPacket.PropertiesBytes),
               ReasonCode = PubAckReasonCode.UnspecifiedError,
               PacketIdentifier = pubCompPacket.PacketIdentifier,
               ReasonString = pubCompPacket.ReasonStringUtf8Bytes.GetUtf8String()
            };
         }

         // they the same entries
         var reasonCode = pubRecPacket.ReasonCode is not PubRecReasonCode.Success
            ? (PubAckReasonCode)pubRecPacket.ReasonCode
            : PubAckReasonCode.Success;

         return new PublishResult()
         {
            UserProperties = UserPropertyCollection.Create(pubCompPacket.PropertiesBytes),
            ReasonCode = reasonCode,
            PacketIdentifier = pubCompPacket.PacketIdentifier,
            ReasonString = null
         };
      }
      catch (Exception error)
      {
         return new StringError(error.ToString());
      }
   }
}
