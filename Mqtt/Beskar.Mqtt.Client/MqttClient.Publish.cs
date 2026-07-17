using System.Text;
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
using Beskar.Utilities.Tracing;

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

      TraceLogger.LogClientInfo("MqttClient.PublishAsync: Publishing to topic '{0}' (QoS: {1}).", Encoding.UTF8.GetString(options.TopicUtf8Bytes.Span), options.QualityOfService);

      if (options.QualityOfService is QualityOfServiceType.AtMostOnce)
      {
         return await PublishAtMostOnceAsync(options, stream, ct);
      }

      var semaphore = _inFlightSemaphore;
      if (semaphore is not null)
      {
         await semaphore.WaitAsync(ct);
      }

      try
      {
         return options.QualityOfService switch
         {
            QualityOfServiceType.ExactlyOnce => await PublishExactlyOnceAsync(options, stream, ct),
            QualityOfServiceType.AtLeastOnce => await PublishAtLeastOnceAsync(options, stream, ct),
            _ => new StringError("Invalid quality of service.")
         };
      }
      finally
      {
         try
         {
            semaphore?.Release();
         }
         catch (ObjectDisposedException)
         {
            // Ignored
         }
      }
   }

   private async Task<Result<PublishResult, StringError>> PublishAtMostOnceAsync(
      PublishOptions options, INetworkStream stream, CancellationToken ct = default)
   {
      try
      {
         TraceLogger.LogClientInfo("MqttClient.PublishAtMostOnceAsync: Sending QoS 0 publish packet to topic '{0}'...", Encoding.UTF8.GetString(options.TopicUtf8Bytes.Span));
         await Send(options, stream, 0, ct);

         return new PublishResult()
         {
            UserProperties = UserPropertyCollection.Empty,
            ReasonCode = PubAckReasonCode.Success
         };
      }
      catch (Exception error)
      {
         TraceLogger.LogClientError("MqttClient.PublishAtMostOnceAsync: QoS 0 publish failed: {0}", error.Message);
         return new StringError(error.ToString());
      }
   }

   private async Task<Result<PublishResult, StringError>> PublishAtLeastOnceAsync(
      PublishOptions options, INetworkStream stream, CancellationToken ct = default)
   {
      try
      {
         TraceLogger.LogClientInfo("MqttClient.PublishAtLeastOnceAsync: Sending QoS 1 publish packet to topic '{0}'...", Encoding.UTF8.GetString(options.TopicUtf8Bytes.Span));
         var pubAckPacket = await SendAndAck<PublishOptions, PubAckPacket>(options, stream, ct);
         TraceLogger.LogClientInfo("MqttClient.PublishAtLeastOnceAsync: Received PUBACK (PacketId: {0}, ReasonCode: {1}).", pubAckPacket.PacketIdentifier, pubAckPacket.ReasonCode);

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
         TraceLogger.LogClientError("MqttClient.PublishAtLeastOnceAsync: QoS 1 publish failed: {0}", error.Message);
         return new StringError(error.ToString());
      }
   }

   private async Task<Result<PublishResult, StringError>> PublishExactlyOnceAsync(
      PublishOptions options, INetworkStream stream, CancellationToken ct = default)
   {
      try
      {
         TraceLogger.LogClientInfo("MqttClient.PublishExactlyOnceAsync: Sending QoS 2 publish packet to topic '{0}'...", Encoding.UTF8.GetString(options.TopicUtf8Bytes.Span));
         var pubRecPacket = await SendAndAck<PublishOptions, PubRecPacket>(options, stream, ct);

         if (pubRecPacket.ReasonCode >= PubRecReasonCode.UnspecifiedError)
         {
            TraceLogger.LogClientWarning("MqttClient.PublishExactlyOnceAsync: Received PUBREC with failure ReasonCode: {0}. Releasing packet identifier.", pubRecPacket.ReasonCode);
            return new PublishResult()
            {
               UserProperties = UserPropertyCollection.Create(pubRecPacket.PropertiesBytes),
               ReasonCode = (PubAckReasonCode)pubRecPacket.ReasonCode,
               PacketIdentifier = pubRecPacket.PacketIdentifier,
               ReasonString = pubRecPacket.ReasonStringUtf8Bytes.GetUtf8String()
            };
         }

         TraceLogger.LogClientInfo("MqttClient.PublishExactlyOnceAsync: Received PUBREC (PacketId: {0}, ReasonCode: {1}). Sending PUBREL...", pubRecPacket.PacketIdentifier, pubRecPacket.ReasonCode);

         var pubRelPacket = new PubRelPacket()
         {
            PacketIdentifier = pubRecPacket.PacketIdentifier,
            ReasonCode = PubRelReasonCode.Success
         };

         var pubCompPacket = await SendAndAck<PubRelPacket, PubCompPacket>(pubRelPacket, stream, ct);
         TraceLogger.LogClientInfo("MqttClient.PublishExactlyOnceAsync: Received PUBCOMP (PacketId: {0}, ReasonCode: {1}). QoS 2 publish complete.", pubCompPacket.PacketIdentifier, pubCompPacket.ReasonCode);

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
         TraceLogger.LogClientError("MqttClient.PublishExactlyOnceAsync: QoS 2 publish failed: {0}", error.Message);
         return new StringError(error.ToString());
      }
   }
}
