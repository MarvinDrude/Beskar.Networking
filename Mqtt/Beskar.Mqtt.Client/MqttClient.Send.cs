using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Encoders.Version3;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Results;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient
{
   public Task<Result<PublishResult, StringError>> PublishAsync(
      PublishOptions options, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public async Task<Result<SubscribeResult, StringError>> SubscribeAsync(
      SubscribeOptions options, CancellationToken ct = default)
   {
      var validateResult = SubscribeOptionsValidator.Validate(options);
      if (!validateResult.IsSuccess) return validateResult.Error;

      var clientResult = ValidateClient();
      if (!clientResult.IsSuccess) return clientResult.Error;

      throw new NotImplementedException();
   }

   public Task<Result<UnsubscribeResult, StringError>> UnsubscribeAsync(
      UnsubscribeOptions options, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public async Task<VoidResult<StringError>> PingAsync(CancellationToken ct = default)
   {
      var clientResult = ValidateClient();
      if (!clientResult.IsSuccess) return clientResult;


      throw new NotImplementedException();
   }

   private async Task SendConnect(INetworkStream stream, ConnectOptions options, CancellationToken ct = default)
   {
      using (await stream.AcquireWriterLock(ct))
      {
         var writer = stream.Transport.Output;
         switch (_protocolVersion)
         {
            case MqttProtocolVersion.V50:
               new PacketVersion5Encoder(writer).WriteConnect(options);
               break;
            case MqttProtocolVersion.V31:
            case MqttProtocolVersion.V311:
               new PacketVersion3Encoder(writer, _protocolVersion).WriteConnect(options);
               break;
            default:
               throw new InvalidOperationException("Unkown protocol version.");
         }

         await writer.FlushAsync(ct);
      }
   }
}
