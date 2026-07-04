using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Protocol.Enums;
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
      throw new NotImplementedException();
   }

   private async Task<Result<PublishResult, StringError>> PublishAtLeastOnceAsync(
      PublishOptions options, INetworkStream stream, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   private async Task<Result<PublishResult, StringError>> PublishExactlyOnceAsync(
      PublishOptions options, INetworkStream stream, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }
}
