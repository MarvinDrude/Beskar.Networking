using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Results;

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
}
