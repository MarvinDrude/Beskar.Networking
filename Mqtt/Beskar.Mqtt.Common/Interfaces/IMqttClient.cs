using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Results;

namespace Beskar.Mqtt.Common.Interfaces;

public interface IMqttClient : IAsyncDisposable
{
   public Task<Result<PublishResult, StringError>> PublishAsync(
      PublishOptions options, CancellationToken ct = default);

   public Task<Result<SubscribeResult, StringError>> SubscribeAsync(
      SubscribeOptions options, CancellationToken ct = default);

   public Task<Result<UnsubscribeResult, StringError>> UnsubscribeAsync(
      UnsubscribeOptions options, CancellationToken ct = default);

   public Task<VoidResult<StringError>> PingAsync(CancellationToken ct = default);
}
