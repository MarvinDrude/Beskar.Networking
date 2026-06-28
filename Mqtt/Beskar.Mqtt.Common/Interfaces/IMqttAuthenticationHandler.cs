using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Models;

namespace Beskar.Mqtt.Common.Interfaces;

public interface IMqttAuthenticationHandler
{
   public Task<VoidResult<StringError>> ExecuteAsync(MqttAuthContext context, CancellationToken ct = default);
}
