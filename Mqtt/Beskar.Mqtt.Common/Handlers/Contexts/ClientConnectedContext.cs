using Beskar.Mqtt.Protocol.Results;

namespace Beskar.Mqtt.Common.Handlers.Contexts;

public sealed class ClientConnectedContext
{
   public required ClientConnectResult ConnectResult { get; init; }
}
