using Beskar.Memory.Pools;
using Beskar.Mqtt.Server.Handlers;
using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server;

public sealed partial class MqttServer
{
   private const int DefaultMaxPoolSize = 2048;
   private const int DefaultInitialPoolSize = 0;

   private readonly ObjectPool<MqttServerClient> _serverClientPool = new (new ObjectPoolOptions<MqttServerClient>
   {
      FactoryFunc = static () => new MqttServerClient(),
      MaxSize = DefaultMaxPoolSize,
      InitialSize = DefaultInitialPoolSize,
      ReturnFunc = static (_) => true
   });

   private readonly ObjectPool<ServerPacketHandler> _packetHandlerPool = new (new ObjectPoolOptions<ServerPacketHandler>
   {
      FactoryFunc = static () => new ServerPacketHandler(),
      MaxSize = DefaultMaxPoolSize,
      InitialSize = DefaultInitialPoolSize,
      ReturnFunc = static (_) => true
   });
}
