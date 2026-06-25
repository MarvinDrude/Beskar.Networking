using Beskar.Mqtt.Common.Builders.Common;

namespace Beskar.Mqtt.Common.Builders.Connecting;

public sealed class ConnectOptions(int builderCapacity = -1)
   : UserPropertiesBaseOptions(builderCapacity)
{
   private readonly int _builderCapacity = builderCapacity;


}
