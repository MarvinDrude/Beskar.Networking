using Beskar.Mqtt.Common.Interfaces.Builders;
using Beskar.Mqtt.Protocol.Interfaces;

namespace Beskar.Mqtt.Common.Builders.Common;

public abstract class UserPropertiesBaseOptions(int builderCapacity = -1) : IClearableOptions, IHeapMqttOptions
{
   private UserPropertyListBuilder? _userProperties;

   /// <summary>
   /// Key-Value pairs by the user.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public UserPropertyListBuilder UserProperties
      => _userProperties ??= new UserPropertyListBuilder(builderCapacity == -1 ? 128 :  builderCapacity);

   internal bool HasUserProperties => _userProperties is { Count: > 0 };

   public virtual void Clear()
   {
      _userProperties?.Clear();
   }
}
