using Beskar.Mqtt.Common.Interfaces.Builders;

namespace Beskar.Mqtt.Common.Builders.Common;

public abstract class UserPropertiesBaseOptions(int builderCapacity = -1) : IClearableOptions
{
   /// <summary>
   /// Key-Value pairs by the user.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public UserPropertyListBuilder UserProperties
      => field ??= new UserPropertyListBuilder(builderCapacity == -1 ? 128 :  builderCapacity);

   public virtual void Clear()
   {
      UserProperties.Clear();
   }
}
