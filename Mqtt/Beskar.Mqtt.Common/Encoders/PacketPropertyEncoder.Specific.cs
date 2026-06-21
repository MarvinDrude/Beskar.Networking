using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders;

public ref partial struct PacketPropertyEncoder
{
   public void WriteSubscriptionIdentifiersAvailable(bool set)
   {
      if (set) return; // default is true so ignore
      Write(PropertyIdentifier.SubscriptionIdentifierAvailable, false);
   }
}
