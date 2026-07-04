using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Builders.Disconnecting;

public sealed class DisconnectOptionsBuilder(DisconnectOptions? options = null)
   : UserPropertiesBaseOptionsBuilder<DisconnectOptionsBuilder, DisconnectOptions>(options ?? new DisconnectOptions())
{
   /// <summary>
   /// Sets the reason code of why the disconnect happened.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public DisconnectOptionsBuilder WithReasonCode(DisconnectReasonCode reasonCode)
   {
      _options.ReasonCode = reasonCode;
      return this;
   }

   /// <summary>
   /// Sets the reason string.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public DisconnectOptionsBuilder WithReasonString(string? reasonString)
   {
      _options.ReasonString = reasonString;
      return this;
   }

   /// <summary>
   /// Sets the session expiry interval in seconds.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public DisconnectOptionsBuilder WithSessionExpiryInterval(uint interval)
   {
      _options.SessionExpiryInterval = interval;
      return this;
   }
}
