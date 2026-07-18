namespace Beskar.Mqtt.ChaosSimulator;

public enum AuthScenario
{
   Valid,
   Invalid,
   Unauthenticated
}

public enum ClientRole
{
   Publisher,
   Subscriber,
   KeepAliveOnly,
   Flaky,
   SlowSubscriber,
   Qos2HeavyPublisher,
   WildcardSubscriber,
   AuthAlternator,
   ChannelCongestor
}
